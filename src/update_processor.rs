use crate::column_graph::{ColumnGraph, ColumnNode, IndexedColumnNode};
use crate::identifier::Identifier;
use crate::table_mapper::{Column, Table};
use crate::temporary_table::TemporaryTable;
use crate::trace_sql;
use indoc::formatdoc;
use itertools::Itertools;
use log::trace;
use rust_decimal::Decimal;
use std::error::Error;
use std::fmt::Debug;
use std::iter::successors;
use thiserror::Error;
use tiberius::{Client, FixedLenType, ToSql, TypeInfo, VarLenType};
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

struct LookupParts {
    set: String,
    outer_apply: String,
}

#[derive(Default)]
struct TargetColumnStatementParts {
    lookups: Vec<LookupParts>,
    parameters: Vec<Box<dyn ToSql>>,
}

#[derive(Debug, Error)]
pub enum UpdateProcessorError {
    #[error(transparent)]
    UpdateFailed(#[from] tiberius::error::Error),
}

pub async fn execute(
    client: &mut Client<Compat<TcpStream>>,
    temporary_table: &TemporaryTable,
    columns: impl IntoIterator<Item = IndexedColumnNode<'_>>,
    column_graph: &ColumnGraph,
) -> Result<(), UpdateProcessorError> {
    let mut static_column_parameter_index: usize = 0;

    let target_column_statement_parts = columns
        .into_iter()
        .try_fold(TargetColumnStatementParts::default(), |mut acc, column| -> Result<TargetColumnStatementParts, Box<dyn Error>> {
            match column.column() {
                ColumnNode::LookupColumn { column: lookup_column, .. } if !column.column().is_transient() => {
                    let (static_column_dependencies, column_dependencies): &(Vec<_>, Vec<_>) =
                        &column_graph
                            .column_dependencies(column.index())
                            .map(|target_column_dependency| {
                                let dependency = successors(Some(target_column_dependency), |prev| {
                                    column_graph.column_dependencies(prev.index()).next()
                                })
                                    .find(|s| !s.column().is_transient())
                                    .unwrap_or(target_column_dependency);

                                let key_column_identifier = match target_column_dependency.column() {
                                    ColumnNode::LookupColumnParserKeyColumn(parser_key_column) => {
                                        parser_key_column.identifier()
                                    }
                                    ColumnNode::LookupColumnProcessedKeyColumn(
                                        processed_key_column,
                                    ) => processed_key_column.identifier(),
                                    _ => unreachable!(
                                        "Lookup columns can only depend on lookup key columns."
                                    ),
                                };

                                (key_column_identifier, dependency)
                            })
                            .partition(|(_, dependency)| {
                                matches!(dependency.column(), ColumnNode::StaticColumn {..})
                            });

                    let column_dependencies_condition = column_dependencies
                        .iter()
                        .map(|(key_column_identifier, dependency)| {
                            format!(
                                "l_inner.{key_column_identifier} = t.{dependency}",
                                key_column_identifier = key_column_identifier.part(),
                                dependency = dependency.unique_identifier().part(),
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(" AND ");

                    let static_column_dependencies_condition = static_column_dependencies
                        .iter()
                        .map(|(key_column_identifier, _)| {
                            let condition = format!(
                                "l_inner.{key_column_identifier} = @P{static_column_parameter_index}",
                                key_column_identifier = key_column_identifier.part(),
                                static_column_parameter_index = static_column_parameter_index,
                            );

                            static_column_parameter_index += 1;

                            condition
                        })
                        .collect::<Vec<_>>()
                        .join(" AND ");

                    let mut static_column_dependencies_parameters = static_column_dependencies
                        .iter()
                        .map(|(_, dependency)| -> Result<Box<dyn ToSql>, Box<dyn Error>> {
                            Ok(match dependency.column() {
                                ColumnNode::StaticColumn{column: static_column, ..} => {
                                    let metadata = dependency.metadata();

                                    match metadata.ty {
                                        TypeInfo::FixedLen(fixed_len) => match fixed_len {
                                            FixedLenType::Int1 => Box::new(static_column.value().parse::<u8>()?),
                                            FixedLenType::Bit => Box::new(static_column.value().parse::<bool>()?),
                                            FixedLenType::Int2 => Box::new(static_column.value().parse::<i16>()?),
                                            FixedLenType::Int4 => Box::new(static_column.value().parse::<i32>()?),
                                            FixedLenType::Float4 => Box::new(static_column.value().parse::<f32>()?),
                                            FixedLenType::Float8 => Box::new(static_column.value().parse::<f64>()?),
                                            FixedLenType::Int8 => Box::new(static_column.value().parse::<i64>()?),
                                            _ => Err(format!("Unsupported FixedLen column ({}) type: {:?}", static_column.identifier().part(), metadata.ty))?,
                                        },
                                        TypeInfo::VarLenSized(var_len_sized) => match var_len_sized.r#type() {
                                            VarLenType::BigVarChar => Box::new(static_column.value().to_owned()),
                                            VarLenType::NVarchar => Box::new(static_column.value().to_owned()),
                                            _ => Err(format!("Unsupported VarLenSized column ({}) type: {:?}", static_column.identifier().part(), metadata.ty))?,
                                        }
                                        TypeInfo::VarLenSizedPrecision { ty, size: _, precision: _, scale: _ } => match ty {
                                            VarLenType::Decimaln => Box::new(static_column.value().parse::<Decimal>()?),
                                            VarLenType::Numericn => Box::new(static_column.value().parse::<Decimal>()?),
                                            VarLenType::Money => Box::new(static_column.value().parse::<Decimal>()?),
                                            _ => Err(format!("Unsupported VarLenSizedPrecision column ({}) type: {:?}", static_column.identifier().part(), metadata.ty))?,
                                        }
                                        TypeInfo::Xml { .. } => {
                                            Err(format!("Unsupported Xml column ({}) type: {:?}", static_column.identifier().part(), metadata.ty))?
                                        }
                                    }
                                }
                                _ => unreachable!(),
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    let statement_part_set = format!(
                        "t.{target_column} = l_{target_column_unescaped}.{output_column}",
                        target_column = column.unique_identifier().part(),
                        target_column_unescaped = column.unique_identifier().part_unescaped(),
                        output_column = lookup_column.output_column_identifier().part(),
                    );

                    let statement_part_outer_apply = formatdoc!(
                        "
                        OUTER APPLY (
                            SELECT TOP 1 l_inner.{output_column}
                            FROM {lookup_table} l_inner
                            WHERE
                                {column_dependencies_condition}
                                {and_static_column_dependencies_condition}
                                {static_column_dependencies_condition}
                        ) l_{target_column_unescaped}
                        ",
                        target_column_unescaped = column.unique_identifier().part_unescaped(),
                        output_column = lookup_column.output_column_identifier().part(),
                        lookup_table = Table::identifier(lookup_column),
                        column_dependencies_condition = column_dependencies_condition,
                        and_static_column_dependencies_condition =
                        if !static_column_dependencies_condition.is_empty() {
                            "AND"
                        } else {
                            ""
                        },
                        static_column_dependencies_condition = static_column_dependencies_condition,
                    );

                    acc.lookups.push(LookupParts {
                        set: statement_part_set,
                        outer_apply: statement_part_outer_apply,
                    });

                    acc.parameters.append(&mut static_column_dependencies_parameters);
                },
                _ => {},
            };

            Ok(acc)
        }).unwrap();

    assert_eq!(
        static_column_parameter_index,
        target_column_statement_parts.parameters.len(),
        "There must be an equal number of bound SQL parameters & placeholders",
    );

    if !target_column_statement_parts.lookups.is_empty() {
        let statement = formatdoc!(
            "
            UPDATE t
            SET
                {statement_parts_set}
            FROM {temporary_table} t
            {statement_parts_outer_apply}
            ",
            temporary_table = temporary_table.identifier().full(),
            statement_parts_set = target_column_statement_parts
                .lookups
                .iter()
                .map(|l| &l.set)
                .join(",\n    "),
            statement_parts_outer_apply = target_column_statement_parts
                .lookups
                .iter()
                .map(|l| &l.outer_apply)
                .join(""),
        );

        let static_column_dependencies_parameters_refs: Vec<&dyn ToSql> =
            target_column_statement_parts
                .parameters
                .iter()
                .map(|p| &**p)
                .collect();

        trace_sql!(statement);

        client
            .execute(&statement, &static_column_dependencies_parameters_refs)
            .await?;
    }

    Ok(())
}
