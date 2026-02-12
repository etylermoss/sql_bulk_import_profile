use crate::column_graph::IndexedColumnNode;
use crate::identifier::{ColumnIdentifier, Identifier, TableIdentifier};
use crate::table_mapper::Column;
use crate::trace_sql;
use indoc::formatdoc;
use log::trace;
use rustc_hash::FxHashMap as HashMap;
use thiserror::Error;
use tiberius::{Client, ColumnFlag};
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

#[derive(Debug, Error)]
pub enum MergeProcessorError {
    #[error("could not find column target for key column: {0}")]
    KeyColumnUnknownTargetColumn(ColumnIdentifier),
    #[error(transparent)]
    MergeFailed(#[from] tiberius::error::Error),
}

pub async fn execute(
    client: &mut Client<Compat<TcpStream>>,
    target_table: &TableIdentifier,
    temporary_table: &TableIdentifier,
    key_columns: impl IntoIterator<Item = &ColumnIdentifier>,
    columns: impl IntoIterator<Item = IndexedColumnNode<'_>>,
) -> Result<(), MergeProcessorError> {
    let key_columns = key_columns.into_iter().collect::<Vec<_>>();
    let columns = columns.into_iter().collect::<Vec<_>>();

    // TODO: probably missing handling of static columns here, since they are transient, and not
    //       handled via update processor.

    let indexed_key_columns: HashMap<&ColumnIdentifier, &IndexedColumnNode<'_>> = key_columns
        .iter()
        .map(|&key_column| {
            columns
                .iter()
                .find(|column| key_column == column.column().identifier())
                .map(|column| (key_column, column))
                .ok_or_else(|| {
                    MergeProcessorError::KeyColumnUnknownTargetColumn(key_column.to_owned())
                })
        })
        .collect::<Result<_, _>>()?;

    let on_key_columns: String = indexed_key_columns
        .iter()
        .map(|(identifier, indexed_column)| {
            format!(
                "T.{key_column} = S.{column}",
                key_column = identifier.part(),
                column = indexed_column.unique_identifier().part(),
            )
        })
        .collect::<Vec<_>>()
        .join(",\n    ");

    let set_update_columns: String = columns
        .iter()
        .filter_map(|column| {
            if column.metadata().flags == ColumnFlag::Identity
                || indexed_key_columns.contains_key(column.column().identifier())
            {
                None
            } else {
                Some(format!(
                    "T.{target_column} = S.{temporary_column}",
                    target_column = column.column().identifier().part(),
                    temporary_column = column.unique_identifier().part(),
                ))
            }
        })
        .collect::<Vec<_>>()
        .join(",\n        ");

    let insert_columns_target: String = columns
        .iter()
        .filter_map(|column| {
            if column.metadata().flags == ColumnFlag::Identity || column.column().is_transient() {
                None
            } else {
                Some(column.column().identifier().part())
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    let insert_columns_temporary: String = columns
        .iter()
        .filter_map(|column| {
            if column.metadata().flags == ColumnFlag::Identity {
                None
            } else {
                Some(column.unique_identifier().part())
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    let statement = formatdoc!(
        "
        MERGE INTO {target_table} AS T
        USING {temporary_table} AS S
        ON
            {on_key_columns}
        WHEN MATCHED THEN
            UPDATE SET
                {set_update_columns}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_columns_target})
            VALUES ({insert_columns_temporary});
        ",
        target_table = target_table,
        temporary_table = temporary_table,
        on_key_columns = on_key_columns,
        set_update_columns = set_update_columns,
        insert_columns_target = insert_columns_target,
        insert_columns_temporary = insert_columns_temporary,
    );

    trace_sql!(statement);

    client.execute(statement, &[]).await?;

    Ok(())
}
