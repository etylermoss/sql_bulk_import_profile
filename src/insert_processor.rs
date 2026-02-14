use crate::column_graph::{ColumnNode, IndexedColumnNode, UniqueColumnIdentifier};
use crate::data_source::DataSourceRecord;
use crate::identifier::{ColumnIdentifier, Identifier};
use crate::table_mapper::{Column, FieldColumn, ParserColumn, Table};
use crate::temporary_table::TemporaryTable;
use rust_decimal::Decimal;
use std::borrow::Cow;
use thiserror::Error;
use tiberius::{
    BaseMetaDataColumn, BulkLoadRequest, Client, ColumnData, ExecuteResult, FixedLenType, IntoSql,
    TokenRow, TypeInfo, VarLenType,
};
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

pub struct InsertProcessor<'a> {
    target_columns: Vec<(
        &'a ParserColumn,
        &'a UniqueColumnIdentifier,
        &'a BaseMetaDataColumn,
    )>,
    bulk_insert: BulkLoadRequest<'a, Compat<TcpStream>>,
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct CreateInsertProcessorError(#[from] tiberius::error::Error);

#[derive(Debug, Error)]
#[error(transparent)]
pub struct FinalizeInsertProcessorError(#[from] tiberius::error::Error);

#[derive(Debug, Error)]
pub enum ProcessRecordError {
    #[error("missing field '{field}' for column '{column}'")]
    RecordMissingField {
        field: String,
        column: ColumnIdentifier,
    },
    #[error(transparent)]
    SendRowFailed(#[from] tiberius::error::Error),
}

impl<'temp_table, 'connection: 'temp_table, 'column_graph: 'temp_table>
    InsertProcessor<'temp_table>
{
    pub async fn new(
        client: &'connection mut Client<Compat<TcpStream>>,
        temporary_table: &'temp_table TemporaryTable,
        columns: impl Iterator<Item = IndexedColumnNode<'column_graph>>,
    ) -> Result<Self, CreateInsertProcessorError> {
        let target_columns = columns
            .map(|column| match column.column() {
                ColumnNode::ParserColumn {
                    column: parser_column,
                    ..
                } => (parser_column, column.unique_identifier(), column.metadata()),
                _ => unreachable!("Expected only parser columns in the first column graph group."),
            })
            .collect::<Vec<(
                &'column_graph ParserColumn,
                &'column_graph UniqueColumnIdentifier,
                &'column_graph BaseMetaDataColumn,
            )>>();

        let target_columns_refs = target_columns
            .iter()
            .map(|(_, unique_identifier, _)| unique_identifier.part())
            .collect::<Vec<_>>();

        let bulk_insert = client
            .bulk_insert_columns(temporary_table.identifier().full(), &target_columns_refs)
            .await?;

        Ok(InsertProcessor {
            target_columns,
            bulk_insert,
        })
    }

    pub async fn process_record(
        &mut self,
        record: DataSourceRecord,
    ) -> Result<(), ProcessRecordError> {
        let mut row = TokenRow::with_capacity(self.target_columns.len());

        for &(parser_column, _, metadata) in &self.target_columns {
            let field_value = record.get(parser_column.field_name()).ok_or_else(|| {
                ProcessRecordError::RecordMissingField {
                    column: parser_column.identifier().to_owned(),
                    field: parser_column.field_name().to_owned(),
                }
            })?;

            let column_data: ColumnData = match metadata.ty {
                TypeInfo::FixedLen(fixed_len) => match fixed_len {
                    FixedLenType::Null => ColumnData::Bit(None),
                    FixedLenType::Int1 => ColumnData::U8(field_value.parse::<u8>().ok()),
                    FixedLenType::Bit => ColumnData::Bit(field_value.parse::<bool>().ok()),
                    FixedLenType::Int2 => ColumnData::I16(field_value.parse::<i16>().ok()),
                    FixedLenType::Int4 => ColumnData::I32(field_value.parse::<i32>().ok()),
                    FixedLenType::Float4 => ColumnData::F32(field_value.parse::<f32>().ok()),
                    FixedLenType::Float8 => ColumnData::F64(field_value.parse::<f64>().ok()),
                    FixedLenType::Int8 => ColumnData::I64(field_value.parse::<i64>().ok()),
                    _ => panic!(
                        "Unsupported FixedLen column ({}) type: {:?}",
                        parser_column.identifier(),
                        metadata.ty
                    ),
                },
                TypeInfo::VarLenSized(var_len_sized) => match var_len_sized.r#type() {
                    VarLenType::BigVarChar => {
                        ColumnData::String(Some(Cow::from(field_value.to_owned())))
                    }
                    VarLenType::NVarchar => {
                        ColumnData::String(Some(Cow::from(field_value.to_owned())))
                    }
                    _ => panic!(
                        "Unsupported VarLenSized column ({}) type: {:?}",
                        parser_column.identifier(),
                        metadata.ty
                    ),
                },
                TypeInfo::VarLenSizedPrecision {
                    ty,
                    size: _,
                    precision: _,
                    scale: _,
                } => match ty {
                    VarLenType::Decimaln => field_value.parse::<Decimal>().ok().into_sql(),
                    VarLenType::Numericn => field_value.parse::<Decimal>().ok().into_sql(),
                    VarLenType::Money => field_value.parse::<Decimal>().ok().into_sql(),
                    _ => panic!(
                        "Unsupported VarLenSizedPrecision column ({}) type: {:?}",
                        parser_column.identifier(),
                        metadata.ty
                    ),
                },
                TypeInfo::Xml { .. } => {
                    panic!(
                        "Unsupported Xml column ({}) type: {:?}",
                        parser_column.identifier(),
                        metadata.ty
                    );
                }
            };

            row.push(column_data);
        }

        self.bulk_insert.send(row).await?;

        Ok(())
    }

    pub async fn finalize(self) -> Result<ExecuteResult, FinalizeInsertProcessorError> {
        Ok(self.bulk_insert.finalize().await?)
    }
}
