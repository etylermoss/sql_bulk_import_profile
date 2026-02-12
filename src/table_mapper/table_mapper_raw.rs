use crate::identifier::TableIdentifier;
use crate::table_mapper::{DeleteAction, DeleteMode, DuplicateAction};
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, JsonSchema, Deserialize)]
#[serde(rename = "TableMapper")]
pub struct TableMapperRaw {
    pub(crate) name: String,
    pub(crate) field_group: String,
    pub(crate) table_identifier: TableIdentifier,
    pub(crate) delete_mode: DeleteMode,
    pub(crate) delete_action: DeleteAction,
    pub(crate) duplicate_action: DuplicateAction,
    pub(crate) preprocess_function: Option<String>,
    pub(crate) columns: Vec<TableMapperColumnRaw>,
    pub(crate) key_columns: Vec<String>,
}

#[derive(Debug, JsonSchema, Deserialize)]
#[serde(rename = "TableMapperColumn")]
pub enum TableMapperColumnRaw {
    Static(StaticColumnRaw),
    Parser(ParserColumnRaw),
    Lookup(LookupColumnRaw),
}

#[derive(Debug, JsonSchema, Deserialize)]
#[serde(rename = "StaticColumn")]
pub struct StaticColumnRaw {
    pub(super) column_identifier: String,
    pub(super) map_column: bool,
    pub(super) value: String,
}

#[derive(Debug, JsonSchema, Deserialize)]
#[serde(rename = "ParserColumn")]
pub struct ParserColumnRaw {
    pub(super) column_identifier: String,
    pub(super) map_column: bool,
    pub(super) field_name: String,
}

#[derive(Debug, JsonSchema, Deserialize)]
#[serde(rename = "LookupColumn")]
pub struct LookupColumnRaw {
    pub(super) column_identifier: String,
    pub(super) map_column: bool,
    pub(super) table_identifier: TableIdentifier,
    pub(super) output_column_identifier: String,
    pub(super) key_columns: Vec<LookupKeyColumnRaw>,
}

#[derive(Debug, JsonSchema, Deserialize)]
#[serde(rename = "LookupKeyColumn")]
pub enum LookupKeyColumnRaw {
    ParserKeyColumn(ParserKeyColumnRaw),
    ProcessedKeyColumn(ProcessedKeyColumnRaw),
}

#[derive(Debug, JsonSchema, Deserialize)]
#[serde(rename = "ParserKeyColumn")]
pub struct ParserKeyColumnRaw {
    pub(super) key_column_identifier: String,
    pub(super) field_name: String,
}

#[derive(Debug, JsonSchema, Deserialize)]
#[serde(rename = "ProcessedKeyColumn")]
pub struct ProcessedKeyColumnRaw {
    pub(super) key_column_identifier: String,
    pub(super) column_identifier: String,
}
