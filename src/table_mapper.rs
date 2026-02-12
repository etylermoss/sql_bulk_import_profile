pub(crate) mod table_mapper_raw;

use crate::identifier::{ColumnIdentifier, ParseIdentifierError, TableIdentifier};
use crate::preprocess::{PreprocessFunctionError, PreprocessRuntime, PreprocessTransform};
use crate::table_mapper::table_mapper_raw::{LookupKeyColumnRaw, TableMapperColumnRaw};
use schemars::JsonSchema;
use serde::Deserialize;
use table_mapper_raw::TableMapperRaw;
use thiserror::Error;

#[derive(Debug)]
#[allow(dead_code)]
pub struct TableMapper {
    name: String,
    field_group: String,
    table_identifier: TableIdentifier,
    delete_mode: DeleteMode,
    delete_action: DeleteAction,
    duplicate_action: DuplicateAction,
    preprocess_transform: Option<Box<dyn PreprocessTransform>>,
    columns: Vec<TableMapperColumn>,
    key_columns: Vec<ColumnIdentifier>,
}

#[derive(Debug, JsonSchema, Deserialize)]
pub enum DeleteMode {
    Partial,
    Full,
}

#[derive(Debug, JsonSchema, Deserialize)]
pub enum DuplicateAction {
    Reject,
    Retain,
    Dump,
    NoCheck,
}

#[derive(Debug, JsonSchema, Deserialize)]
pub enum DeleteAction {
    None,
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub enum TableMapperColumn {
    Static(StaticColumn),
    Parser(ParserColumn),
    Lookup(LookupColumn),
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct StaticColumn {
    column_identifier: ColumnIdentifier,
    map_column: bool,
    value: String,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct ParserColumn {
    column_identifier: ColumnIdentifier,
    map_column: bool,
    field_name: String,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct LookupColumn {
    column_identifier: ColumnIdentifier,
    map_column: bool,
    table_identifier: TableIdentifier,
    output_column_identifier: ColumnIdentifier,
    key_columns: Vec<LookupKeyColumn>,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum LookupKeyColumn {
    ParserKeyColumn(ParserKeyColumn),
    ProcessedKeyColumn(ProcessedKeyColumn),
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct ParserKeyColumn {
    key_column_identifier: ColumnIdentifier,
    field_name: String,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct ProcessedKeyColumn {
    key_column_identifier: ColumnIdentifier,
    column_identifier: ColumnIdentifier,
}

pub trait Table {
    fn identifier(&self) -> &TableIdentifier;
}

pub trait Column {
    fn identifier(&self) -> &ColumnIdentifier;
    fn map(&self) -> bool;
}

// impl for Parser, Static, Lookup
pub trait MappableColumn: Column {
    fn map(&self) -> bool;
}

pub trait FieldColumn: Column {
    fn field_name(&self) -> &str;
}

impl LookupColumn {
    pub fn iter_key_columns(&self) -> impl ExactSizeIterator<Item = &LookupKeyColumn> {
        self.key_columns.iter()
    }

    pub fn output_column_identifier(&self) -> &ColumnIdentifier {
        &self.output_column_identifier
    }
}

impl ParserColumn {
    pub fn new(
        column_identifier: &ColumnIdentifier,
        map_column: bool,
        field_name: &str,
    ) -> ParserColumn {
        ParserColumn {
            column_identifier: column_identifier.clone(),
            map_column,
            field_name: field_name.to_owned(),
        }
    }
}

impl StaticColumn {
    pub fn value(&self) -> &str {
        &self.value
    }
}

#[derive(Debug, Error)]
#[error("could not create table mapper '{table_identifier}'")]
pub struct CreateTableMapperError {
    table_identifier: TableIdentifier,
    #[source]
    source: CreateTableMapperErrorKind,
}

impl CreateTableMapperError {
    pub fn new(
        table_identifier: &TableIdentifier,
        source: impl Into<CreateTableMapperErrorKind>,
    ) -> Self {
        Self {
            table_identifier: table_identifier.clone(),
            source: source.into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum CreateTableMapperErrorKind {
    #[error(transparent)]
    ParseTableMapperIdentifierError(#[from] ParseTableMapperIdentifierError),
    #[error("no preprocess script loaded")]
    NoPreprocessScript,
    #[error("could not find preprocess function '{0}'")]
    FindPreprocessFunction(String),
    #[error("could not create preprocess function '{0}'")]
    CreatePreprocessFunction(String, #[source] PreprocessFunctionError),
}

#[derive(Debug, Error)]
#[error("invalid column identifier {0}: {1}")]
pub struct ParseTableMapperIdentifierError(String, ParseIdentifierError);

impl TableMapper {
    pub fn new(
        raw: TableMapperRaw,
        preprocess_runtime: Option<&dyn PreprocessRuntime>,
    ) -> Result<Self, CreateTableMapperError> {
        let columns: Vec<TableMapperColumn> = raw
            .columns
            .into_iter()
            .map(|table_mapper_column_raw| Ok(
                match table_mapper_column_raw {
                    TableMapperColumnRaw::Static(static_column_raw) => TableMapperColumn::Static(StaticColumn {
                        column_identifier: ColumnIdentifier::with_table(&raw.table_identifier, &static_column_raw.column_identifier)
                            .map_err(|err| ParseTableMapperIdentifierError(static_column_raw.column_identifier, err))?,
                        map_column: static_column_raw.map_column,
                        value: static_column_raw.value,
                    }),
                    TableMapperColumnRaw::Parser(parser_column_raw) => TableMapperColumn::Parser(ParserColumn {
                        column_identifier: ColumnIdentifier::with_table(&raw.table_identifier, &parser_column_raw.column_identifier)
                            .map_err(|err| ParseTableMapperIdentifierError(parser_column_raw.column_identifier, err))?,
                        map_column: parser_column_raw.map_column,
                        field_name: parser_column_raw.field_name,
                    }),
                    TableMapperColumnRaw::Lookup(lookup_column_raw) => {
                        let output_column_identifier = ColumnIdentifier::with_table(&lookup_column_raw.table_identifier, &lookup_column_raw.output_column_identifier)
                            .map_err(|err| ParseTableMapperIdentifierError(lookup_column_raw.output_column_identifier, err))?;

                        let key_columns = lookup_column_raw
                            .key_columns
                            .into_iter()
                            .map(|lookup_key_column_raw| Ok(
                                match lookup_key_column_raw {
                                    LookupKeyColumnRaw::ParserKeyColumn(parser_key_column_raw) => LookupKeyColumn::ParserKeyColumn(ParserKeyColumn {
                                        key_column_identifier: ColumnIdentifier::with_table(&lookup_column_raw.table_identifier, &parser_key_column_raw.key_column_identifier)
                                            .map_err(|err| ParseTableMapperIdentifierError(parser_key_column_raw.key_column_identifier, err))?,
                                        field_name: parser_key_column_raw.field_name,
                                    }),
                                    LookupKeyColumnRaw::ProcessedKeyColumn(processed_key_column_raw) => LookupKeyColumn::ProcessedKeyColumn(ProcessedKeyColumn {
                                        key_column_identifier: ColumnIdentifier::with_table(&lookup_column_raw.table_identifier, &processed_key_column_raw.key_column_identifier)
                                            .map_err(|err| ParseTableMapperIdentifierError(processed_key_column_raw.key_column_identifier, err))?,
                                        column_identifier: ColumnIdentifier::with_table(&raw.table_identifier, &processed_key_column_raw.column_identifier)
                                            .map_err(|err| ParseTableMapperIdentifierError(processed_key_column_raw.column_identifier, err))?,
                                    }),
                                }
                            ))
                            .collect::<Result<Vec<LookupKeyColumn>, ParseTableMapperIdentifierError>>()?;

                        TableMapperColumn::Lookup(LookupColumn {
                            column_identifier: ColumnIdentifier::with_table(&raw.table_identifier, &lookup_column_raw.column_identifier)
                                .map_err(|err| ParseTableMapperIdentifierError(lookup_column_raw.column_identifier, err))?,
                            map_column: lookup_column_raw.map_column,
                            table_identifier: lookup_column_raw.table_identifier,
                            output_column_identifier,
                            key_columns,
                        })
                    }
                }
            ))
            .collect::<Result<_, ParseTableMapperIdentifierError>>().map_err(|err| {
            CreateTableMapperError::new(&raw.table_identifier, err)
        })?;

        let key_columns: Vec<ColumnIdentifier> = raw
            .key_columns
            .into_iter()
            .map(|key_column| {
                ColumnIdentifier::with_table(&raw.table_identifier, &key_column)
                    .map_err(|err| ParseTableMapperIdentifierError(key_column, err))
            })
            .collect::<Result<_, ParseTableMapperIdentifierError>>()
            .map_err(|err| CreateTableMapperError::new(&raw.table_identifier, err))?;

        let preprocess_transform: Option<Box<dyn PreprocessTransform>> = raw
            .preprocess_function
            .as_deref()
            .map(|preprocess_function_name| {
                let preprocess_runtime = preprocess_runtime.ok_or_else(|| {
                    CreateTableMapperError::new(
                        &raw.table_identifier,
                        CreateTableMapperErrorKind::NoPreprocessScript,
                    )
                })?;

                match preprocess_runtime.function(preprocess_function_name) {
                    Ok(Some(preprocess_function)) => Ok(preprocess_function),
                    Ok(None) => Err(CreateTableMapperError::new(
                        &raw.table_identifier,
                        CreateTableMapperErrorKind::FindPreprocessFunction(
                            preprocess_function_name.to_owned(),
                        ),
                    )),
                    Err(err) => Err(CreateTableMapperError::new(
                        &raw.table_identifier,
                        CreateTableMapperErrorKind::CreatePreprocessFunction(
                            preprocess_function_name.to_owned(),
                            err,
                        ),
                    )),
                }
            })
            .transpose()?;

        Ok(TableMapper {
            name: raw.name,
            field_group: raw.field_group,
            table_identifier: raw.table_identifier,
            delete_mode: raw.delete_mode,
            delete_action: raw.delete_action,
            duplicate_action: raw.duplicate_action,
            preprocess_transform,
            columns,
            key_columns,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn field_group(&self) -> &str {
        &self.field_group
    }

    pub fn preprocess_transform(&self) -> Option<&dyn PreprocessTransform> {
        self.preprocess_transform.as_deref()
    }

    pub fn columns(&self) -> impl ExactSizeIterator<Item = &TableMapperColumn> {
        self.columns.iter()
    }

    pub fn key_columns(&self) -> impl ExactSizeIterator<Item = &ColumnIdentifier> {
        self.key_columns.iter()
    }
}

impl Table for TableMapper {
    fn identifier(&self) -> &TableIdentifier {
        &self.table_identifier
    }
}

impl Table for LookupColumn {
    fn identifier(&self) -> &TableIdentifier {
        &self.table_identifier
    }
}

impl Column for StaticColumn {
    fn identifier(&self) -> &ColumnIdentifier {
        &self.column_identifier
    }

    fn map(&self) -> bool {
        self.map_column
    }
}

impl Column for ParserColumn {
    fn identifier(&self) -> &ColumnIdentifier {
        &self.column_identifier
    }

    fn map(&self) -> bool {
        self.map_column
    }
}

impl Column for LookupColumn {
    fn identifier(&self) -> &ColumnIdentifier {
        &self.column_identifier
    }

    fn map(&self) -> bool {
        self.map_column
    }
}

impl Column for ParserKeyColumn {
    fn identifier(&self) -> &ColumnIdentifier {
        &self.key_column_identifier
    }

    fn map(&self) -> bool {
        false
    }
}

impl Column for ProcessedKeyColumn {
    fn identifier(&self) -> &ColumnIdentifier {
        &self.key_column_identifier
    }

    fn map(&self) -> bool {
        false
    }
}

impl ProcessedKeyColumn {
    pub fn column_identifier(&self) -> &ColumnIdentifier {
        &self.column_identifier
    }
}

impl Column for TableMapperColumn {
    fn identifier(&self) -> &ColumnIdentifier {
        match self {
            TableMapperColumn::Static(static_column) => Column::identifier(static_column),
            TableMapperColumn::Parser(parser_column) => Column::identifier(parser_column),
            TableMapperColumn::Lookup(lookup_column) => Column::identifier(lookup_column),
        }
    }

    fn map(&self) -> bool {
        match self {
            TableMapperColumn::Static(static_column) => Column::map(static_column),
            TableMapperColumn::Parser(parser_column) => Column::map(parser_column),
            TableMapperColumn::Lookup(lookup_column) => Column::map(lookup_column),
        }
    }
}

impl FieldColumn for ParserColumn {
    fn field_name(&self) -> &str {
        &self.field_name
    }
}

impl FieldColumn for ParserKeyColumn {
    fn field_name(&self) -> &str {
        &self.field_name
    }
}
