pub(crate) mod import_profile_raw;

use crate::data_source::{
    DataSourceErrorIndex, DataSourceRecord, DataSourceStreamItem, ReadRecordError,
};
use crate::delimited_data_source::{CreateDelimitedDataSourceError, DelimitedDataSource};
use crate::import_options::ImportOptions;
use crate::import_profile::import_profile_raw::ImportProfileRaw;
use crate::preprocess;
use crate::preprocess::{
    LoadPreprocessRuntimeError, PreprocessTransform, PreprocessTransformError,
};
use crate::table_mapper::{CreateTableMapperError, TableMapper};
use crate::xml_data_source::{CreateXmlDataSourceError, XmlDataSource};
use futures::{Stream, TryStreamExt};
use log::warn;
use rustc_hash::FxHashMap as HashMap;
use schemars::JsonSchema;
use serde::Deserialize;
use std::borrow::Cow;
use std::fmt::Debug;
use std::io::Read;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, JsonSchema)]
#[schemars(with = "ImportProfileRaw")]
pub struct ImportProfile {
    name: String,
    description: Option<String>,
    data_source_config: ImportProfileDataSourceConfig,
    table_mappers: Vec<TableMapper>,
}

#[derive(Debug, JsonSchema, Deserialize)]
pub enum ImportProfileDataSourceConfig {
    XmlDataSourceConfig {
        path: PathBuf,
        field_groups: HashMap<String, Vec<Field>>,
        selector: String,
    },
    DelimitedDataSourceConfig {
        path: PathBuf,
        field_groups: HashMap<String, Vec<Field>>,
        reader_config: DelimitedReaderConfig,
    },
}

#[derive(Debug, JsonSchema, Deserialize)]
pub struct Field {
    name: String,
    formatters: Option<Vec<Formatter>>,
    required: Option<Required>,
}

impl Field {
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, JsonSchema, Deserialize)]
pub enum Formatter {
    /// Trim whitespace characters from the field
    Trim,
    /// Uppercase the field
    Uppercase,
    /// Lowercase the field
    Lowercase,
    /// Apply regex substitution to the field
    Regex,
    /// Map values for the field
    Map {
        default: Option<String>,
        mappings: Vec<(String, String)>,
    },
}

impl Formatter {
    pub fn apply<'formatter, 'value>(
        &'formatter self,
        value: Cow<'value, str>,
    ) -> Cow<'value, str> {
        todo!()
    }
}

#[derive(Debug, JsonSchema, Deserialize)]
pub enum Required {
    /// Drop record if field is empty
    Drop,
    /// Error on record if field is empty
    Error,
}

#[derive(Debug, Copy, Clone, JsonSchema, Deserialize)]
pub enum DelimitedReaderConfig {
    Csv,
    Txt,
    Custom(DelimitedReaderCustomConfig),
}

#[derive(Debug, Copy, Clone, JsonSchema, Deserialize)]
pub struct DelimitedReaderCustomConfig {
    pub delimiter: char,
    pub terminator: Terminator,
    #[serde(default)]
    pub quote: char,
    #[serde(default)]
    pub quoting: bool,
    #[serde(default)]
    pub comment: Option<char>,
    #[serde(default)]
    pub escape: Option<char>,
    #[serde(default)]
    pub double_quote: bool,
}

#[derive(Debug, Default, Copy, Clone, JsonSchema, Deserialize)]
pub enum Terminator {
    #[default]
    CRLF,
    Any(char),
}

impl Default for DelimitedReaderCustomConfig {
    fn default() -> Self {
        Self {
            delimiter: Default::default(),
            terminator: Default::default(),
            quote: '"',
            quoting: true,
            comment: None,
            escape: None,
            double_quote: true,
        }
    }
}

#[derive(Debug, Error)]
pub enum CreateImportProfileError {
    #[error("could not deserialize import profile")]
    DeserializationError(
        #[from]
        #[source]
        serde_json::Error,
    ),
    #[error("table mappers empty")]
    TableMappersEmpty,
    #[error("could not create table mapper")]
    CreateTableMapperError(
        #[from]
        #[source]
        CreateTableMapperError,
    ),
    #[error("could not load preprocess script")]
    LoadPreprocessScriptError(
        #[from]
        #[source]
        LoadPreprocessRuntimeError,
    ),
}

impl ImportProfile {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn data_source_config(&self) -> &ImportProfileDataSourceConfig {
        &self.data_source_config
    }

    pub fn table_mappers(&self) -> impl ExactSizeIterator<Item = &TableMapper> {
        self.table_mappers.iter()
    }

    pub async fn new<R: Read>(reader: R) -> Result<Self, CreateImportProfileError> {
        let mut deserializer = serde_json::Deserializer::from_reader(reader);
        let raw = ImportProfileRaw::deserialize(&mut deserializer)
            .map_err(CreateImportProfileError::DeserializationError)?;

        if !raw
            .table_mappers
            .iter()
            .any(|table_mapper| !table_mapper.columns.is_empty())
        {
            return Err(CreateImportProfileError::TableMappersEmpty);
        }

        if let Some(preprocess_script) = &raw.preprocess_script
            && raw.table_mappers.iter().all(|table_mapper| {
                table_mapper
                    .preprocess_function
                    .as_deref()
                    .is_none_or(str::is_empty)
            })
        {
            warn!("Preprocess script '{}' is unused", preprocess_script);
        }

        let preprocess_runtime = raw
            .preprocess_script
            .map(preprocess::load_preprocess_runtime)
            .transpose()?;

        Ok(ImportProfile {
            name: raw.name,
            description: raw.description,
            data_source_config: raw.data_source_config,
            table_mappers: raw
                .table_mappers
                .into_iter()
                .map(|table_mapper| TableMapper::new(table_mapper, preprocess_runtime.as_deref()))
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Debug, Error)]
#[error("error creating data source for file: {data_source_path}")]
pub struct CreateDataSourceError {
    data_source_path: PathBuf,
    #[source]
    source: CreateDataSourceErrorKind,
}

impl CreateDataSourceError {
    fn new(data_source_path: &Path, source: impl Into<CreateDataSourceErrorKind>) -> Self {
        CreateDataSourceError {
            data_source_path: data_source_path.to_owned(),
            source: source.into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum CreateDataSourceErrorKind {
    #[error("unknown field group: {0}")]
    UnknownFieldGroup(String),
    #[error(transparent)]
    Xml(#[from] CreateXmlDataSourceError),
    #[error(transparent)]
    Delimited(#[from] CreateDelimitedDataSourceError),
}

impl ImportProfileDataSourceConfig {
    pub async fn create_data_source<'profile, 'stream>(
        &'profile self,
        table_mapper: &'profile TableMapper,
        import_options: &ImportOptions,
    ) -> Result<Box<dyn Stream<Item = DataSourceStreamItem> + 'stream>, CreateDataSourceError>
    where
        'profile: 'stream,
    {
        let field_group = table_mapper.field_group();
        let preprocess_transform = table_mapper.preprocess_transform();

        match self {
            ImportProfileDataSourceConfig::XmlDataSourceConfig {
                path,
                field_groups,
                selector,
            } => {
                let fields = field_groups.get(field_group).ok_or_else(|| {
                    CreateDataSourceError::new(
                        path,
                        CreateDataSourceErrorKind::UnknownFieldGroup(field_group.to_owned()),
                    )
                })?;

                Self::prepare_stream(
                    XmlDataSource::new(
                        import_options.path_override.as_ref().unwrap_or(path),
                        fields,
                        selector,
                    )
                    .await,
                    path,
                    fields,
                    preprocess_transform,
                )
            }
            ImportProfileDataSourceConfig::DelimitedDataSourceConfig {
                path,
                field_groups,
                reader_config,
            } => {
                let fields = field_groups.get(field_group).ok_or_else(|| {
                    CreateDataSourceError::new(
                        path,
                        CreateDataSourceErrorKind::UnknownFieldGroup(field_group.to_owned()),
                    )
                })?;

                Self::prepare_stream(
                    DelimitedDataSource::new(
                        import_options.path_override.as_ref().unwrap_or(path),
                        fields,
                        *reader_config,
                    )
                    .await,
                    path,
                    fields,
                    preprocess_transform,
                )
            }
        }
    }

    fn prepare_stream<'profile, 'stream, S, E>(
        result: Result<S, impl Into<CreateDataSourceErrorKind>>,
        path: &Path,
        fields: &'profile [Field],
        preprocess_transform: Option<&'profile dyn PreprocessTransform>,
    ) -> Result<Box<dyn Stream<Item = DataSourceStreamItem> + 'stream>, CreateDataSourceError>
    where
        S: Stream<Item = Result<DataSourceRecord, E>> + 'stream,
        E: ReadRecordError,
        'profile: 'stream,
    {
        Ok(Box::new(
            result
                .map_err(|err| CreateDataSourceError::new(path, err.into()))?
                .map_err(|err| -> Box<dyn ReadRecordError> { Box::new(err) })
                // WIP: field formatters / required
                // .try_filter_map(move |record| async move {
                //     let index = record.index();
                //
                //     let blah = fields.iter().map(|field| {
                //         match record.field(field.name()) {
                //             Some(record_field) => {
                //                 let mut record_field: Cow<str> = record_field.into();
                //
                //                 for formatter in &field.formatters {
                //                     record_field = formatter.apply(&*record_field);
                //                 }
                //
                //                 Ok(Some(record_field))
                //             }
                //             None => match field.required {
                //                 Some(Required::Drop) => Ok(None),
                //                 Some(Required::Error) => Err(()),
                //                 None => Ok(None),
                //             }
                //         }
                //     }).collect::<Result<Vec<_>, _>>();
                //
                //     // TODO: use Cow to only clone when needed,
                //
                //     //let record2 = DataSourceRecord::from_iter(blah, index);
                //
                //     Ok(Some(record))
                // })
                .try_filter_map(move |record| async move {
                    if let Some(function) = preprocess_transform {
                        let index = record.index();

                        function
                            .transform(record)
                            .map_err(|err| -> Box<dyn ReadRecordError> {
                                Box::new(PreprocessReadRecordError::new(
                                    DataSourceErrorIndex {
                                        record_number: Some(index.record_number),
                                        line_number: index.line_start,
                                    },
                                    err,
                                ))
                            })
                    } else {
                        Ok(Some(record))
                    }
                }),
        ))
    }
}

#[derive(Debug, Error)]
#[error("error preprocessing record ({index}): {source}")]
pub struct PreprocessReadRecordError {
    index: DataSourceErrorIndex,
    #[source]
    source: PreprocessTransformError,
}

impl PreprocessReadRecordError {
    fn new(index: DataSourceErrorIndex, source: impl Into<PreprocessTransformError>) -> Self {
        Self {
            index,
            source: source.into(),
        }
    }
}

impl ReadRecordError for PreprocessReadRecordError {
    fn index(&self) -> DataSourceErrorIndex {
        self.index
    }
}
