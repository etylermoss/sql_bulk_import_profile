use crate::column_graph::{ColumnGraph, CreateColumnGraphError};
use crate::data_source::{DataSourceRecordIndex, DataSourceStreamItem, ReadRecordError};
use crate::identifier::{ColumnIdentifier, Identifier, ParseIdentifierError, TableIdentifier};
use crate::import_options::ImportOptions;
use crate::import_profile::{CreateDataSourceError, ImportProfile};
use crate::insert_processor::{
    CreateInsertProcessorError, FinalizeInsertProcessorError, InsertProcessor, ProcessRecordError,
};
use crate::merge_processor::MergeProcessorError;
use crate::table_mapper::{Table, TableMapper, TableMapperColumn};
use crate::temporary_table::{CreateTemporaryTableError, TemporaryTable};
use crate::update_processor::UpdateProcessorError;
use crate::{merge_processor, update_processor};
use futures::{Stream, StreamExt};
use itertools::{Itertools, Position};
use log::{error, info, warn};
use rustc_hash::{FxBuildHasher as BuildHasher, FxHashMap as HashMap, FxHashSet as HashSet};
use std::iter::once;
use std::pin::Pin;
use thiserror::Error;
use tiberius::{BaseMetaDataColumn, Client};
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

#[derive(Debug, Error)]
#[error("error executing import profile '{import_profile_name}'")]
pub struct ImportExecutorError {
    import_profile_name: String,
    #[source]
    source: ImportExecutorErrorKind,
}

impl ImportExecutorError {
    fn new(import_profile: &ImportProfile, source: impl Into<ImportExecutorErrorKind>) -> Self {
        Self {
            import_profile_name: import_profile.name().to_string(),
            source: source.into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ImportExecutorErrorKind {
    #[error("table '{0}' metadata could not be retrieved")]
    TableMetadataRetrievalFailed(TableIdentifier, #[source] tiberius::error::Error),
    #[error("column graph could not be created")]
    CreateColumnGraph(#[from] CreateColumnGraphError),
    #[error("data source could not be created")]
    CreateDataSource(#[from] CreateDataSourceError),
    #[error("temporary table could not be created")]
    CreateTemporaryTable(#[from] CreateTemporaryTableError),
    #[error("table mapper could not be executed")]
    ExecuteTableMapper(#[from] ExecuteTableMapperError),
    #[error("temporary table could not be finalized")]
    FinalizeTemporaryTable(#[source] tiberius::error::Error),
}

pub async fn import_executor(
    client: &mut Client<Compat<TcpStream>>,
    import_profile: ImportProfile,
    import_options: ImportOptions,
) -> Result<(), ImportExecutorError> {
    let table_names = import_profile
        .table_mappers()
        .flat_map(|table_mapper| {
            table_mapper
                .columns()
                .filter_map(|column| match column {
                    TableMapperColumn::Lookup(lookup_column) => {
                        Some(Table::identifier(lookup_column))
                    }
                    _ => None,
                })
                .chain(once(Table::identifier(table_mapper)))
        })
        .collect::<HashSet<_>>();

    let mut table_metadata = HashMap::<
        &TableIdentifier,
        HashMap<ColumnIdentifier, BaseMetaDataColumn>,
    >::with_capacity_and_hasher(table_names.len(), BuildHasher);

    for table_name in table_names {
        table_metadata.insert(
            table_name,
            client
                .column_metadata(table_name.full(), &["*"])
                .await
                .map_err(|err| ImportExecutorError::new(
                    &import_profile,
                    ImportExecutorErrorKind::TableMetadataRetrievalFailed(table_name.to_owned(), err)
                ))?
                .into_iter()
                .map(|metadata| Ok((
                    ColumnIdentifier::with_table(table_name, &metadata.col_name)?,
                    metadata.base,
                )))
                .collect::<Result<HashMap<ColumnIdentifier, BaseMetaDataColumn>, ParseIdentifierError>>()
                .expect("Metadata column identifiers should be valid"),
        );
    }

    let data_source_config = import_profile.data_source_config();

    for table_mapper in import_profile.table_mappers() {
        let mut data_source: Pin<Box<dyn Stream<Item = DataSourceStreamItem>>> = data_source_config
            .create_data_source(table_mapper, &import_options)
            .await
            .map_err(|err| ImportExecutorError::new(&import_profile, err))?
            .into();

        let column_graph = ColumnGraph::new(table_mapper, &table_metadata, &import_options)
            .map_err(|err| ImportExecutorError::new(&import_profile, err))?;

        let temporary_table = TemporaryTable::new(client, table_mapper.identifier(), &column_graph)
            .await
            .map_err(|err| ImportExecutorError::new(&import_profile, err))?;

        let result = execute_table_mapper(
            client,
            &mut data_source,
            &column_graph,
            &temporary_table,
            table_mapper,
        )
        .await;

        if let Err(err) = temporary_table.finalize(client, &import_options).await {
            return Err(ImportExecutorError::new(
                &import_profile,
                ImportExecutorErrorKind::FinalizeTemporaryTable(err),
            ));
        };

        if let Err(err) = result {
            return Err(ImportExecutorError::new(&import_profile, err));
        }
    }

    Ok(())
}

#[derive(Debug, Error)]
pub enum ExecuteTableMapperError {
    #[error("insert processor could not be created")]
    CreateInsertProcessor(
        #[from]
        #[source]
        CreateInsertProcessorError,
    ),
    #[error("record could not be read")]
    ReadRecordFailed(
        #[from]
        #[source]
        Box<dyn ReadRecordError>,
    ),
    #[error("record could not be executed")]
    ExecuteRecordFailed(
        #[from]
        #[source]
        ExecuteRecordError,
    ),
    #[error("insert processor could not be finalized")]
    FinalizeInsertProcessor(
        #[from]
        #[source]
        FinalizeInsertProcessorError,
    ),
    #[error("update processor failed")]
    UpdateProcessor(
        #[from]
        #[source]
        UpdateProcessorError,
    ),
    #[error("merge processor failed")]
    MergeProcessor(
        #[from]
        #[source]
        MergeProcessorError,
    ),
}

async fn execute_table_mapper<'table_mapper, 'stream>(
    client: &mut Client<Compat<TcpStream>>,
    data_source: &mut Pin<Box<dyn Stream<Item = DataSourceStreamItem> + 'stream>>,
    column_graph: &ColumnGraph,
    temporary_table: &TemporaryTable,
    table_mapper: &'table_mapper TableMapper,
) -> Result<(), ExecuteTableMapperError>
where
    'table_mapper: 'stream,
{
    info!(
        "Created temporary table {} for table mapper {}",
        temporary_table.identifier(),
        table_mapper.name()
    );

    for (position, (group_index, group)) in column_graph.groups().enumerate().with_position() {
        if matches!(position, Position::First | Position::Only) {
            let mut insert_processor = InsertProcessor::new(client, temporary_table, group).await?;

            info!(
                "Insert processor created for table mapper {}",
                table_mapper.name()
            );

            let insert_error = loop {
                match data_source.next().await {
                    Some(Ok(record)) => {
                        let index = record.index();

                        if let Err(err) = insert_processor.process_record(record).await.map_err(
                            |err| {
                                if let ProcessRecordError::RecordMissingField { .. } = &err {
                                    warn!(
                                        "Insert processor missing field for table mapper {}: {}",
                                        table_mapper.name(),
                                        err,
                                    );
                                }
                                err
                            },
                        ) {
                            break Err(ExecuteTableMapperError::ExecuteRecordFailed(
                                ExecuteRecordError::new(index, err),
                            ));
                        }
                    }
                    Some(Err(err)) => break Err(ExecuteTableMapperError::ReadRecordFailed(err)),
                    None => break Ok(()),
                }
            }
            .err();

            let result = insert_processor.finalize().await?;

            if let Some(err) = insert_error {
                return Err(err);
            }

            info!(
                "Insert processor completed for table mapper {}, affected {} rows",
                table_mapper.name(),
                result.total()
            );
        } else {
            update_processor::execute(client, temporary_table, group, column_graph).await?;

            info!(
                "Update processor completed for table mapper {}, group {}",
                table_mapper.name(),
                group_index,
            );
        }
    }

    merge_processor::execute(
        client,
        table_mapper.identifier(),
        temporary_table.identifier(),
        table_mapper.key_columns(),
        column_graph.target_columns(),
    )
    .await?;

    Ok(())
}

#[derive(Debug, Error)]
#[error("error executing record ({index})")]
pub struct ExecuteRecordError {
    index: DataSourceRecordIndex,
    #[source]
    source: ProcessRecordError,
}

impl ExecuteRecordError {
    fn new(index: DataSourceRecordIndex, source: impl Into<ProcessRecordError>) -> Self {
        Self {
            index,
            source: source.into(),
        }
    }
}
