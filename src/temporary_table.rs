use crate::column_graph::ColumnGraph;
use crate::identifier::{Identifier, SchemaIdentifier, TableIdentifier};
use crate::import_options::ImportOptions;
use crate::table_mapper::Table;
use crate::trace_sql;
use indoc::formatdoc;
use itertools::{Itertools, Position};
use log::trace;
use thiserror::Error;
use tiberius::{Client, ColumnFlag};
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

#[derive(Debug)]
pub struct TemporaryTable {
    table_identifier: TableIdentifier,
}

#[derive(Debug, Error)]
pub enum CreateTemporaryTableError {
    #[error("no non transient columns for temporary table creation")]
    NoNonTransientColumns,
    #[error("temporary table could not be created: {0}")]
    CreateTableFailed(#[from] tiberius::error::Error),
}

impl TemporaryTable {
    pub async fn new(
        client: &mut Client<Compat<TcpStream>>,
        target_table: &TableIdentifier,
        column_graph: &ColumnGraph,
    ) -> Result<TemporaryTable, CreateTemporaryTableError> {
        let schema: SchemaIdentifier = "[import]".parse().unwrap();
        let table_identifier = TableIdentifier::with_schema(&schema, target_table.part_unescaped())
            .expect("Temporary table identifier should be valid");

        let columns = column_graph
            .groups()
            .with_position()
            .flat_map(|(position, group)| {
                group.filter_map(move |node| {
                    if !node.column().is_transient() {
                        let metadata = node.metadata();
                        let nullable = metadata.flags.contains(ColumnFlag::Nullable)
                            || matches!(position, Position::Middle | Position::Last);

                        Some(format!(
                            "{column_name} {column_type} {nullable}",
                            column_name = node.unique_identifier().part(),
                            column_type = metadata.ty,
                            nullable = if nullable { "NULL" } else { "NOT NULL" },
                        ))
                    } else {
                        None
                    }
                })
            })
            .collect::<Vec<_>>();

        if columns.is_empty() {
            return Err(CreateTemporaryTableError::NoNonTransientColumns);
        }

        let statement = formatdoc!(
            "
            IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{table_name}') AND type in (N'U'))
            BEGIN
                DROP TABLE {table_name}
            END

            CREATE TABLE {table_name} (
                {columns}
            )
            ",
            table_name = table_identifier.full(),
            columns = columns.join(",\n    "),
        );

        trace_sql!(statement);

        client.execute(statement, &[]).await?;

        Ok(TemporaryTable { table_identifier })
    }

    pub async fn finalize(
        self,
        client: &mut Client<Compat<TcpStream>>,
        import_options: &ImportOptions,
    ) -> Result<(), tiberius::error::Error> {
        if !import_options.no_drop {
            let statement = formatdoc!(
                "
                DROP TABLE {table_name}
                ",
                table_name = self.table_identifier.full(),
            );

            trace_sql!(statement);

            client.execute(statement, &[]).await?;
        }

        Ok(())
    }
}

impl Table for TemporaryTable {
    fn identifier(&self) -> &TableIdentifier {
        &self.table_identifier
    }
}
