use color_eyre::Report;
use log::{LevelFilter, info};
use simplelog::{ColorChoice, TermLogger, TerminalMode};
use sql_bulk_import_profile::identifier::{DatabaseIdentifier, Identifier};
use std::borrow::Cow;
use std::sync::{Arc, OnceLock, Weak};
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, Image};
use thiserror::Error;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, OnceCell};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

// Set environment variable TESTCONTAINERS_COMMAND=keep to keep the containers running.

struct SqlServer;

impl SqlServer {
    pub const USERNAME: &'static str = "sa";
    pub const PASSWORD: &'static str = "_86MUysiFq4gcjt42_";
    const PORT: u16 = 1433;
    const NAME: &'static str = "mcr.microsoft.com/mssql/server";
    const TAG: &'static str = "2025-latest";
    const ENV_VARS: &[(&str, &str)] = &[
        ("ACCEPT_EULA", "Y"),
        ("MSSQL_SA_PASSWORD", Self::PASSWORD),
        ("MSSQL_PID", "Developer"),
    ];
}

impl Image for SqlServer {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn tag(&self) -> &str {
        Self::TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![
            WaitFor::message_on_stdout("SQL Server is now ready for client connections"),
            WaitFor::message_on_stdout("Recovery is complete"),
        ]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        Self::ENV_VARS.iter().copied()
    }
}

#[derive(Debug, Error)]
pub enum RunWithDatabaseError {
    #[error("could not start or retrieve container")]
    RetrieveContainerFailed(#[from] RetrieveContainerError),
    #[error(transparent)]
    CreateClient(#[from] CreateClientError),
    #[error("could not create database: {0}")]
    CreateDatabaseFailed(tiberius::error::Error),
    #[error("could not create schema: {0}")]
    CreateSchemaFailed(tiberius::error::Error),
}

pub async fn run_with_database<F, T>(database: &DatabaseIdentifier, func: F) -> Result<T, Report>
where
    F: AsyncFnOnce(Client<Compat<TcpStream>>) -> Result<T, Report>,
{
    static SHARED_SETUP: OnceLock<()> = OnceLock::new();

    SHARED_SETUP.get_or_init(|| {
        color_eyre::config::HookBuilder::default()
            .install()
            .expect("Eyre hook installation should not fail");

        TermLogger::init(
            LevelFilter::Trace,
            simplelog::ConfigBuilder::new()
                .add_filter_ignore_str("bollard")
                .add_filter_ignore_str("hyper_util")
                .add_filter_ignore_str("testcontainers")
                .add_filter_ignore_str("tiberius")
                .build(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )
        .expect("Logger initialization should not fail");
    });

    let container = retrieve_container()
        .await
        .map_err(RunWithDatabaseError::RetrieveContainerFailed)?;

    // create client for master database
    let mut client = create_client(&container, &"master".parse().unwrap()).await?;

    // create the test database
    client
        .execute(format!("CREATE DATABASE {}", database), &[])
        .await
        .map_err(RunWithDatabaseError::CreateDatabaseFailed)?;

    drop(client);

    // create client for test database
    let mut client = create_client(&container, &database).await?;

    // create the import schema on the test database
    client
        .simple_query("CREATE SCHEMA [import]")
        .await
        .map_err(RunWithDatabaseError::CreateSchemaFailed)?;

    info!("Created and connected to database {}", database);

    func(client).await.map_err(Into::into)
}

#[derive(Debug, Error)]
pub enum RetrieveContainerError {
    #[error(transparent)]
    StartContainer(testcontainers::TestcontainersError),
    #[error("could not retrieve container address")]
    RetrieveContainerAddress(#[source] testcontainers::TestcontainersError),
}

async fn retrieve_container() -> Result<Arc<ContainerAsync<SqlServer>>, RetrieveContainerError> {
    static SQL_SERVER_CONTAINER: OnceCell<Mutex<Weak<ContainerAsync<SqlServer>>>> =
        OnceCell::const_new();

    let mut guard = SQL_SERVER_CONTAINER
        .get_or_init(async || Mutex::new(Weak::new()))
        .await
        .lock()
        .await;

    if let Some(container) = guard.upgrade() {
        Ok(container)
    } else {
        let image = SqlServer;
        let container = Arc::new(
            image
                .start()
                .await
                .map_err(RetrieveContainerError::StartContainer)?,
        );

        info!(
            "Created container (host: {}, port: {})",
            container
                .get_host()
                .await
                .map_err(RetrieveContainerError::RetrieveContainerAddress)?,
            container
                .get_host_port_ipv4(SqlServer::PORT)
                .await
                .map_err(RetrieveContainerError::RetrieveContainerAddress)?
        );

        *guard = Arc::downgrade(&container);

        Ok(container)
    }
}

#[derive(Debug, Error)]
#[error("error creating SQL client ({database}): {source}")]
pub struct CreateClientError {
    database: DatabaseIdentifier,
    #[source]
    source: CreateClientErrorKind,
}

impl CreateClientError {
    fn new(database: DatabaseIdentifier, source: impl Into<CreateClientErrorKind>) -> Self {
        CreateClientError {
            database,
            source: source.into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum CreateClientErrorKind {
    #[error("could not retrieve container address: {0}")]
    RetrieveContainerAddress(#[from] testcontainers::TestcontainersError),
    #[error("could not create TCP stream: {0}")]
    CreateStreamFailed(#[from] std::io::Error),
    #[error("could not connect to SQL server: {0}")]
    ConnectFailed(#[from] tiberius::error::Error),
}

async fn create_client(
    container: &ContainerAsync<SqlServer>,
    database: &DatabaseIdentifier,
) -> Result<Client<Compat<TcpStream>>, CreateClientError> {
    let mut config = Config::new();

    config.host(
        container
            .get_host()
            .await
            .map_err(|err| CreateClientError::new(database.to_owned(), err))?,
    );

    config.port(
        container
            .get_host_port_ipv4(SqlServer::PORT)
            .await
            .map_err(|err| CreateClientError::new(database.to_owned(), err))?,
    );

    config.database(database.part_unescaped());

    config.authentication(AuthMethod::sql_server(
        SqlServer::USERNAME,
        SqlServer::PASSWORD,
    ));

    config.trust_cert();

    let tcp = TcpStream::connect(config.get_addr())
        .await
        .map_err(|err| CreateClientError::new(database.to_owned(), err))?;

    tcp.set_nodelay(true)
        .map_err(|err| CreateClientError::new(database.to_owned(), err))?;

    Client::connect(config, tcp.compat_write())
        .await
        .map_err(|err| CreateClientError::new(database.to_owned(), err))
}
