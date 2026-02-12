use clap::{Parser, ValueEnum};
use color_eyre::Report;
use log::LevelFilter;
use simplelog::{ColorChoice, TermLogger, TerminalMode};
use sql_bulk_import_profile::import_executor;
use sql_bulk_import_profile::import_options::ImportOptions;
use sql_bulk_import_profile::import_profile::ImportProfile;
use std::fs::File;
use std::path::PathBuf;
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[tokio::main]
async fn main() -> Result<(), Report> {
    color_eyre::install()?;

    let args = Args::parse();

    TermLogger::init(
        args.log_level.into(),
        simplelog::Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )?;

    let config = Config::from_ado_string(&args.connection_string)?;
    let tcp = TcpStream::connect(config.get_addr()).await?;

    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp.compat_write()).await?;

    let import_profile_file = File::open(&args.import_profile)?;
    let import_profile: ImportProfile = ImportProfile::new(import_profile_file).await?;

    import_executor::import_executor(&mut client, import_profile, args.options).await?;

    Ok(())
}

#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    #[arg(short, long, env = "SBIP_CONNECTION_STRING")]
    connection_string: String,
    #[arg(short, long)]
    import_profile: PathBuf,
    #[command(flatten)]
    options: ImportOptions,
    #[arg(short, long, env = "LOG_LEVEL", default_value = "warn")]
    log_level: LevelFilterArg,
}

#[derive(Debug, Clone, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum LevelFilterArg {
    /// A level lower than all log levels.
    Off,
    /// Corresponds to the `Error` log level.
    Error,
    /// Corresponds to the `Warn` log level.
    Warn,
    /// Corresponds to the `Info` log level.
    Info,
    /// Corresponds to the `Debug` log level.
    Debug,
    /// Corresponds to the `Trace` log level.
    Trace,
}

impl From<LevelFilterArg> for LevelFilter {
    fn from(v: LevelFilterArg) -> Self {
        match v {
            LevelFilterArg::Off => LevelFilter::Off,
            LevelFilterArg::Error => LevelFilter::Error,
            LevelFilterArg::Warn => LevelFilter::Warn,
            LevelFilterArg::Info => LevelFilter::Info,
            LevelFilterArg::Debug => LevelFilter::Debug,
            LevelFilterArg::Trace => LevelFilter::Trace,
        }
    }
}
