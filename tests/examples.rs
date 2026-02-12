mod csv;
mod sql_server;
mod txt;
mod xml;

use crate::sql_server::run_with_database;
use color_eyre::Report;
use sql_bulk_import_profile::import_executor;
use sql_bulk_import_profile::import_options::ImportOptions;
use sql_bulk_import_profile::import_profile::ImportProfile;
use std::io::Cursor;
use tiberius::{Client, IntoRow, Row};
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

macro_rules! include_example {
    ($path:literal) => {
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/examples/", $path))
    };
}

#[tokio::test]
async fn currencies_import_profile() -> Result<(), Report> {
    run_with_database(
        &"currencies_import_profile".parse()?,
        async |mut client: Client<Compat<TcpStream>>| {
            let import_profile = ImportProfile::new(Cursor::new(include_example!(
                "currencies_import_profile.json"
            )))
            .await?;

            client
                .execute(
                    "
                    CREATE TABLE [Currency]
                    (
                         ID   INT PRIMARY KEY IDENTITY(1, 1) NOT NULL
                        ,Code NVARCHAR(3) NOT NULL
                        ,Name NVARCHAR(255) NOT NULL
                    );

                    INSERT INTO [Currency] (Code, Name)
                    VALUES
                         ('EUR', 'Euro')
                    ",
                    &[],
                )
                .await?;

            import_executor::import_executor(&mut client, import_profile, ImportOptions::default())
                .await?;

            let result = client
                .simple_query("SELECT [ID], [Code], [Name] FROM [dbo].[Currency]")
                .await?
                .into_first_result()
                .await?;

            let expected = vec![
                (1, "EUR", "EURO"),                // Unchanged
                (2, "GBP", "GREAT BRITISH POUND"), // Insert (preprocessed)
                (3, "CHF", "CONFOEDERATIO HELVETICA FRANC"), // Insert (preprocessed)
                                                   //(4, "PLN", "POLSKI ZŁOTY"),                  // Insert (preprocessed)
            ];

            itertools::assert_equal(
                expected.into_iter().map(IntoRow::into_row),
                result.iter().map(Row::data).cloned(),
            );

            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn companies_import_profile() -> Result<(), Report> {
    run_with_database(
        &"companies_import_profile".parse()?,
        async |mut client: Client<Compat<TcpStream>>| {
            let import_profile = ImportProfile::new(Cursor::new(include_example!(
                "companies_import_profile.json"
            )))
            .await?;

            client
                .execute(
                    "
                    CREATE TABLE [Country]
                    (
                         ID   INT PRIMARY KEY IDENTITY(1, 1) NOT NULL
                        ,Code NVARCHAR(255) NOT NULL
                        ,Name NVARCHAR(255) NOT NULL
                    );

                    INSERT INTO [Country] (Code, Name)
                    VALUES
                         ('FR', 'France')
                        ,('DE', 'Germany')
                        ,('PL', 'Poland')

                    CREATE TABLE [Company]
                    (
                         ID        INT PRIMARY KEY IDENTITY(1, 1) NOT NULL
                        ,Code      NVARCHAR(4) NOT NULL
                        ,Name      NVARCHAR(255) NOT NULL
                        ,CountryID INT NOT NULL FOREIGN KEY REFERENCES [Country] (ID)
                    );

                    INSERT INTO [Company] (Code, Name, CountryID)
                    VALUES
                         ('AAPL', 'Apple', 1)
                        ,('MSFT', 'Microsoft', 2)
                        ,('GOOG', 'Google', 3)
                    ",
                    &[],
                )
                .await?;

            import_executor::import_executor(&mut client, import_profile, ImportOptions::default())
                .await?;

            let result = client
                .simple_query("SELECT [ID], [Code], [Name], [CountryID] FROM [dbo].[Company]")
                .await?
                .into_first_result()
                .await?;

            let expected = vec![
                (1, "AAPL", "Apple", 1),             // Unchanged
                (2, "MSFT", "Microsoft", 1),         // Update CountryID
                (3, "GOOG", "Alphabet (Google)", 3), // Update Name
                (4, "NVDA", "NVIDIA", 2),            // Insert
            ];

            itertools::assert_equal(
                expected.into_iter().map(IntoRow::into_row),
                result.iter().map(Row::data).cloned(),
            );

            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn countries_import_profile() -> Result<(), Report> {
    return Ok(());

    run_with_database(
        &"countries_import_profile".parse()?,
        async |mut client: Client<Compat<TcpStream>>| {
            let import_profile = ImportProfile::new(Cursor::new(include_example!(
                "countries_import_profile.json"
            )))
            .await?;

            client
                .execute(
                    "
                    CREATE TABLE [Continent]
                    (
                         ID INT IDENTITY(1,1) PRIMARY KEY
                        ,Name NVARCHAR(100) NOT NULL
                    );

                    CREATE TABLE [Country]
                    (
                         ID INT IDENTITY(1,1) PRIMARY KEY
                        ,Code CHAR(3) NOT NULL UNIQUE
                        ,Name NVARCHAR(150) NOT NULL
                        ,ContinentID INT NOT NULL FOREIGN KEY REFERENCES [Continent] (ID)
                    );

                    CREATE TABLE [Border]
                    (
                         A_CountryID INT NOT NULL FOREIGN KEY REFERENCES [Country] (ID)
                        ,B_CountryID INT NOT NULL FOREIGN KEY REFERENCES [Country] (ID)
                        ,CONSTRAINT PK_Border PRIMARY KEY (A_CountryID, B_CountryID)
                        ,CONSTRAINT CHK_Border_NoSelf CHECK (A_CountryID <> B_CountryID)
                        ,CONSTRAINT CHK_Border_Order CHECK (A_CountryID < B_CountryID)
                    );

                    CREATE TABLE [Mountain]
                    (
                         ID INT IDENTITY(1,1) PRIMARY KEY
                        ,Name NVARCHAR(150) NOT NULL
                        ,CountryID INT NOT NULL FOREIGN KEY REFERENCES [Country] (ID)
                        ,Height DECIMAL(19,6) NOT NULL
                    );
                    ",
                    &[],
                )
                .await?;

            import_executor::import_executor(&mut client, import_profile, ImportOptions::default())
                .await?;

            // TODO implement test

            Ok(())
        },
    )
    .await
}
