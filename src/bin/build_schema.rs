use schemars::schema_for;
use serde::Serialize;
use sql_bulk_import_profile::import_profile::ImportProfile;
use std::error::Error;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let output_path = Path::new("sql_bulk_import_profile.schema.json");
    let output_file = std::fs::File::create(output_path)?;
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b"\t");
    let mut serializer = serde_json::Serializer::with_formatter(output_file, formatter);

    schema_for!(ImportProfile).serialize(&mut serializer)?;

    println!("Schema written to {}.", output_path.display());

    Ok(())
}
