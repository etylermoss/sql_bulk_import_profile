# SQL Bulk Import Profile

SQL Server tool for bulk inserting data from an XML / TXT / CSV file.

## Schema

Import profiles should conform to `sql_bulk_import_profile.schema.json` - see examples for guidance.

## Usage

```
Usage: sql_bulk_import_profile [OPTIONS] --connection-string <CONNECTION_STRING> --import-profile <IMPORT_PROFILE>

Options:
  -c, --connection-string <CONNECTION_STRING>
          [env: CONNECTION_STRING=]

  -i, --import-profile <IMPORT_PROFILE>
          

  -l, --log-level <LOG_LEVEL>
          Possible values:
          - off:   A level lower than all log levels
          - error: Corresponds to the `Error` log level
          - warn:  Corresponds to the `Warn` log level
          - info:  Corresponds to the `Info` log level
          - debug: Corresponds to the `Debug` log level
          - trace: Corresponds to the `Trace` log level
          
          [env: LOG_LEVEL=]
          [default: warn]

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version

Data Source:
  -p, --path-override <PATH_OVERRIDE>
          Search for the data source file here instead of from the import profile

  -d, --deletion <DELETION>
          Possible values:
          - retain: Retain the data source file
          - delete: Delete the data source file
          
          [default: retain]

Developer:
      --no-merge
          Do not merge results from the temporary table to the target table

      --no-drop
          Do not drop the temporary table after each table mapper execution

      --no-duplicate-optimization
          Do not merge duplicate columns
```

## TODO

* Formatters / Validators
* Partial / full delete modes
* Partition deletes
* Result formatting
* More examples
* More tests