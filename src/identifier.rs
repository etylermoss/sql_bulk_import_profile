use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer};
use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::str::FromStr;
use thiserror::Error;

macro_rules! impl_identifier_json {
    ($ty:ty) => {
        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>
            {
                let identifier: Cow<'de, str> = Cow::deserialize(deserializer)?;
                Self::from_str(identifier.as_ref()).map_err(serde::de::Error::custom)
            }
        }

        impl JsonSchema for $ty {
            fn schema_name() -> Cow<'static, str> {
                Cow::Borrowed(stringify!($ty))
            }

            fn schema_id() -> Cow<'static, str> {
                Cow::Borrowed(concat!(module_path!(), "::", stringify!($ty)))
            }

            fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
                json_schema!({
                    "type": "string"
                })
            }

            fn inline_schema() -> bool {
                true
            }
        }
    };
}

#[derive(Debug, Clone, Hash, PartialOrd, Ord, PartialEq, Eq)]
pub struct DatabaseIdentifier {
    full: String,
}

impl_identifier_json!(DatabaseIdentifier);

#[derive(Debug, Clone, Hash, PartialOrd, Ord, PartialEq, Eq)]
pub struct SchemaIdentifier {
    full: String,
}

impl_identifier_json!(SchemaIdentifier);

#[derive(Debug, Clone, Hash, PartialOrd, Ord, PartialEq, Eq)]
pub struct TableIdentifier {
    full: String,
    separator_schema_table: usize,
}

impl_identifier_json!(TableIdentifier);

#[derive(Debug, Clone, Hash, PartialOrd, Ord, PartialEq, Eq)]
pub struct ColumnIdentifier {
    full: String,
    separator_schema_table: usize,
    separator_table_column: usize,
}

impl_identifier_json!(ColumnIdentifier);

#[derive(Debug, Error)]
pub enum ParseIdentifierError {
    #[error("invalid character '{0}'")]
    InvalidCharacter(char),
    #[error("invalid brackets")]
    InvalidBrackets,
    #[error("too few parts")]
    TooFewParts,
    #[error("too many parts ({0})")]
    TooManyParts(usize),
}

impl TableIdentifier {
    pub fn with_schema(
        schema: &SchemaIdentifier,
        table: &str,
    ) -> Result<Self, ParseIdentifierError> {
        let table = normalize_identifier_part(table)?;

        let mut full = String::with_capacity(schema.full.len() + 1 + table.len());

        full.push_str(&schema.full);
        full.push('.');
        full.push_str(&table);

        Ok(TableIdentifier {
            full,
            separator_schema_table: schema.full.len(),
        })
    }

    pub fn schema(&self) -> &str {
        &self.full[..self.separator_schema_table]
    }
}

impl ColumnIdentifier {
    pub fn with_table(table: &TableIdentifier, column: &str) -> Result<Self, ParseIdentifierError> {
        let column = normalize_identifier_part(column)?;

        let mut full = String::with_capacity(table.full.len() + 1 + column.len());

        full.push_str(&table.full);
        full.push('.');
        full.push_str(&column);

        Ok(ColumnIdentifier {
            full,
            separator_schema_table: table.separator_schema_table,
            separator_table_column: table.full.len(),
        })
    }

    pub fn schema(&self) -> &str {
        &self.full[..self.separator_schema_table]
    }

    pub fn table(&self) -> &str {
        &self.full[..self.separator_table_column]
    }
}

pub trait Identifier: FromStr<Err = ParseIdentifierError> {
    fn part(&self) -> &str;

    fn full(&self) -> &str;

    fn part_unescaped(&self) -> &str {
        &self.part()[1..self.part().len() - 1]
    }
}

impl Identifier for DatabaseIdentifier {
    fn part(&self) -> &str {
        &self.full
    }

    fn full(&self) -> &str {
        &self.full
    }
}

impl Identifier for SchemaIdentifier {
    fn part(&self) -> &str {
        &self.full
    }

    fn full(&self) -> &str {
        &self.full
    }
}

impl Identifier for TableIdentifier {
    fn part(&self) -> &str {
        &self.full[self.separator_schema_table + 1..]
    }

    fn full(&self) -> &str {
        &self.full
    }
}

impl Identifier for ColumnIdentifier {
    fn part(&self) -> &str {
        &self.full[self.separator_table_column + 1..]
    }

    fn full(&self) -> &str {
        &self.full
    }
}

impl FromStr for DatabaseIdentifier {
    type Err = ParseIdentifierError;

    fn from_str(identifier: &str) -> Result<Self, Self::Err> {
        Ok(DatabaseIdentifier {
            full: normalize_identifier_part(identifier)?.into_owned(),
        })
    }
}

impl FromStr for SchemaIdentifier {
    type Err = ParseIdentifierError;

    fn from_str(identifier: &str) -> Result<Self, Self::Err> {
        Ok(SchemaIdentifier {
            full: normalize_identifier_part(identifier)?.into_owned(),
        })
    }
}

impl FromStr for TableIdentifier {
    type Err = ParseIdentifierError;

    fn from_str(identifier: &str) -> Result<Self, Self::Err> {
        let mut parts = identifier.split('.').map(normalize_identifier_part);

        let part_first = parts.next().transpose()?;
        let part_second = parts.next().transpose()?;
        let part_extra = parts.next().is_some();

        let (schema, table) = match (part_first, part_second, part_extra) {
            (Some(schema), Some(table), false) => (schema, table),
            (Some(table), None, false) => (Cow::from("[dbo]"), table),
            (Some(_), Some(_), true) => {
                return Err(ParseIdentifierError::TooManyParts(parts.count()));
            }
            _ => return Err(ParseIdentifierError::TooFewParts),
        };

        let mut full = String::with_capacity(schema.len() + 1 + table.len());

        full.push_str(&schema);
        full.push('.');
        full.push_str(&table);

        Ok(TableIdentifier {
            full,
            separator_schema_table: schema.len(),
        })
    }
}

impl FromStr for ColumnIdentifier {
    type Err = ParseIdentifierError;

    fn from_str(identifier: &str) -> Result<Self, Self::Err> {
        let mut parts = identifier.split('.').map(normalize_identifier_part);

        let part_first = parts.next().transpose()?;
        let part_second = parts.next().transpose()?;
        let part_third = parts.next().transpose()?;
        let part_extra = parts.next().is_some();

        let (schema, table, column) = match (part_first, part_second, part_third, part_extra) {
            (Some(schema), Some(table), Some(column), false) => (schema, table, column),
            (Some(table), Some(column), None, false) => (Cow::from("[dbo]"), table, column),
            (Some(_), Some(_), Some(_), true) => {
                return Err(ParseIdentifierError::TooManyParts(parts.count()));
            }
            _ => return Err(ParseIdentifierError::TooFewParts),
        };

        let mut full = String::with_capacity(schema.len() + 1 + table.len() + 1 + column.len());

        full.push_str(&schema);
        full.push('.');
        full.push_str(&table);
        full.push('.');
        full.push_str(&column);

        Ok(ColumnIdentifier {
            full,
            separator_schema_table: schema.len(),
            separator_table_column: schema.len() + 1 + table.len(),
        })
    }
}

impl Display for DatabaseIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.full)
    }
}

impl Display for SchemaIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.full)
    }
}

impl Display for TableIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.full)
    }
}

impl Display for ColumnIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.part())
    }
}

impl From<&TableIdentifier> for SchemaIdentifier {
    fn from(value: &TableIdentifier) -> Self {
        SchemaIdentifier {
            full: value.full[..value.separator_schema_table].to_owned(),
        }
    }
}

impl From<&ColumnIdentifier> for SchemaIdentifier {
    fn from(value: &ColumnIdentifier) -> Self {
        SchemaIdentifier {
            full: value.full[..value.separator_schema_table].to_owned(),
        }
    }
}

impl From<&ColumnIdentifier> for TableIdentifier {
    fn from(value: &ColumnIdentifier) -> Self {
        TableIdentifier {
            full: value.full[..value.separator_table_column].to_owned(),
            separator_schema_table: value.separator_schema_table,
        }
    }
}

fn normalize_identifier_part<'a, T>(part: T) -> Result<Cow<'a, str>, ParseIdentifierError>
where
    T: Into<Cow<'a, str>>,
{
    let part = part.into();

    let mut part_chars = part.char_indices().peekable();
    let mut start_bracket = false;
    let mut end_bracket = false;

    while let Some((i, c)) = part_chars.next() {
        if i == 0 && c == '[' {
            start_bracket = true;
        } else if part_chars.peek().is_none() && c == ']' {
            end_bracket = true;
        } else if !c.is_alphanumeric() && !matches!(c, '@' | '$' | '#' | '_') {
            return Err(ParseIdentifierError::InvalidCharacter(c));
        }
    }

    match (start_bracket, end_bracket) {
        (true, false) | (false, true) => Err(ParseIdentifierError::InvalidBrackets),
        (true, true) => Ok(part),
        (false, false) => {
            let mut part_normalized = String::with_capacity(1 + part.len() + 1);

            part_normalized.push('[');
            part_normalized.push_str(&part);
            part_normalized.push(']');

            Ok(part_normalized.into())
        }
    }
}
