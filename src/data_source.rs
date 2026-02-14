pub mod string_map;

use crate::data_source::string_map::{StringMap, StringMapIter};
use rustc_hash::FxBuildHasher;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::num::NonZero;
use std::sync::Arc;

pub type DataSourceStreamItem = Result<DataSourceRecord, Box<dyn ReadRecordError>>;

pub trait ReadRecordError: Error + Display + Debug + Send + Sync + 'static {
    fn index(&self) -> DataSourceErrorIndex;
}

impl Error for Box<dyn ReadRecordError> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.as_ref().source()
    }
}

#[derive(Debug, Copy, Clone)]
pub struct DataSourceRecordIndex {
    pub record_number: NonZero<u64>,
    pub line_start: u64,
    pub line_end: u64,
}

impl Display for DataSourceRecordIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "record: {}, line: {} - {}",
            self.record_number, self.line_start, self.line_end,
        )
    }
}

#[derive(Debug, Copy, Clone)]
pub struct DataSourceErrorIndex {
    pub record_number: Option<NonZero<u64>>,
    pub line_number: u64,
}

impl Display for DataSourceErrorIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(record_number) = self.record_number {
            write!(f, "record: {}, line: {}", record_number, self.line_number,)
        } else {
            write!(f, "record: N/A, line: {}", self.line_number)
        }
    }
}

#[derive(Debug)]
pub struct DataSourceRecord {
    fields: StringMap<Arc<str>>,
    index: DataSourceRecordIndex,
}

impl Display for DataSourceRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}, fields: {{ {} }}", self.index, self.fields)?;

        Ok(())
    }
}

impl DataSourceRecord {
    pub fn new(
        fields: StringMap<Arc<str>>,
        index: DataSourceRecordIndex,
    ) -> Self {
        DataSourceRecord {
            fields,
            index,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.fields.get(key)
    }

    #[inline(always)]
    pub fn index(&self) -> DataSourceRecordIndex {
        self.index
    }
}

impl<'a> IntoIterator for &'a DataSourceRecord {
    type Item = (&'a Arc<str>, &'a str);
    type IntoIter = StringMapIter<'a, Arc<str>, FxBuildHasher>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.into_iter()
    }
}
