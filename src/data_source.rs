use hash_map::Iter;
use rustc_hash::FxBuildHasher as BuildHasher;
use rustc_hash::FxHashMap as HashMap;
use std::collections::hash_map;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::num::NonZero;
use std::ops::Range;
use std::sync::Arc;
use thiserror::__private18::AsDynError;

pub type DataSourceStreamItem = Result<DataSourceRecord, Box<dyn ReadRecordError>>;

pub trait ReadRecordError: AsDynError<'static> + Error + Send + Sync + 'static {
    fn index(&self) -> DataSourceErrorIndex;
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
    index: DataSourceRecordIndex,
    field_data: String,
    field_indices: HashMap<Arc<str>, Range<usize>>,
}

impl Display for DataSourceRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}, fields: {{ ", self.index)?;

        for (i, (key, value)) in self.field_indices.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }

            write!(f, "\"{}\": \"{}\"", key, &self.field_data[value.clone()])?;
        }

        write!(f, " }}")?;

        Ok(())
    }
}

impl DataSourceRecord {
    pub fn new(
        field_data: String,
        field_indices: HashMap<Arc<str>, Range<usize>>,
        index: DataSourceRecordIndex,
    ) -> Self {
        DataSourceRecord {
            field_data,
            field_indices,
            index,
        }
    }

    pub fn from_iter<K: AsRef<str>, V: AsRef<str>>(
        fields: impl ExactSizeIterator<Item = (K, V)>,
        index: DataSourceRecordIndex,
    ) -> Self {
        let mut field_data = String::with_capacity(fields.len() * 16);
        let mut field_indices = HashMap::with_capacity_and_hasher(fields.len(), BuildHasher);

        for (k, v) in fields {
            let start = field_data.len();

            field_data.push_str(v.as_ref());
            field_indices.insert(Arc::from(k.as_ref()), start..field_data.len());
        }

        Self {
            field_data,
            field_indices,
            index,
        }
    }

    pub fn field(&self, key: &str) -> Option<&str> {
        self.field_indices
            .get(key)
            .map(|r| &self.field_data[r.clone()])
    }

    pub fn index(&self) -> DataSourceRecordIndex {
        self.index
    }
}

pub struct DataSourceRecordIter<'a> {
    data: &'a str,
    inner: Iter<'a, Arc<str>, Range<usize>>,
}

impl<'a> Iterator for DataSourceRecordIter<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(k, v)| (k.as_ref(), &self.data[v.clone()]))
    }
}

impl<'a> IntoIterator for &'a DataSourceRecord {
    type Item = (&'a str, &'a str);
    type IntoIter = DataSourceRecordIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        DataSourceRecordIter {
            data: self.field_data.as_ref(),
            inner: self.field_indices.iter(),
        }
    }
}
