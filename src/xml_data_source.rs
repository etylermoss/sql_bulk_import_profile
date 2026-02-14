mod xml_data_source_stream;

use crate::import_profile::Field;
use arrayvec::ArrayVec;
use indexmap::{IndexMap, IndexSet};
use quick_xml::Reader;
use rustc_hash::FxBuildHasher as BuildHasher;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::fs::File;
use tokio::io::BufReader;

#[derive(Debug, Error)]
pub enum CreateXmlDataSourceError {
    #[error("could not open data source file: {0}")]
    OpenFileError(PathBuf, #[source] std::io::Error),
    #[error("invalid selector: {0}")]
    InvalidSelector(String),
    #[error("invalid fields")]
    InvalidFields,
}

#[derive(Debug)]
pub struct XmlDataSource<R> {
    reader: Reader<BufReader<R>>,
    buffer: Vec<u8>,
    selector_parts: ArrayVec<Box<str>, 8>,
    fields: IndexSet<Arc<str>, BuildHasher>,
    depth: usize,
    record_number: Option<NonZeroU64>,
    line_number: u64,
    current_record_state: CurrentRecordState,
}

#[derive(Debug)]
struct CurrentRecordState {
    field_data: String,
    field_indices: IndexMap<Arc<str>, usize, BuildHasher>,
    field_index: Option<usize>,
    field_start: usize,
    line_start: u64,
}

impl CurrentRecordState {
    fn new(fields_length: usize) -> Self {
        Self {
            field_data: String::new(),
            field_indices: IndexMap::with_capacity_and_hasher(fields_length, BuildHasher),
            field_index: None,
            field_start: 0,
            line_start: 0,
        }
    }
}

impl XmlDataSource<File> {
    pub async fn new<'fields>(
        path: impl AsRef<Path>,
        fields: impl IntoIterator<Item = &'fields Field>,
        selector: &str,
    ) -> Result<Self, CreateXmlDataSourceError> {
        let file = File::open(&path).await.map_err(|err| {
            CreateXmlDataSourceError::OpenFileError(path.as_ref().to_owned(), err)
        })?;

        let buf_reader = BufReader::new(file);
        let reader = Reader::from_reader(buf_reader);

        let selector_parts: ArrayVec<Box<str>, 8> = selector
            .split('/')
            .filter_map(|selector_part| {
                let selector_part = selector_part.trim();

                if !selector_part.is_empty() {
                    Some(Box::from(selector_part))
                } else {
                    None
                }
            })
            .collect();

        if selector_parts.is_empty() {
            return Err(CreateXmlDataSourceError::InvalidSelector(
                selector.to_owned(),
            ));
        }

        let fields: IndexSet<Arc<str>, BuildHasher> = fields
            .into_iter()
            .filter_map(|field| {
                let field = field.name();

                if !field.is_empty() {
                    Some(Arc::from(field))
                } else {
                    None
                }
            })
            .collect();

        if fields.is_empty() {
            return Err(CreateXmlDataSourceError::InvalidFields);
        }

        let current_record_state = CurrentRecordState::new(fields.len());

        Ok(XmlDataSource {
            reader,
            buffer: Vec::new(),
            selector_parts,
            fields,
            depth: 0,
            record_number: None,
            line_number: 0,
            current_record_state,
        })
    }
}
