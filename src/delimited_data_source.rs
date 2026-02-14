mod delimited_data_source_stream;

use crate::data_source::string_map::StringMap;
use crate::data_source::{DataSourceRecord, DataSourceRecordIndex};
use crate::import_profile::{
    DelimitedReaderConfig, DelimitedReaderCustomConfig, Field, Terminator,
};
use csv_core::{ReadRecordResult, Reader};
use indexmap::{IndexMap, IndexSet};
use rustc_hash::FxBuildHasher as BuildHasher;
use std::char::TryFromCharError;
use std::num::NonZero;
use std::path::Path;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use std::sync::Arc;
use thiserror::Error;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

#[derive(Debug)]
pub struct DelimitedDataSource<R> {
    buf_reader: BufReader<R>,
    fields: IndexSet<Arc<str>, BuildHasher>,
    reader: Reader,
    record_buffer: RecordBuffer,
    record_number: Option<NonZero<u64>>,
}

#[derive(Debug)]
struct RecordBuffer {
    output_buffer: Vec<u8>,
    output_used: usize,
    ends_buffer: Vec<usize>,
    ends_used: usize,
}

#[derive(Debug, Error)]
pub enum CreateDelimitedDataSourceError {
    #[error("could not open data source file: {0}")]
    OpenFileError(#[from] std::io::Error),
    #[error("could not read data source file: {0}")]
    ReadError(#[from] ReadDelimitedDataSourceError),
    #[error("missing fields [{}] from header", .0.join(", "))]
    MissingFields(Vec<String>),
}

#[derive(Debug, Error)]
pub enum ReadDelimitedDataSourceError {
    #[error("could not build delimited reader")]
    CreateDelimitedReaderError(#[from] BuildDelimitedReaderError),
    #[error("could not read into buffer")]
    FillBufferError(#[from] std::io::Error),
    #[error("could not parse headers")]
    ParseHeadersError(#[from] Utf8Error),
}

#[derive(Debug, Error)]
pub enum BuildDelimitedReaderError {
    #[error("delimiter character is invalid")]
    Delimiter(#[source] TryFromCharError),
    #[error("terminator character is invalid")]
    Terminator(#[source] TryFromCharError),
    #[error("quote character is invalid")]
    Quote(#[source] TryFromCharError),
    #[error("comment character is invalid")]
    Comment(#[source] TryFromCharError),
    #[error("escape character is invalid")]
    Escape(#[source] TryFromCharError),
}

impl DelimitedDataSource<File> {
    pub async fn new<'fields>(
        path: impl AsRef<Path>,
        fields: impl IntoIterator<Item = &'fields Field>,
        config: DelimitedReaderConfig,
    ) -> Result<Self, CreateDelimitedDataSourceError> {
        let file = File::open(&path).await?;
        let buf_reader = BufReader::new(file);
        let delimited_data_source =
            DelimitedDataSource::with_buf_reader(config, buf_reader).await?;

        let missing_field_names: Vec<String> = fields
            .into_iter()
            .filter_map(|field| {
                if !delimited_data_source.fields.contains(field.name()) {
                    Some(field.name().to_owned())
                } else {
                    None
                }
            })
            .collect();

        if !missing_field_names.is_empty() {
            return Err(CreateDelimitedDataSourceError::MissingFields(
                missing_field_names,
            ));
        }

        Ok(delimited_data_source)
    }
}

impl<R: AsyncRead + Unpin> DelimitedDataSource<R> {
    async fn with_buf_reader(
        config: DelimitedReaderConfig,
        mut buf_reader: BufReader<R>,
    ) -> Result<Self, ReadDelimitedDataSourceError> {
        let mut reader = config.build_reader()?;
        let mut record_buffer = RecordBuffer::default();

        loop {
            break match buf_reader.fill_buf().await {
                Ok(fill_buf) => {
                    let (result, bytes_fill_buf, bytes_output, bytes_end) = reader.read_record(
                        fill_buf,
                        &mut record_buffer.output_buffer[record_buffer.output_used..],
                        &mut record_buffer.ends_buffer[record_buffer.ends_used..],
                    );

                    buf_reader.consume(bytes_fill_buf);

                    record_buffer.output_used += bytes_output;
                    record_buffer.ends_used += bytes_end;

                    match result {
                        ReadRecordResult::InputEmpty => continue,
                        ReadRecordResult::OutputFull => {
                            record_buffer.expand_output();
                            continue;
                        }
                        ReadRecordResult::OutputEndsFull => {
                            record_buffer.expand_ends();
                            continue;
                        }
                        ReadRecordResult::Record => {
                            let fields = record_buffer.ends_buffer[..record_buffer.ends_used]
                                .iter()
                                .scan(0, |prev, &curr| {
                                    Some(
                                        str::from_utf8(&record_buffer.output_buffer[*prev..curr])
                                            .map(|field| {
                                                *prev = curr;
                                                Arc::from(field)
                                            }),
                                    )
                                })
                                .collect::<Result<_, _>>()?;

                            record_buffer.clear();

                            Ok(Self {
                                buf_reader,
                                fields,
                                reader,
                                record_buffer,
                                record_number: None,
                            })
                        }
                        ReadRecordResult::End => {
                            unimplemented!("only parsing header row for now");
                        }
                    }
                }
                Err(err) => Err(ReadDelimitedDataSourceError::FillBufferError(err)),
            };
        }
    }
}

#[derive(Debug, Error)]
enum ParseRecordError {
    #[error("invalid UTF-8 characters in record")]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("too few fields")]
    TooFewFields,
    #[error("too many fields")]
    TooManyFields,
}

impl RecordBuffer {
    fn expand_output(&mut self) {
        self.output_buffer.resize(self.output_used * 2, 0);
    }

    fn expand_ends(&mut self) {
        self.ends_buffer.resize(self.ends_used * 2, 0);
    }

    fn clear(&mut self) -> Vec<u8> {
        let output_buffer = std::mem::take(&mut self.output_buffer);

        self.output_buffer.resize(output_buffer.len(), 0);
        self.output_used = 0;
        self.ends_buffer.fill(0);
        self.ends_used = 0;

        output_buffer
    }

    fn create_data_source_record(
        &mut self,
        field_names: &IndexSet<Arc<str>, BuildHasher>,
        index: DataSourceRecordIndex,
    ) -> Result<DataSourceRecord, ParseRecordError> {
        if self.ends_buffer[..self.ends_used].len() < field_names.len() {
            self.clear();
            return Err(ParseRecordError::TooFewFields);
        }

        let field_indices: Result<IndexMap<Arc<str>, usize, _>, ParseRecordError> = self
            .ends_buffer[..self.ends_used]
            .iter()
            .enumerate()
            .map(|(idx, &curr)| match field_names.get_index(idx) {
                Some(field) => Ok((field.clone(), curr)),
                None => Err(ParseRecordError::TooManyFields),
            })
            .collect();

        let field_data = self.clear();

        let fields = unsafe { StringMap::new(String::from_utf8(field_data)?, field_indices?) };

        Ok(DataSourceRecord::new(fields, index))
    }
}

impl Default for RecordBuffer {
    fn default() -> Self {
        Self {
            output_buffer: vec![0; 1024],
            output_used: 0,
            ends_buffer: vec![0; 64],
            ends_used: 0,
        }
    }
}

impl DelimitedReaderConfig {
    fn build_reader(self) -> Result<Reader, BuildDelimitedReaderError> {
        use BuildDelimitedReaderError::*;
        use csv_core::ReaderBuilder;

        Ok(match self {
            DelimitedReaderConfig::Csv => Reader::new(),
            DelimitedReaderConfig::Txt => ReaderBuilder::new().delimiter(b'\t').build(),
            DelimitedReaderConfig::Custom(DelimitedReaderCustomConfig {
                delimiter,
                terminator,
                quote,
                quoting,
                comment,
                escape,
                double_quote,
            }) => ReaderBuilder::new()
                .delimiter(delimiter.try_into().map_err(Delimiter)?)
                .terminator(terminator.try_into().map_err(Terminator)?)
                .quote(quote.try_into().map_err(Quote)?)
                .quoting(quoting)
                .comment(comment.map(u8::try_from).transpose().map_err(Comment)?)
                .escape(escape.map(u8::try_from).transpose().map_err(Escape)?)
                .double_quote(double_quote)
                .build(),
        })
    }
}

impl TryFrom<Terminator> for csv_core::Terminator {
    type Error = TryFromCharError;

    fn try_from(value: Terminator) -> Result<Self, Self::Error> {
        match value {
            Terminator::CRLF => Ok(csv_core::Terminator::CRLF),
            Terminator::Any(char) => Ok(csv_core::Terminator::Any(char.try_into()?)),
        }
    }
}
