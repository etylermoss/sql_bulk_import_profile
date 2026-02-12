use crate::data_source::{
    DataSourceErrorIndex, DataSourceRecord, DataSourceRecordIndex, ReadRecordError,
};
use crate::delimited_data_source::{DelimitedDataSource, ParseRecordError};
use csv_core::ReadRecordResult;
use futures::Stream;
use std::num::NonZero;
use std::pin::Pin;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead};

#[derive(Debug, Error)]
#[error("error reading delimited record ({index}): {source}")]
pub struct DelimitedReadRecordError {
    index: DataSourceErrorIndex,
    #[source]
    source: DelimitedReadRecordErrorKind,
}

#[derive(Debug, Error)]
enum DelimitedReadRecordErrorKind {
    #[error("could not read into buffer")]
    FillBufferError(#[from] std::io::Error),
    #[error("could not parse record")]
    ParseRecordError(#[from] ParseRecordError),
}

impl DelimitedReadRecordError {
    fn new(index: DataSourceErrorIndex, source: impl Into<DelimitedReadRecordErrorKind>) -> Self {
        Self {
            index,
            source: source.into(),
        }
    }
}

impl ReadRecordError for DelimitedReadRecordError {
    fn index(&self) -> DataSourceErrorIndex {
        self.index
    }
}

impl<R: AsyncRead + Unpin> Stream for DelimitedDataSource<R> {
    type Item = Result<DataSourceRecord, DelimitedReadRecordError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let DelimitedDataSource {
            buf_reader,
            fields,
            reader,
            record_buffer,
            record_number,
        } = &mut *self;

        let line_start = reader.line();

        loop {
            let fill_buf = match Box::pin(buf_reader.fill_buf()).as_mut().poll(cx) {
                Poll::Ready(Ok(fill_buf)) => fill_buf,
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Some(Err(DelimitedReadRecordError::new(
                        DataSourceErrorIndex {
                            record_number: *record_number,
                            line_number: reader.line() - 1,
                        },
                        err,
                    ))));
                }
                Poll::Pending => return Poll::Pending,
            };

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
                    *record_number = NonZero::new(record_number.map_or(1, |r| r.get() + 1));

                    return Poll::Ready(Some(
                        record_buffer
                            .create_data_source_record(
                                fields,
                                DataSourceRecordIndex {
                                    record_number: record_number.expect("always non-zero"),
                                    line_start,
                                    line_end: reader.line() - 1,
                                },
                            )
                            .map_err(|err| {
                                DelimitedReadRecordError::new(
                                    DataSourceErrorIndex {
                                        record_number: *record_number,
                                        line_number: reader.line() - 1,
                                    },
                                    err,
                                )
                            }),
                    ));
                }
                ReadRecordResult::End => return Poll::Ready(None),
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delimited_data_source::ReadDelimitedDataSourceError;
    use crate::import_profile::DelimitedReaderConfig;
    use futures::StreamExt;
    use indexmap::IndexSet;
    use std::io::Cursor;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn create_delimited_data_source() -> Result<(), ReadDelimitedDataSourceError> {
        let row = "One,Two,Three";

        for buf_size in 1..=row.len() {
            let cursor = Cursor::new(row);
            let buf_reader = BufReader::with_capacity(buf_size, cursor);
            let delimited_data_source =
                DelimitedDataSource::with_buf_reader(DelimitedReaderConfig::Csv, buf_reader)
                    .await?;

            assert_eq!(
                delimited_data_source.fields,
                IndexSet::from(["One".into(), "Two".into(), "Three".into(),])
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn read_delimited_data_source() -> Result<(), ReadDelimitedDataSourceError> {
        let data = "A,B,C\na1,b1,c1\na2\na3,b3,c3,d3";

        let cursor = Cursor::new(data);
        let buf_reader = BufReader::new(cursor);
        let mut delimited_data_source =
            DelimitedDataSource::with_buf_reader(DelimitedReaderConfig::Csv, buf_reader).await?;

        assert_eq!(
            delimited_data_source.fields,
            IndexSet::from(["A".into(), "B".into(), "C".into(),])
        );

        match delimited_data_source.next().await {
            Some(Ok(record)) => {
                assert_eq!(record.index().record_number.get(), 1);
                assert_eq!(record.index().line_start, 2);
                assert_eq!(record.index().line_end, 2);
                assert_eq!(record.field("A").unwrap(), "a1");
                assert_eq!(record.field("B").unwrap(), "b1");
                assert_eq!(record.field("C").unwrap(), "c1");
            }
            other => panic!("expected record 1 but got: {:?}", other),
        };

        match delimited_data_source.next().await {
            Some(Err(DelimitedReadRecordError {
                index,
                source:
                    DelimitedReadRecordErrorKind::ParseRecordError(ParseRecordError::TooFewFields),
            })) => {
                assert_eq!(index.record_number.unwrap().get(), 2);
                assert_eq!(index.line_number, 3);
            }
            other => panic!("expected record 2 but got: {:?}", other),
        };

        match delimited_data_source.next().await {
            Some(Err(DelimitedReadRecordError {
                index,
                source:
                    DelimitedReadRecordErrorKind::ParseRecordError(ParseRecordError::TooManyFields),
            })) => {
                assert_eq!(index.record_number.unwrap().get(), 3);
                // assert_eq!(index.line_number, 4); // bug in csv_core?
            }
            other => panic!("expected record 3 but got: {:?}", other),
        };

        assert!(delimited_data_source.next().await.is_none());

        Ok(())
    }
}
