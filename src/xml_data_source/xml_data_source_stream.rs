use crate::data_source::string_map::StringMap;
use crate::data_source::{
    DataSourceErrorIndex, DataSourceRecord, DataSourceRecordIndex, ReadRecordError,
};
use crate::xml_data_source::{CurrentRecordState, XmlDataSource};
use futures::Stream;
use memchr::memchr_iter;
use quick_xml::events::Event::{CData, Empty, End, Eof, GeneralRef, Start, Text};
use std::num::NonZero;
use std::pin::Pin;
use std::str::Utf8Error;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::io::AsyncRead;

#[derive(Debug, Error)]
#[error("error reading XML record ({index})")]
pub struct XmlReadRecordError {
    index: DataSourceErrorIndex,
    #[source]
    source: XmlReadRecordErrorKind,
}

#[derive(Debug, Error)]
enum XmlReadRecordErrorKind {
    #[error("error reading XML")]
    XmlError(#[from] quick_xml::Error),
    #[error("error decoding XML")]
    XmlDecodingError(#[from] quick_xml::encoding::EncodingError),
    #[error("unexpected start tag: {0}")]
    UnexpectedStartTag(String),
    #[error("unknown field: {0}")]
    UnknownField(String),
    #[error("error interpreting UTF-8")]
    Utf8Error(#[from] Utf8Error),
}

impl XmlReadRecordError {
    fn new(index: DataSourceErrorIndex, source: impl Into<XmlReadRecordErrorKind>) -> Self {
        Self {
            index,
            source: source.into(),
        }
    }
}

impl ReadRecordError for XmlReadRecordError {
    fn index(&self) -> DataSourceErrorIndex {
        self.index
    }
}

impl<R: AsyncRead + Unpin> Stream for XmlDataSource<R> {
    type Item = Result<DataSourceRecord, XmlReadRecordError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        #[inline(always)]
        fn str_from_utf8(
            bytes: &[u8],
            index: DataSourceErrorIndex,
        ) -> Result<&str, XmlReadRecordError> {
            str::from_utf8(bytes).map_err(|err| XmlReadRecordError::new(index, err))
        }

        #[inline(always)]
        fn buffer_lines(buffer: &[u8]) -> u64 {
            memchr_iter(b'\n', buffer).count() as u64
        }

        let XmlDataSource {
            reader,
            buffer,
            selector_parts,
            fields,
            depth,
            record_number,
            line_number,
            current_record_state:
                CurrentRecordState {
                    field_data: current_data,
                    field_indices: current_field_indices,
                    field_index: current_field_index,
                    field_start: current_field_start,
                    line_start: current_line_start,
                },
        } = &mut *self;

        loop {
            buffer.clear();

            let event_result = Box::pin(reader.read_event_into_async(buffer))
                .as_mut()
                .poll(cx);

            let event = match event_result {
                Poll::Ready(Ok(event)) => event,
                Poll::Ready(Err(err)) => {
                    *line_number += buffer_lines(buffer);

                    return Poll::Ready(Some(Err(XmlReadRecordError::new(
                        DataSourceErrorIndex {
                            record_number: *record_number,
                            line_number: *line_number,
                        },
                        XmlReadRecordErrorKind::XmlError(err),
                    ))));
                }
                Poll::Pending => return Poll::Pending,
            };

            *line_number += buffer_lines(&event);

            let index = DataSourceErrorIndex {
                record_number: *record_number,
                line_number: *line_number + 1,
            };

            match event {
                Start(start) => {
                    *depth += 1;

                    let local_name = start.local_name().into_inner();

                    if *depth <= selector_parts.len() {
                        if *depth == selector_parts.len() {
                            *current_line_start = *line_number + 1;
                        }

                        if selector_parts[*depth - 1].as_bytes() != local_name {
                            return Poll::Ready(Some(Err(XmlReadRecordError::new(
                                index,
                                XmlReadRecordErrorKind::UnexpectedStartTag(
                                    str_from_utf8(local_name, index)?.to_owned(),
                                ),
                            ))));
                        }
                    } else {
                        let current_depth_past_selector = *depth - selector_parts.len();

                        if current_depth_past_selector == 1 {
                            let name = str_from_utf8(local_name, index)?;

                            if let Some(field_index) = fields.get_index_of(name) {
                                *current_field_index = Some(field_index);
                            } else {
                                return Poll::Ready(Some(Err(XmlReadRecordError::new(
                                    index,
                                    XmlReadRecordErrorKind::UnknownField(name.to_owned()),
                                ))));
                            }
                        } else {
                            *current_data += "<";
                            *current_data += str_from_utf8(&start, index)?;
                            *current_data += ">";
                        }
                    }
                }
                End(end) => {
                    *depth -= 1;

                    if *depth > selector_parts.len() {
                        *current_data += "</";
                        *current_data += str_from_utf8(&end, index)?;
                        *current_data += ">";
                    } else if *depth == selector_parts.len() {
                        if let Some(field_name) = current_field_index
                            .take()
                            .and_then(|field_index| fields.get_index(field_index))
                        {
                            current_field_indices.insert(field_name.clone(), current_data.len());

                            *current_field_start = current_data.len();
                        } else {
                            unreachable!(
                                "ill-formed document (unexpected end tag) should be rejected by quick-xml"
                            );
                        }
                    } else if *depth == selector_parts.len() - 1 {
                        *record_number = NonZero::new(record_number.map_or(1, |r| r.get() + 1));

                        let record_fields = unsafe {
                            StringMap::new(
                                std::mem::take(current_data),
                                std::mem::take(current_field_indices),
                            )
                        };

                        let record = DataSourceRecord::new(
                            record_fields,
                            DataSourceRecordIndex {
                                record_number: record_number.expect("always non-zero"),
                                line_start: *current_line_start,
                                line_end: *line_number + 1,
                            },
                        );

                        self.current_record_state = CurrentRecordState::new(fields.len());

                        return Poll::Ready(Some(Ok(record)));
                    }
                }
                Text(text) if current_field_index.is_some() => {
                    *current_data += str_from_utf8(&text, index)?;
                }
                Empty(empty) if current_field_index.is_some() => {
                    *current_data += "<";
                    *current_data += str_from_utf8(&empty, index)?;
                    *current_data += "/>";
                }
                GeneralRef(general_ref) if current_field_index.is_some() => {
                    *current_data += &general_ref
                        .decode()
                        .map_err(|err| XmlReadRecordError::new(index, err))?;
                }
                CData(cdata) if current_field_index.is_some() => {
                    *current_data += str_from_utf8(&cdata, index)?;
                }
                Eof => {
                    return Poll::Ready(None);
                }
                _ => {}
            }
        }
    }
}
