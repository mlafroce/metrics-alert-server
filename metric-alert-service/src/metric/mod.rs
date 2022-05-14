use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use std::io;
use std::io::{Read, Write};
use crossbeam_channel::Sender;
use crate::metric::query::QueryParams;

pub mod metric_writer;
pub mod query_handler;
pub mod query;

/// A query is a tuple with the query parameters and a sender (an address) to write the result
pub type Query = (QueryParams, Sender<Vec<f32>>);
pub type DateRange = (DateTime<Utc>, DateTime<Utc>);

#[derive(Deserialize, Serialize)]
pub enum MetricAction {
    Insert(Metric),
    Query(QueryParams),
}

impl MetricAction {
    pub fn from_stream<R: Read>(mut stream: R) -> io::Result<Self> {
        let mut action_code = [0u8];
        stream.read_exact(&mut action_code)?;
        match action_code[0] {
            b'I' => Ok(MetricAction::Insert(Metric::from_stream(&mut stream)?)),
            b'Q' => Ok(MetricAction::Query(QueryParams::from_stream(&mut stream)?)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid code")),
        }
    }

    pub fn write_to<W: Write>(&self, stream: &mut W) -> io::Result<()> {
        match self {
            MetricAction::Insert(metric) => {
                stream.write_all(&[b'I'])?;
                metric.write_to(stream)?;
            }
            MetricAction::Query(query) => {
                stream.write_all(&[b'Q'])?;
                query.write_to(stream)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Metric {
    pub metric_id: String,
    value: f32,
    timestamp: Option<chrono::DateTime<Utc>>
}

impl Metric {
    pub fn from_stream<R: Read>(stream: &mut R) -> io::Result<Self> {
        let mut size_buf = [0; 4];
        stream.read_exact(&mut size_buf)?;
        let size = u32::from_be_bytes(size_buf);
        let mut metric_id_buf = vec![0u8; size as usize];
        stream.read_exact(metric_id_buf.as_mut_slice())?;
        let metric_id = String::from_utf8(metric_id_buf).unwrap();
        stream.read_exact(&mut size_buf)?;
        let value = f32::from_be_bytes(size_buf);
        let mut timestamp_buf = [0; 8];
        stream.read_exact(&mut timestamp_buf)?;
        let timestamp_i64 = i64::from_be_bytes(timestamp_buf);
        let naive = NaiveDateTime::from_timestamp(timestamp_i64, 0);
        let timestamp = Some(DateTime::from_utc(naive, Utc));
        Ok(Self { metric_id, value, timestamp })
    }

    pub fn write_to<W: Write>(&self, stream: &mut W) -> io::Result<()> {
        stream.write_all(&(self.metric_id.len() as u32).to_be_bytes())?;
        stream.write_all(self.metric_id.as_bytes())?;
        stream.write_all(&self.value.to_be_bytes())?;
        if let Some(timestamp) = self.timestamp {
            stream.write_all(&timestamp.timestamp().to_be_bytes())?;
        } else {
            stream.write_all(&0_i64.to_be_bytes())?;
        }
        Ok(())
    }
}

pub struct MetricIterator<R: Read> {
    source: R,
}

impl<R: Read> MetricIterator<R> {
    pub fn new(source: R) -> Self {
        Self { source }
    }
}

impl<R: Read> Iterator for MetricIterator<R> {
    type Item = Metric;

    fn next(&mut self) -> Option<Self::Item> {
        Metric::from_stream(&mut self.source).ok()
    }
}
