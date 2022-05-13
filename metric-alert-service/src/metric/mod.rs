use chrono::{DateTime, NaiveDateTime, Utc};
use log::debug;
use serde::{Deserialize, Deserializer, Serialize};
use std::io;
use std::io::{BufReader, Read, Write};
use std::sync::mpsc::Sender;

pub mod metric_writer;
pub mod query_handler;

/// A query is a tuple with the query parameters and a sender (an address) to write the result
pub type Query = (QueryParams, Sender<Option<f32>>);
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
        Ok(Self { metric_id, value })
    }

    pub fn write_to<W: Write>(&self, stream: &mut W) -> io::Result<()> {
        stream.write_all(&(self.metric_id.len() as u32).to_be_bytes())?;
        stream.write_all(self.metric_id.as_bytes())?;
        stream.write_all(&self.value.to_be_bytes())?;
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

#[derive(Debug, Deserialize, Serialize)]
pub struct QueryParams {
    pub metric_id: String,
    #[serde(default)]
    #[serde(deserialize_with = "date_deserializer")]
    pub date_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    pub aggregation: QueryAggregation,
    pub window_secs: f32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum QueryAggregation {
    Avg,
    Min,
    Max,
    Count,
}

impl QueryParams {
    pub fn from_stream<R: Read>(stream: R) -> io::Result<Self> {
        let mut reader = BufReader::new(stream);
        let mut size_buf = [0; 4];
        reader.read_exact(&mut size_buf)?;
        let size = u32::from_be_bytes(size_buf);
        let mut metric_id_buf = vec![0u8; size as usize];
        reader.read_exact(metric_id_buf.as_mut_slice())?;
        let metric_id = String::from_utf8(metric_id_buf).unwrap();
        let mut has_time_range = [0];
        reader.read_exact(&mut has_time_range)?;
        let mut date_range = None;
        if has_time_range[0] == b'Y' {
            let mut timestamp_buf = [0; 8];
            reader.read_exact(timestamp_buf.as_mut_slice())?;
            let from_ts = i64::from_be_bytes(timestamp_buf);
            reader.read_exact(timestamp_buf.as_mut_slice())?;
            let to_ts = i64::from_be_bytes(timestamp_buf);
            let from = DateTime::from_utc(NaiveDateTime::from_timestamp(from_ts, 0), Utc);
            let to = DateTime::from_utc(NaiveDateTime::from_timestamp(to_ts, 0), Utc);
            date_range = Some((from, to));
        } else if has_time_range[0] != b'N' {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid code"));
        }

        let mut aggregation_buf = [0];
        reader.read_exact(&mut aggregation_buf)?;
        let aggregation = match aggregation_buf[0] {
            b'a' => QueryAggregation::Avg,
            b'm' => QueryAggregation::Min,
            b'M' => QueryAggregation::Max,
            b'c' => QueryAggregation::Count,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid aggregation",
                ))
            }
        };

        reader.read_exact(&mut size_buf)?;
        let window_secs = f32::from_be_bytes(size_buf);
        Ok(Self {
            metric_id,
            date_range,
            aggregation,
            window_secs,
        })
    }

    pub fn write_to<W: Write>(&self, stream: &mut W) -> io::Result<()> {
        stream.write_all(&(self.metric_id.len() as u32).to_be_bytes())?;
        stream.write_all(self.metric_id.as_bytes())?;
        if let Some((from, to)) = self.date_range {
            stream.write_all(&[b'Y'])?;
            stream.write_all(&from.timestamp().to_be_bytes())?;
            stream.write_all(&to.timestamp().to_be_bytes())?;
        } else {
            stream.write_all(&[b'N'])?;
        }
        let aggregation_code = match self.aggregation {
            QueryAggregation::Avg => b'a',
            QueryAggregation::Min => b'm',
            QueryAggregation::Max => b'M',
            QueryAggregation::Count => b'c',
        };
        stream.write_all(&[aggregation_code])?;
        stream.write_all(&self.window_secs.to_be_bytes())?;
        Ok(())
    }

    fn process_metrics(&self, metrics: impl Iterator<Item = Metric>) -> Option<f32> {
        debug!("Processing metrics...");
        let values = metrics.map(|metric| metric.value);
        match self.aggregation {
            QueryAggregation::Avg => {
                let mut count = 0;
                let mut sum = 0.0;
                values.for_each(|v| {
                    sum += v;
                    count += 1;
                });
                if count > 0 {
                    Some(sum / count as f32)
                } else {
                    None
                }
            }
            // TODO: handle NaN values
            QueryAggregation::Max => values.max_by(|l, r| l.partial_cmp(r).unwrap()),
            QueryAggregation::Min => values.min_by(|l, r| l.partial_cmp(r).unwrap()),
            QueryAggregation::Count => Some(values.count() as f32),
        }
    }
}

fn date_deserializer<'de, D>(deserializer: D) -> Result<Option<DateRange>, D::Error>
where
    D: Deserializer<'de>,
{
    println!("Deserializing!");
    if let Ok((from_str, to_str)) = Deserialize::deserialize(deserializer) {
        let from_naive = NaiveDateTime::parse_from_str(from_str, "%Y-%m-%d %H:%M:%S").unwrap();
        let to_naive = NaiveDateTime::parse_from_str(to_str, "%Y-%m-%d %H:%M:%S").unwrap();
        let range = (
            DateTime::from_utc(from_naive, Utc),
            DateTime::from_utc(to_naive, Utc),
        );
        Ok(Some(range))
    } else {
        Ok(None)
    }
}
