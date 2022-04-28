use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::io;
use std::io::{BufReader, Read, Write};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::de::value::StrDeserializer;

pub mod metric_writer;
pub mod query_handler;

pub enum MetricAction {
    Insert(Metric),
    Query(QueryParams)
}

impl MetricAction {
    pub fn from_stream<R: Read>(mut stream: R) -> io::Result<Self> {
        let mut action_code = [0u8];
        stream.read_exact(&mut action_code)?;
        match action_code[0] as char {
            'I' => Ok(MetricAction::Insert(Metric::from_stream(stream)?)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid code"))
        }
    }

    pub fn write_to<W: Write>(&self, stream: &mut W) -> io::Result<()> {
        match self {
            MetricAction::Insert(metric) => {
                stream.write_all(&['I' as u8])?;
                metric.write_to(stream)?;
            }
            MetricAction::Query(query) => {
                stream.write_all(&['Q' as u8])?;
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
    pub fn from_stream<R: Read>(stream: R) -> io::Result<Self> {
        let mut reader = BufReader::new(stream);
        let mut size_buf = [0; 4];
        reader.read_exact(&mut size_buf)?;
        let size = u32::from_be_bytes(size_buf);
        let mut metric_id_buf = vec![0u8; size as usize];
        reader.read_exact(metric_id_buf.as_mut_slice())?;
        let metric_id = String::from_utf8(metric_id_buf).unwrap();
        reader.read_exact(&mut size_buf)?;
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

#[derive(Debug)]
pub struct QueryParams {
    pub metric_id: String,
    pub date_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    pub aggregation: QueryAggregation,
    pub window_secs: f32
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QueryParamsSerializable {
    pub metric_id: String,
    pub date_range: Option<(String, String)>,
    pub aggregation: QueryAggregation,
    pub window_secs: f32
}

impl QueryParamsSerializable {
    pub fn to_query_params(&self) -> QueryParams {
        let date_range;
        if let Some((from_str, to_str)) = &self.date_range {
            let from_naive = NaiveDateTime::parse_from_str(from_str, "%Y-%m-%d %H:%M:%S").unwrap();
            let to_naive = NaiveDateTime::parse_from_str(to_str, "%Y-%m-%d %H:%M:%S").unwrap();
            date_range = Some((DateTime::from_utc(from_naive, Utc), DateTime::from_utc(to_naive, Utc)));
        } else {
            date_range = None;
        }
        QueryParams {
            metric_id: self.metric_id.clone(),
            date_range,
            aggregation: self.aggregation.clone(),
            window_secs: self.window_secs
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum QueryAggregation {
    Avg, Min, Max, Count
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
        if has_time_range[0] == 'Y' as u8 {
            let mut timestamp_buf = [0; 8];
            reader.read_exact(timestamp_buf.as_mut_slice())?;
            let from_ts = i64::from_be_bytes(timestamp_buf);
            reader.read_exact(timestamp_buf.as_mut_slice())?;
            let to_ts = i64::from_be_bytes(timestamp_buf);
            let from = DateTime::from_utc(NaiveDateTime::from_timestamp(from_ts, 0), Utc);
            let to = DateTime::from_utc(NaiveDateTime::from_timestamp(to_ts, 0), Utc);
            date_range = Some((from, to));
        } else if has_time_range[0] != 'N' as u8 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid code"));
        }

        let mut aggregation_buf = [0];
        reader.read_exact(&mut aggregation_buf)?;
        let aggregation = match aggregation_buf[0] as char {
            'a' => QueryAggregation::Avg,
            'm' => QueryAggregation::Min,
            'M' => QueryAggregation::Max,
            'c' => QueryAggregation::Count,
            _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid aggregation"))
        };

        reader.read_exact(&mut size_buf)?;
        let window_secs = f32::from_be_bytes(size_buf);
        Ok(Self { metric_id, date_range, aggregation, window_secs })
    }

    pub fn write_to<W: Write>(&self, stream: &mut W) -> io::Result<()> {
        stream.write_all(&(self.metric_id.len() as u32).to_be_bytes())?;
        if let Some((from, to)) = self.date_range {
            stream.write_all(&['Y' as u8])?;
            stream.write_all(&from.timestamp().to_be_bytes())?;
            stream.write_all(&to.timestamp().to_be_bytes())?;
        } else {
            stream.write_all(&['N' as u8])?;
        }
        let aggregation_code = match self.aggregation {
            QueryAggregation::Avg => 'a',
            QueryAggregation::Min => 'm',
            QueryAggregation::Max => 'M',
            QueryAggregation::Count => 'c',
        };
        stream.write_all(&[aggregation_code as u8])?;
        stream.write_all(&self.window_secs.to_be_bytes())?;
        Ok(())
    }
}
