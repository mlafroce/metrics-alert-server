use chrono::{DateTime, Duration, MIN_DATETIME, NaiveDateTime, Utc};
use log::debug;
use serde::{Deserialize, Deserializer, Serialize};
use std::io;
use std::io::{BufReader, Read, Write};
use std::ops::Sub;
use crate::metric::{DateRange, Metric};

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

    pub(crate) fn process_metrics(&self, metrics: impl Iterator<Item = Metric>) -> Vec<f32> {
        debug!("Processing metrics...");
        let mut result_vec = vec!();
        let mut last_window_start = MIN_DATETIME;
        if self.window_secs == 0.0 {
            let values = metrics.map(|metric| metric.value);
            let result = match self.aggregation {
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
            };
            if let Some(value) = result {
                result_vec.push(value);
            }
        } else {
            debug!("Processing with window secs: {}", self.window_secs);
            let mut count = 0;
            let mut sum = 0.0;
            let mut min = 0.0;
            let mut max = 0.0;
            for metric in metrics {
                debug!("Metric: {:?}", metric);
                let timestamp = metric.timestamp.unwrap();
                if timestamp.sub(Duration::seconds(self.window_secs as i64)) > last_window_start {
                    if last_window_start != MIN_DATETIME {
                        match self.aggregation {
                            QueryAggregation::Avg => {
                                if count > 0 {
                                    result_vec.push(sum / count as f32);
                                }
                            }
                            // TODO: handle NaN values
                            QueryAggregation::Max => result_vec.push(max),
                            QueryAggregation::Min => result_vec.push(min),
                            QueryAggregation::Count => result_vec.push(count as f32),
                        }
                    };
                    max = f32::MIN;
                    min = f32::MAX;
                    sum = 0.0;
                    count = 0;
                    last_window_start = timestamp;
                }
                sum += metric.value;
                count += 1;
                max = f32::max(max, metric.value);
                min = f32::min(min, metric.value);
            }
            debug!("Finished metrics...");
            if last_window_start != MIN_DATETIME {
                match self.aggregation {
                    QueryAggregation::Avg => {
                        if count > 0 {
                            result_vec.push(sum / count as f32);
                        }
                    }
                    // TODO: handle NaN values
                    QueryAggregation::Max => result_vec.push(max),
                    QueryAggregation::Min => result_vec.push(min),
                    QueryAggregation::Count => result_vec.push(count as f32),
                }
            }
        };
        result_vec
    }
}

fn date_deserializer<'de, D>(deserializer: D) -> Result<Option<DateRange>, D::Error>
    where
        D: Deserializer<'de>,
{
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
