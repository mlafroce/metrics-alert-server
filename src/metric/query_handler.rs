use crate::metric::{MetricIterator, Query, QueryParams};
use chrono::{DateTime, Duration, DurationRound, Utc};
use log::{info, warn};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io;
use std::ops::Add;
use std::sync::mpsc::Receiver;
use threadpool::ThreadPool;

const TEMP_FILE_LIFETIME: i64 = 5;

pub struct QueryHandlerPool {
    pool: ThreadPool,
}

impl QueryHandlerPool {
    pub fn new(receivers: Vec<Receiver<Query>>) -> Self {
        let n_receivers = receivers.len();
        let pool = ThreadPool::new(n_receivers);
        for receiver in receivers.into_iter() {
            pool.execute(move || {
                let mut handler = QueryHandler::new(n_receivers);
                handler.run(receiver).unwrap();
            });
        }
        Self { pool }
    }

    pub fn stop(&mut self) {
        self.pool.join();
    }
}

struct QueryHandler {
    hash_modulus: usize,
}

impl QueryHandler {
    pub fn new(hash_modulus: usize) -> Self {
        Self { hash_modulus }
    }

    pub fn run(&mut self, receiver: Receiver<Query>) -> io::Result<()> {
        loop {
            match receiver.recv() {
                Ok((query_params, result_sender)) => {
                    info!("Handling query");
                    let result = self.handle_query(query_params)?;
                    result_sender.send(result).ok();
                }
                Err(_) => {
                    warn!("Error while handling query! Are we shutting down?");
                    return Ok(());
                }
            }
        }
    }

    fn handle_query(&mut self, query: QueryParams) -> io::Result<Option<f32>> {
        let mut hasher = DefaultHasher::new();
        query.metric_id.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        let metric_hash = hash % self.hash_modulus;
        // TODO: Remove duplicated code
        let result = if let Some((date_begin, date_end)) = query.date_range {
            let date_iter = date_range_iterator(date_begin, date_end);
            let metric_iterator = date_iter
                .map(|date| format!("metrics/{}_{:x}.metric.tmp", metric_hash, date.timestamp()))
                .flat_map(File::open)
                .flat_map(MetricIterator::new);
            query.process_metrics(metric_iterator)
        } else {
            let paths = std::fs::read_dir("metrics/").unwrap();
            let metric_iterator = paths
                .flatten()
                .flat_map(|path| path.file_name().into_string())
                .filter(|path| {
                    let metric_hash_str = format!("{}", metric_hash);
                    path.starts_with(&metric_hash_str)
                })
                .map(|path| format!("metrics/{}", path))
                .flat_map(File::open)
                .flat_map(MetricIterator::new);
            query.process_metrics(metric_iterator)
        };
        Ok(result)
    }
}

fn date_range_iterator(
    date_begin: DateTime<Utc>,
    date_end: DateTime<Utc>,
) -> impl Iterator<Item = DateTime<Utc>> {
    let mut current_time_slice = date_begin
        .duration_trunc(Duration::seconds(TEMP_FILE_LIFETIME))
        .unwrap();
    let end = date_end
        .duration_trunc(Duration::seconds(TEMP_FILE_LIFETIME))
        .unwrap();
    std::iter::from_fn(move || {
        if current_time_slice <= end {
            let res = current_time_slice;
            current_time_slice = current_time_slice.add(Duration::seconds(TEMP_FILE_LIFETIME));
            Some(res)
        } else {
            None
        }
    })
}
