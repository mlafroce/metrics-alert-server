use std::collections::hash_map::DefaultHasher;
use crate::metric::query::{QueryAggregation, QueryParams};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::{BufRead, BufReader};
use std::ops::Sub;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use crossbeam_channel::{unbounded as channel, Sender};
use chrono::Duration;
use log::debug;
use threadpool::ThreadPool;
use crate::metric::Query;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AlarmConfig {
    metric_id: String,
    aggregation: QueryAggregation,
    window_secs: f32,
    limit: f32,
}

pub struct AlarmManager {
    pool: ThreadPool,
    configs: Vec<AlarmConfig>,
    frequency: Duration
}

impl AlarmManager {
    pub fn from_file(alarm_file: String, frequency: Duration) -> io::Result<Self> {
        let pool = ThreadPool::new(1);
        let file = File::open(alarm_file)?;
        let reader = BufReader::new(file);
        let configs = reader
            .lines()
            .flatten()
            .flat_map(|line| serde_json::from_str::<AlarmConfig>(&line))
            .collect::<Vec<_>>();
        debug!("Loaded alarms: {:?}", configs);
        Ok( Self {pool, configs, frequency} )
    }

    pub fn start(&mut self, query_senders: Vec<Sender<Query>>, term_flag: Arc<AtomicBool>) {
        let configs = self.configs.clone();
        let frequency = self.frequency;
        self.pool.execute(move || {
            while !term_flag.load(Ordering::Relaxed) {
                for config in &configs {
                    let time_from = chrono::Utc::now().sub(frequency);
                    let query_params = QueryParams {
                        metric_id: config.metric_id.clone(),
                        date_range: Some((time_from, chrono::Utc::now())),
                        aggregation: config.aggregation.clone(),
                        window_secs: config.window_secs
                    };
                    let mut hasher = DefaultHasher::new();
                    query_params.metric_id.hash(&mut hasher);
                    let hash = hasher.finish() as usize;
                    let idx = hash % query_senders.len();
                    debug!("Querying alarm {:?} in pipe {}", query_params, idx);
                    let (result_sender, result_recv) = channel();
                    let query = (query_params, result_sender);
                    query_senders[idx].send(query).ok();
                    for result in result_recv.recv().unwrap() {
                        debug!("[ALARM] {:?} has {:?}: {} (limit: {}", config.metric_id, config.aggregation, result, config.limit);
                        if result > config.limit {
                            println!("[ALARM] {:?} has {:?} over {}", config.metric_id, config.aggregation, config.limit);
                        }
                    }
                }
                debug!("Sleeping: {:?}", frequency);
                // FIXME: Instead of a big sleep, should sleep shorter intervals for faster graceful quit
                std::thread::sleep(frequency.to_std().unwrap());
            }
        })
    }

    pub fn stop(&self) {
        self.pool.join()
    }
}