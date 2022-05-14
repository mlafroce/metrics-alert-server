use crate::metric::Metric;
use chrono::{DateTime, Duration, DurationRound, Utc};
use log::{debug, warn};
use std::fs::File;
use std::io;
use crossbeam_channel::{Receiver, RecvTimeoutError};
use threadpool::ThreadPool;

const TEMP_FILE_LIFETIME: i64 = 5;

pub struct MetricWriterPool {
    pool: ThreadPool,
}

impl MetricWriterPool {
    pub fn new(receivers: Vec<Receiver<Metric>>, metrics_root: String) -> Self {
        let pool = ThreadPool::new(receivers.len());
        for (id, receiver) in receivers.into_iter().enumerate() {
            let root_clone = metrics_root.clone();
            pool.execute(move || {
                let mut handler = MetricWriter::new(id, root_clone).unwrap();
                handler.run(receiver).unwrap();
            });
        }
        Self { pool }
    }

    pub fn stop(&mut self) {
        self.pool.join();
    }
}

struct MetricWriter {
    id: usize,
    metrics_root: String,
    current_time_slice: DateTime<Utc>,
    current_file: File,
}

impl MetricWriter {
    pub fn new(id: usize, metrics_root: String) -> io::Result<Self> {
        let time = chrono::Utc::now();
        let trunc_time = time
            .duration_trunc(Duration::seconds(TEMP_FILE_LIFETIME))
            .unwrap();
        let path = format!("{}/writer/{}.metric.tmp", metrics_root,id);
        let current_file = File::create(path)?;
        Ok(Self {
            id,
            metrics_root,
            current_time_slice: trunc_time,
            current_file,
        })
    }

    pub fn run(&mut self, receiver: crossbeam_channel::Receiver<Metric>) -> io::Result<()> {
        loop {
            self.check_file_swap()?;
            match receiver.recv_timeout(std::time::Duration::from_secs(5)) {
                Ok(metric) => {
                    self.handle_metric(metric)?;
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    warn!("Error while receiving metric! Are we shutting down?");
                    return Ok(());
                }
            }
        }
    }

    fn handle_metric(&mut self, mut metric: Metric) -> io::Result<()> {
        metric.timestamp = Some(chrono::Utc::now());
        debug!("Writing metric {:?}", metric);
        metric.write_to(&mut self.current_file)?;
        Ok(())
    }

    fn check_file_swap(&mut self) -> io::Result<()> {
        let time = chrono::Utc::now();
        let trunc_time = time
            .duration_trunc(Duration::seconds(TEMP_FILE_LIFETIME))
            .unwrap();
        if self.current_time_slice != trunc_time {
            let old_path = format!("{}/writer/{}.metric.tmp", self.metrics_root, self.id);
            let new_path = format!(
                "{}/{}_{:x}.metric.tmp",
                self.metrics_root,
                self.id,
                self.current_time_slice.timestamp()
            );
            std::fs::rename(&old_path, &new_path)?;
            self.current_file = File::create(old_path)?;
            self.current_time_slice = trunc_time;
        }
        Ok(())
    }
}
