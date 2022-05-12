use crate::metric::Metric;
use chrono::{DateTime, Duration, DurationRound, Utc};
use log::{debug, warn};
use std::fs::File;
use std::io;
use std::sync::mpsc::Receiver;
use threadpool::ThreadPool;

const TEMP_FILE_LIFETIME: i64 = 5;

pub struct MetricWriterPool {
    pool: ThreadPool,
}

impl MetricWriterPool {
    pub fn new(receivers: Vec<Receiver<Metric>>) -> Self {
        let pool = ThreadPool::new(receivers.len());
        for (id, receiver) in receivers.into_iter().enumerate() {
            pool.execute(move || {
                let mut handler = MetricWriter::new(id).unwrap();
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
    current_time_slice: DateTime<Utc>,
    current_file: File,
}

impl MetricWriter {
    pub fn new(id: usize) -> io::Result<Self> {
        let time = chrono::Utc::now();
        let trunc_time = time
            .duration_trunc(Duration::seconds(TEMP_FILE_LIFETIME))
            .unwrap();
        let path = format!("metrics/writer/{}.metric.tmp", id);
        let current_file = File::create(path)?;
        Ok(Self {
            id,
            current_time_slice: trunc_time,
            current_file,
        })
    }

    pub fn run(&mut self, receiver: Receiver<Metric>) -> io::Result<()> {
        loop {
            self.check_file_swap()?;
            match receiver.recv() {
                Ok(metric) => {
                    self.handle_metric(metric)?;
                }
                Err(_) => {
                    warn!("Error while receiving metric! Are we shutting down?");
                    return Ok(());
                }
            }
        }
    }

    fn handle_metric(&mut self, metric: Metric) -> io::Result<()> {
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
            let old_path = format!("metrics/writer/{}.metric.tmp", self.id);
            let new_path = format!(
                "metrics/{}_{:x}.metric.tmp",
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
