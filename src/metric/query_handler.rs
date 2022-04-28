use std::io;
use std::sync::mpsc::Receiver;
use log::warn;
use threadpool::ThreadPool;
use crate::metric::QueryParams;

const TEMP_FILE_LIFETIME: i64 = 5;

pub struct QueryHandlerPool {
    pool: ThreadPool,
}

impl QueryHandlerPool {
    pub fn new(receivers: Vec<Receiver<QueryParams>>) -> Self {
        let pool = ThreadPool::new(receivers.len());
        for (id, receiver) in receivers.into_iter().enumerate() {
            pool.execute(move || {
                let mut handler = QueryHandler::new(id);
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
    id: usize,
}

impl QueryHandler {
    pub fn new(id: usize) -> Self {
        Self { id }
    }

    pub fn run(&mut self, receiver: Receiver<QueryParams>) -> io::Result<()> {
        loop {
            match receiver.recv() {
                Ok(metric) => {
                    self.handle_metric(metric)?;
                }
                Err(e) => {warn!("Error!");}
            }
        }
    }

    fn handle_metric(&mut self, metric: QueryParams) -> io::Result<()> {
        Ok(())
    }
}