use crate::metric::{Metric, MetricAction, Query, QueryParams};
use log::{debug, error, info, warn};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::{Duration, Instant};
use threadpool::ThreadPool;

const CONNECTION_MAX_WAIT: Duration = Duration::from_millis(200);
const NUM_THREADS: usize = 4;

pub struct ConnectionHandler {
    connection: TcpStream,
    metric_senders: Vec<Sender<Metric>>,
    query_senders: Vec<Sender<Query>>,
}

impl ConnectionHandler {
    pub fn run(connection_receiver: Receiver<TcpStream>, metric_senders: Vec<Sender<Metric>>, query_senders: Vec<Sender<Query>>) {
        info!("Starting pool with {:?} workers", NUM_THREADS);
        let pool = ThreadPool::new(NUM_THREADS);
        for connection in connection_receiver {
            let metric_senders_clone = metric_senders.clone();
            let query_senders_clone = query_senders.clone();
            let connection_ts = Instant::now();
            let job = move || {
                if connection_ts.elapsed() < CONNECTION_MAX_WAIT {
                    debug!("Handling connection");
                    let handler = ConnectionHandler {
                        connection,
                        metric_senders: metric_senders_clone,
                        query_senders: query_senders_clone,
                    };
                    if let Err(e) = handler.handle_connection() {
                        error!("Failed to handle connection: {:?}", e);
                    }
                } else {
                    warn!("Connection dropped");
                }
            };
            pool.execute(job);
        }
    }
}

impl ConnectionHandler {
    fn handle_connection(&self) -> io::Result<()> {
        let write_con = self.connection.try_clone()?;
        let reader_con = self.connection.try_clone()?;
        let action = MetricAction::from_stream(reader_con)?;
        self.send_action(action, write_con)?;
        Ok(())
    }

    pub fn send_action(&self, action: MetricAction, mut write_con: TcpStream) -> io::Result<()> {
        let mut hasher = DefaultHasher::new();
        match action {
            MetricAction::Insert(metric) => {
                metric.metric_id.hash(&mut hasher);
                let hash = hasher.finish() as usize;
                let idx = hash % self.metric_senders.len();
                debug!("Inserting {:?} into pipe {}", metric, idx);
                self.metric_senders[idx].send(metric).ok();
                write_con.write_all("OK".as_bytes())?;
            },
            MetricAction::Query(query_params) => {
                query_params.metric_id.hash(&mut hasher);
                let hash = hasher.finish() as usize;
                let idx = hash % self.metric_senders.len();
                debug!("Querying {:?} in pipe {}", query_params, idx);
                let (result_sender, result_recv) = channel();
                let query = (query_params, result_sender);
                self.query_senders[idx].send(query).ok();
                if let Some(result) = result_recv.recv().unwrap() {
                    write_con.write_all(format!("{{ result: 'ok', value: {} }}\n", result).as_bytes())?;
                } else {
                    write_con.write_all(format!("{{ result: 'not_found' }}\n").as_bytes())?;
                }
            }
        }
        Ok(())
    }
}
