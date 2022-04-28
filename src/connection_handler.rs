use crate::metric::{Metric, MetricAction};
use log::{debug, info, warn};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, Instant};
use threadpool::ThreadPool;
use tp1::metric::QueryParams;

const CONNECTION_MAX_WAIT: Duration = Duration::from_millis(200);
const NUM_THREADS: usize = 4;

pub struct ConnectionHandler {
    connection: TcpStream,
    metric_senders: Vec<Sender<Metric>>,
    query_senders: Vec<Sender<QueryParams>>,
}

impl ConnectionHandler {
    pub fn run(connection_receiver: Receiver<TcpStream>, metric_senders: Vec<Sender<Metric>>, query_senders: Vec<Sender<QueryParams>>) {
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
                    handler.handle_connection();
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
        let mut write_con = self.connection.try_clone()?;
        let reader_con = self.connection.try_clone()?;
        let action = MetricAction::from_stream(reader_con)?;
        self.send_action(action, write_con);
        Ok(())
    }

    pub fn send_action(&self, action: MetricAction, mut write_con: TcpStream) {
        let mut hasher = DefaultHasher::new();
        metric.metric_id.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        let idx = hash % self.metric_senders.len();
        match action {
            MetricAction::Insert(metric) => {
                debug!("Inserting {:?} into pipe {}", metric, idx);
                self.metric_senders[idx].send(metric);
                write_con.write_all("OK".as_bytes())?;
            },
            MetricAction::Query(query_params) => {
                self.query_senders[idx].send(query_params);
            }
        }
    }
}
