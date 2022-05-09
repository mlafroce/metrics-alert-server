use crate::connection_handler::ConnectionHandler;
use crate::load_balancer::LoadBalancer;
use crate::metric::metric_writer::MetricWriterPool;
use log::info;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::channel;
use std::sync::Arc;
use crate::metric::query_handler::QueryHandlerPool;

mod connection_handler;
mod load_balancer;
mod metric;

const METRIC_WRITER_POOL_SIZE: usize = 4;

fn main() {
    let term_flag = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&term_flag)).unwrap();
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&term_flag)).unwrap();

    let (connection_sender, connection_receiver) = channel();

    let port = "12345";

    env_logger::init();
    info!("Listening on :{}", port);
    let mut acceptor = LoadBalancer::new("0.0.0.0:12345", connection_sender).unwrap();
    acceptor.start(term_flag).unwrap();

    let mut metric_senders = Vec::with_capacity(METRIC_WRITER_POOL_SIZE);
    let mut metric_receivers = Vec::with_capacity(METRIC_WRITER_POOL_SIZE);
    for _ in 0..METRIC_WRITER_POOL_SIZE {
        let (sender, receiver) = channel();
        metric_senders.push(sender);
        metric_receivers.push(receiver);
    }

    let mut query_senders = Vec::with_capacity(METRIC_WRITER_POOL_SIZE);
    let mut query_receivers = Vec::with_capacity(METRIC_WRITER_POOL_SIZE);
    for _ in 0..METRIC_WRITER_POOL_SIZE {
        let (sender, receiver) = channel();
        query_senders.push(sender);
        query_receivers.push(receiver);
    }

    let metric_writer_pool = MetricWriterPool::new(metric_receivers);
    let query_writer_pool = QueryHandlerPool::new(query_receivers);
    ConnectionHandler::run(connection_receiver, metric_senders, query_senders);

    acceptor.stop();
}
