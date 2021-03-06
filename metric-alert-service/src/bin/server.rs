use envconfig::Envconfig;
use log::info;
use std::io;
use std::sync::atomic::AtomicBool;
use crossbeam_channel::unbounded as channel;
use std::sync::Arc;
use chrono::Duration;
use tp1::alarm::AlarmManager;
use tp1::connection_handler::ConnectionHandler;
use tp1::load_balancer::LoadBalancer;
use tp1::metric::metric_writer::MetricWriterPool;
use tp1::metric::query_handler::QueryHandlerPool;

const METRIC_WRITER_POOL_SIZE: usize = 4;
const ALARM_FREQUENCY_SECS: usize = 60;

#[derive(Envconfig)]
struct Config {
    /// logger level: valid values: "DEBUG", "INFO", "WARN", "ERROR"
    #[envconfig(from = "LOGGING_LEVEL", default = "INFO")]
    logging_level: String,
    /// server port
    #[envconfig(from = "SERVER_PORT", default = "12345")]
    server_port: String,
    /// Folder with stored metrics
    #[envconfig(from = "METRICS_ROOT", default = "metrics")]
    metrics_root: String,
    /// Folder with alarm configurations
    #[envconfig(from = "ALARM_FILE", default = "alarms.json")]
    alarm_file: String,
}

fn main() {
    let env_config = Config::init_from_env().unwrap();
    println!("Setting logger level: {}", env_config.logging_level);
    std::env::set_var("RUST_LOG", env_config.logging_level.clone());
    env_logger::init();
    let writer_path = format!("{}/writer", env_config.metrics_root);
    info!("Creating metric folder if it doesn't exists... {}", writer_path);
    std::fs::create_dir_all(writer_path).unwrap();
    run_server(env_config).unwrap();
}

fn run_server(config: Config) -> io::Result<()> {
    let term_flag = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&term_flag)).unwrap();
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&term_flag)).unwrap();

    let (connection_sender, connection_receiver) = channel();

    info!("Listening on :{}", config.server_port);
    let socket_address = format!("0.0.0.0:{}", config.server_port);
    let mut acceptor = LoadBalancer::new(socket_address, connection_sender)?;
    acceptor.start(term_flag.clone())?;

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

    let metrics_root = config.metrics_root;

    let mut metric_writer_pool = MetricWriterPool::new(metric_receivers, metrics_root.clone());
    let mut query_handler_pool = QueryHandlerPool::new(query_receivers, metrics_root);

    let mut alarm_manager = AlarmManager::from_file(config.alarm_file, Duration::seconds(ALARM_FREQUENCY_SECS as i64))?;
    alarm_manager.start(query_senders.clone(), term_flag);

    ConnectionHandler::run(connection_receiver, metric_senders, query_senders);

    alarm_manager.stop();
    acceptor.stop();
    metric_writer_pool.stop();
    query_handler_pool.stop();
    Ok(())
}
