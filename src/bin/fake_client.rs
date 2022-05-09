use rand::Rng;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::{Duration, SystemTime};
use chrono::{DateTime, Utc};
use tp1::metric::{Metric, MetricAction, QueryAggregation, QueryParams};

fn main() {
    let metric_path = "actions.txt";
    let host_addr = "0.0.0.0:12345";
    run_fake_client(metric_path, host_addr).unwrap();
}

fn run_fake_client(metric_path: &str, host_addr: &str) -> io::Result<()> {
    let file = File::open(metric_path)?;
    let mut out = File::create("test.json")?;
    let reader = BufReader::new(file);
    let metrics = reader
        .lines()
        .flatten()
        .flat_map(|line| serde_json::from_str::<MetricAction>(&line))
        .map(|action| {
            let s = serde_json::to_string(&action).unwrap();
            out.write_all(s.as_bytes()).ok();
            return action;
        })
        .collect::<Vec<_>>();

    let query = QueryParams {date_range: Some((Utc::now(), Utc::now())), window_secs: 1.0, aggregation: QueryAggregation::Avg, metric_id:"Hola".to_owned()};
    let action = MetricAction::Query(query);
    let s = serde_json::to_string(&action).unwrap();
    out.write_all(s.as_bytes()).ok();

    let mut rng = rand::thread_rng();
    if metrics.is_empty() {
        println!("No metrics loaded, aborting");
        return Ok(());
    }

    println!("Loaded {} fake metrics, looping...", metrics.len());
    loop {
        let mut connection = TcpStream::connect(host_addr)?;
        std::thread::sleep(Duration::from_millis(rng.gen_range(0..100)));
        let metric_idx = rng.gen_range(0..metrics.len());
        metrics[metric_idx].write_to(&mut connection)?;
    }
}
