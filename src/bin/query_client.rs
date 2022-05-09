use rand::Rng;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::time::Duration;
use tp1::metric::{MetricAction, QueryParams};

fn main() {
    let metric_path = "queries.txt";
    let host_addr = "0.0.0.0:12345";
    run_queries(metric_path, host_addr).unwrap();
}

fn run_queries(metric_path: &str, host_addr: &str) -> io::Result<()> {
    let file = File::open(metric_path)?;
    let reader = BufReader::new(file);
    let queries = reader
        .lines()
        .flatten()
        .flat_map(|line| serde_json::from_str::<QueryParams>(&line))
        .map(|params| { MetricAction::Query(params) });

    let mut rng = rand::thread_rng();

    for query in queries {
        let mut connection = TcpStream::connect(host_addr)?;
        std::thread::sleep(Duration::from_millis(rng.gen_range(0..100)));
        query.write_to(&mut connection)?;
    }
    Ok(())
}
