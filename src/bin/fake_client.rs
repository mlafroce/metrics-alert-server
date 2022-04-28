use rand::Rng;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::time::Duration;
use tp1::metric::{Metric, MetricAction};

fn main() {
    let metric_path = "metrics.txt";
    let host_addr = "0.0.0.0:12345";
    run_fake_client(metric_path, host_addr);
}

fn run_fake_client(metric_path: &str, host_addr: &str) -> io::Result<()> {
    let file = File::open(metric_path)?;
    let reader = BufReader::new(file);
    let metrics = reader
        .lines()
        .flatten()
        .flat_map(|line| serde_json::from_str::<Metric>(&line))
        .map(|metric| { MetricAction::Insert(metric) })
        .collect::<Vec<_>>();

    let mut rng = rand::thread_rng();
    if metrics.len() == 0 {
        println!("No metrics loaded, aborting");
        return Ok(());
    }

    println!("Loaded {} fake metrics, looping...", metrics.len());
    loop {
        let mut connection = TcpStream::connect(host_addr)?;
        std::thread::sleep(Duration::from_millis(rng.gen_range(0..100)));
        let metric_idx = rng.gen_range(0..metrics.len());
        metrics[metric_idx].write_to(&mut connection);
    }
}
