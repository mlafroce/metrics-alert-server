use envconfig::Envconfig;
use log::{info, warn};
use rand::Rng;
use std::fs::File;
use std::io;
use std::io::{stdin, BufRead, BufReader};
use std::net::TcpStream;
use std::time::Duration;
use tp1::metric::MetricAction;

#[derive(Envconfig)]
struct Config {
    /// logger level: valid values: "DEBUG", "INFO", "WARN", "ERROR"
    #[envconfig(from = "LOGGING_LEVEL", default = "INFO")]
    logging_level: String,
    /// server host
    #[envconfig(from = "SERVER_HOST", default = "localhost")]
    server_host: String,
    /// server port
    #[envconfig(from = "SERVER_PORT", default = "12345")]
    server_port: String,
    /// interactive mode enables user input
    #[envconfig(from = "INTERACTIVE", default = "false")]
    interactive: bool,
    /// on automated mode, read possible actions from ACTION_FILE
    #[envconfig(from = "ACTION_FILE", default = "actions.txt")]
    action_file: String,
    /// repeat automated actions
    #[envconfig(from = "REPEAT", default = "true")]
    repeat: bool,
    /// repeat automated actions after N millis
    #[envconfig(from = "REPEAT_TIME", default = "100")]
    repeat_time: u32,
}

fn main() {
    let env_config = Config::init_from_env().unwrap();
    println!("Setting logger level: {}", env_config.logging_level);
    std::env::set_var("RUST_LOG", env_config.logging_level.clone());
    env_logger::init();
    run_client(env_config).unwrap();
}

fn run_client(config: Config) -> io::Result<()> {
    let host_addr = format!("{}:{}", config.server_host, config.server_port);
    if config.interactive {
        for line in stdin().lock().lines().flatten() {
            if let Ok(action) = serde_json::from_str::<MetricAction>(line.as_str()) {
                if let Err(e) = do_request(&host_addr, &action) {
                    println!("Couldn't execute action: {}", e);
                    break;
                }
            }
        }
    } else {
        let file = File::open(config.action_file)?;
        let reader = BufReader::new(file);
        let actions = reader
            .lines()
            .flatten()
            .flat_map(|line| serde_json::from_str::<MetricAction>(&line))
            .collect::<Vec<_>>();
        if actions.is_empty() {
            warn!("No metrics loaded, aborting");
            return Ok(());
        }

        info!("Loaded {} fake metrics, looping...", actions.len());
        let mut rng = rand::thread_rng();
        if config.repeat {
            loop {
                let metric_idx = rng.gen_range(0..actions.len());
                let action = &actions[metric_idx];
                do_request(&host_addr, action)?;
                if config.repeat_time > 0 {
                    std::thread::sleep(Duration::from_millis(
                        rng.gen_range(0..config.repeat_time as u64),
                    ));
                }
            }
        } else {
            let metric_idx = rng.gen_range(0..actions.len());
            let action = &actions[metric_idx];
            if let Err(e) = do_request(&host_addr, action) {
                println!("Couldn't execute action: {}", e);
            }
        }
    }
    Ok(())
}

fn do_request(host_addr: &String, action: &MetricAction) -> io::Result<()> {
    info!("Connecting to {}", host_addr);
    let mut connection = TcpStream::connect(host_addr)?;
    action.write_to(&mut connection)?;
    let reader = BufReader::new(connection);
    if let Some(response) = reader.lines().flatten().next() {
        println!("Server response: {}", response);
    } else {
        println!("Server didn't answer");
    }
    Ok(())
}
