use log::warn;
use std::io;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct LoadBalancer {
    listener: Option<TcpListener>,
    connection_sender: Option<Sender<TcpStream>>,
    handle: Option<JoinHandle<()>>,
}

impl LoadBalancer {
    pub fn new<T: ToSocketAddrs>(
        addr: T,
        connection_sender: Sender<TcpStream>,
    ) -> io::Result<Self> {
        let listener = Some(TcpListener::bind(addr)?);
        let connection_sender = Some(connection_sender);
        let handle = None;
        Ok(Self {
            listener,
            connection_sender,
            handle,
        })
    }

    pub fn start(&mut self, term_flag: Arc<AtomicBool>) -> io::Result<()> {
        if let Some(listener) = self.listener.take() {
            if let Some(connection_sender) = self.connection_sender.take() {
                let handle = std::thread::spawn(move || {
                    LoadBalancer::run(listener, connection_sender, term_flag);
                });
                self.handle = Some(handle);
            }
        }
        Ok(())
    }

    pub fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.join();
        }
    }

    fn run(
        listener: TcpListener,
        connection_sender: Sender<TcpStream>,
        term_flag: Arc<AtomicBool>,
    ) -> io::Result<()> {
        listener.set_nonblocking(true);
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    // TODO: Handle
                    connection_sender.send(stream).ok();
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_secs(1));
                }
                Err(e) => panic!("encountered IO error: {}", e),
            }
            if term_flag.load(Ordering::Relaxed) {
                warn!("Server going down!");
                break;
            }
        }
        Ok(())
    }
}
