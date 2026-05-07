use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MonitorSnapshot {
    pub timestamp: Duration,
    pub cpu_usage: f64,
    pub available_workers: usize,
    pub io_queue_length: usize,
    pub cpu_queue_length: usize,
}

pub fn monitor_thread(global_cpu: Arc<Mutex<f64>>,worker_available: Arc<Mutex<usize>>,io_queue_len: Arc<Mutex<usize>>,cpu_queue_len: Arc<Mutex<usize>>,running: Arc<Mutex<bool>>,) -> Vec<MonitorSnapshot> {
    let mut snapshots = Vec::new();
    let start_time = std::time::Instant::now();
    
    while *running.lock().unwrap() {
        let snapshot = MonitorSnapshot {
            timestamp: start_time.elapsed(),
            cpu_usage: *global_cpu.lock().unwrap(),
            available_workers: *worker_available.lock().unwrap(),
            io_queue_length: *io_queue_len.lock().unwrap(),
            cpu_queue_length: *cpu_queue_len.lock().unwrap(),
        };
        
        snapshots.push(snapshot);
        
        thread::sleep(Duration::from_millis(10));
    }
    snapshots
}