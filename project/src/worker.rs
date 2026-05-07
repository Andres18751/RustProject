use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use crate::task::{Task, TaskKind};

pub fn worker_thread(id: usize,task_receiver: std::sync::mpsc::Receiver<Task>,completion_sender: std::sync::mpsc::Sender<CompletedTask>,global_cpu: Arc<Mutex<f64>>,worker_available: Arc<Mutex<usize>>,){
    loop {
        match task_receiver.recv() {
            Ok(task) => { 
                {
                    let mut available = worker_available.lock().unwrap();
                    *available -= 1;
                }
                let start_time = std::time::Instant::now();
                

                match task.kind {
                    TaskKind::IO => {
                        
                        thread::sleep(task.duration);
                    }
                    TaskKind::CPU => {
                       
                        let end = start_time + task.duration;
                        while std::time::Instant::now() < end {
                           
                            std::hint::spin_loop();
                        }
                    }
                }
                
                {
                    let mut cpu = global_cpu.lock().unwrap();
                    *cpu -= task.cpu_usage;
                }
                
                {
                    let mut available = worker_available.lock().unwrap();
                    *available += 1;
                }
                let wait_time = start_time
                    .checked_duration_since(task.arrival_time)
                    .unwrap_or_default();
                let completed = CompletedTask {
                    task_id: task.id,
                    worker_id: id,
                    kind: task.kind,
                    start_time,
                    completion_time: start_time.elapsed(),
                    wait_time,
                };
                if completion_sender.send(completed).is_err() {
                    break;
                }
            }
            Err(_) => {
                break;
            }
        }
    }
}

#[derive(Debug)]
pub struct CompletedTask {
    pub task_id: usize,
    pub worker_id: usize,
    pub kind: TaskKind,  
    pub start_time: std::time::Instant,
    pub completion_time: Duration,
    pub wait_time: Duration,
}