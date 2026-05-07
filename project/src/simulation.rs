use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use crate::generator::TaskGenerator;
use crate::manager::{SchedulingPolicy, TaskManager};
use crate::worker::{worker_thread, CompletedTask};
use crate::monitor::{monitor_thread, MonitorSnapshot};
use crate::task::TaskKind;

pub struct Simulation {
    config: SimulationConfig,
}

pub struct SimulationConfig {
    pub num_workers: usize,
    pub io_count: usize,
    pub cpu_count: usize,
    pub scheduling_policy: String,
}

impl Simulation {
    pub fn new(config: SimulationConfig) -> Self {
        Simulation { config }
    }
    
    pub fn run(&self) {
        let io_count = self.config.io_count;
        let cpu_count = self.config.cpu_count;
        
        let global_cpu = Arc::new(Mutex::new(0.0));
        let worker_available = Arc::new(Mutex::new(self.config.num_workers));
        let io_queue_len = Arc::new(Mutex::new(0));
        let cpu_queue_len = Arc::new(Mutex::new(0));
        let running = Arc::new(Mutex::new(true));
        
        let (task_sender, task_receiver) = mpsc::channel();
        let (completion_sender, completion_receiver) = mpsc::channel();
        
        let generator = TaskGenerator::new(io_count, cpu_count);
        let gen_handle = thread::spawn(move || {
            generator.generate_and_send(task_sender);
        });
        
        let policy = match self.config.scheduling_policy.as_str() {
            "FIFO" => SchedulingPolicy::FIFO,
            "Optimized" => SchedulingPolicy::Optimized,
            _ => SchedulingPolicy::FIFO,
        };
        
        let manager_cpu = Arc::clone(&global_cpu);
        let manager_workers = Arc::clone(&worker_available);
        let manager_io_len = Arc::clone(&io_queue_len);
        let manager_cpu_len = Arc::clone(&cpu_queue_len);
        let manager_running = Arc::clone(&running);
        
        let dispatcher_handle = thread::spawn(move || {
            let mut manager = TaskManager::new(
                policy,
                manager_cpu.clone(),
                manager_workers.clone(),
            );

            let cpu_for_workers = manager_cpu;
            let workers_for_workers = manager_workers;

            let mut worker_senders = Vec::new();
            let mut worker_threads = Vec::new();
            
            for worker_id in 0..8 {
                let (worker_sender, worker_receiver) = mpsc::channel();
                worker_senders.push(worker_sender);
                
                let worker_cpu = Arc::clone(&cpu_for_workers);
                let worker_available = Arc::clone(&workers_for_workers);
                let comp_sender = completion_sender.clone();
                
                let worker_handle = thread::spawn(move || {
                    worker_thread(
                        worker_id,
                        worker_receiver,
                        comp_sender,          
                        worker_cpu,
                        worker_available,
                    );
                });
                
                worker_threads.push(worker_handle);
            }

            let num_workers = 8;
            let mut next_worker: usize = 0;
            let mut generator_done = false;

                loop {
                    if !generator_done {
                        match task_receiver.recv() {
                            Ok(task) => {
                                manager.receive_task(task);
                                *manager_io_len.lock().unwrap() = manager.io_queue_len();
                                *manager_cpu_len.lock().unwrap() = manager.cpu_queue_len();
                            }
                        Err(_) => {
                            generator_done = true;
                        }
                    }   
                }

                loop {
                    let free = *workers_for_workers.lock().unwrap();
                        if free == 0 {
                            break;
                        }

                    let mut task = match manager.select_next_task() {
                        Some(t) => t,
                        None => break,
                    };
                    {
                        let mut cpu = cpu_for_workers.lock().unwrap();
                            if *cpu + task.cpu_usage <= 1.0 {
                                *cpu += task.cpu_usage;
                            } else {
                                manager.push_task_front(task);
                                break;
                            }
                        }

                    let idx = next_worker % num_workers;
                    if worker_senders[idx].send(task).is_err() {
                        break;
                    }
                next_worker += 1;
                }
                
                if generator_done && manager.io_queue_len() == 0&& manager.cpu_queue_len() == 0{
                    if *workers_for_workers.lock().unwrap() == num_workers {
                        break;
                    }
                }

                if generator_done {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }

            drop(worker_senders);
            for handle in worker_threads {
                handle.join().unwrap();
            }
        });
        
        let monitor_cpu = Arc::clone(&global_cpu);
        let monitor_workers = Arc::clone(&worker_available);
        let monitor_io_len = Arc::clone(&io_queue_len);
        let monitor_cpu_len = Arc::clone(&cpu_queue_len);
        let monitor_running = Arc::clone(&running);

        let monitor_handle = thread::spawn(move || {
            monitor_thread(
                monitor_cpu,
                monitor_workers,
                monitor_io_len,
                monitor_cpu_len,
                monitor_running,
            )
        });

        gen_handle.join().unwrap();
        
        dispatcher_handle.join().unwrap();
        
        *running.lock().unwrap() = false;
        
        let snapshots = monitor_handle.join().unwrap();
        let mut completed_tasks = Vec::new();
        while let Ok(task) = completion_receiver.try_recv() {
            completed_tasks.push(task);
        }
        
        self.print_results(&snapshots, &completed_tasks);
    }
    
    fn write_monitor_csv(&self, filename: &str, snapshots: &[MonitorSnapshot]) {
        use std::io::Write;
        let mut file = std::fs::File::create(filename).expect("cannot create CSV");
        writeln!(file, "timestamp_ms,cpu_usage,available_workers,io_queue,cpu_queue").unwrap();
        for s in snapshots {
            writeln!(file, "{},{:.4},{},{},{}",
                s.timestamp.as_millis(),
                s.cpu_usage,
                s.available_workers,
                s.io_queue_length,
                s.cpu_queue_length
            ).unwrap();
        }
    }

    fn print_results(&self, snapshots: &[MonitorSnapshot], completed: &[CompletedTask]) {

        let io_completed = completed.iter().filter(|t| matches!(t.kind, TaskKind::IO)).count();
        let cpu_completed = completed.iter().filter(|t| matches!(t.kind, TaskKind::CPU)).count();

        let avg_wait_ms = if !completed.is_empty() {
            completed.iter().map(|t| t.wait_time.as_millis() as f64).sum::<f64>() / completed.len() as f64
        } else { 0.0 };

        let max_wait_ms = completed.iter()
            .map(|t| t.wait_time.as_millis())
            .max()
            .unwrap_or(0);

        let avg_turnaround_ms = if !completed.is_empty() {
            completed.iter()
                .map(|t| (t.wait_time + t.completion_time).as_millis() as f64)
                .sum::<f64>() / completed.len() as f64
        } else { 0.0 };

        let avg_cpu = if !snapshots.is_empty() {
            snapshots.iter().map(|s| s.cpu_usage).sum::<f64>() / snapshots.len() as f64
        } else { 0.0 };

        let avg_available = if !snapshots.is_empty() {
            snapshots.iter().map(|s| s.available_workers as f64).sum::<f64>() / snapshots.len() as f64
        } else { 0.0 };
        let avg_active = self.config.num_workers as f64 - avg_available;

        let makespan_ms = snapshots.last().map(|s| s.timestamp.as_millis()).unwrap_or(0);

        let total_runtime_ms = makespan_ms;

        println!("\n== {} simulation ==", self.config.scheduling_policy);
        println!("{} tasks, {}% IO / {}% CPU, {} workers, cap 100%",
            io_completed + cpu_completed,
            io_completed * 100 / (io_completed + cpu_completed),
            cpu_completed * 100 / (io_completed + cpu_completed),
            self.config.num_workers);
        println!("\n- results -");
        println!("total runtime    : {} ms", total_runtime_ms);
        println!("makespan         : {} ms", makespan_ms);
        println!("tasks completed  : {} (IO={}, CPU={})", io_completed + cpu_completed, io_completed, cpu_completed);
        println!("avg wait time    : {:.2} ms", avg_wait_ms);
        println!("avg turnaround time : {:.2} ms", avg_turnaround_ms);
        println!("max wait time    : {} ms", max_wait_ms);
        println!("avg CPU usage    : {:.2} %", avg_cpu * 100.0);
        println!("avg workers active : {:.2} / {}", avg_active, self.config.num_workers);
        println!("monitor samples  : {}", snapshots.len());
        let log_name = format!(
            "monitor_{}_{}io_{}cpu.csv",
            self.config.scheduling_policy, self.config.io_count, self.config.cpu_count
        );
        self.write_monitor_csv(&log_name, snapshots);
        println!("monitor csv    : {}", log_name);
    }
}