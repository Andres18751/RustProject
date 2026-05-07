use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, Instant};
use crate::task::{Task, TaskKind};

pub struct TaskGenerator {
    seed: u64,
    io_count: usize,
    cpu_count: usize,
}

impl TaskGenerator {
    pub fn new(io_count: usize, cpu_count: usize) -> Self {
        TaskGenerator {
            seed: 42,
            io_count,
            cpu_count,
        }
    }
    pub fn generate_and_send(&self, sender: Sender<Task>) {
        let mut rng = ChaCha8Rng::seed_from_u64(self.seed);
        let total_tasks = self.io_count + self.cpu_count;
        let mut tasks: Vec<Task> = Vec::with_capacity(total_tasks);

        for i in 0..self.io_count {
            tasks.push(Task::new(i, TaskKind::IO, Instant::now())); 
        }
        for i in 0..self.cpu_count {
            tasks.push(Task::new(self.io_count + i, TaskKind::CPU, Instant::now()));
        }

        for i in (1..tasks.len()).rev() {
            let j = rng.gen_range(0..=i);
            tasks.swap(i, j);
        }


         for (index, mut task) in tasks.into_iter().enumerate() {
            task.id = index;
            thread::sleep(Duration::from_millis(20));    
            task.arrival_time = Instant::now();                

            if sender.send(task).is_err() {
                break;
            }
        }
    }
}