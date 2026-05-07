use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use crate::task::{Task, TaskKind};

pub struct TaskManager {
    io_queue: VecDeque<Task>,
    cpu_queue: VecDeque<Task>,
    global_cpu_usage: Arc<Mutex<f64>>,
    worker_available: Arc<Mutex<usize>>,
    max_cpu_usage: f64,
    policy: SchedulingPolicy,
}

#[derive(Debug, Clone)]
pub enum SchedulingPolicy {
    FIFO,
    Optimized,
}

impl TaskManager {
     pub fn new(policy: SchedulingPolicy, global_cpu: Arc<Mutex<f64>>, worker_available: Arc<Mutex<usize>>,) -> Self {
        TaskManager {io_queue: VecDeque::new(), cpu_queue: VecDeque::new(), global_cpu_usage: global_cpu, worker_available, max_cpu_usage: 1.0,  policy,}
    }

    pub fn receive_task(&mut self, task: Task) {
        match task.kind {
            TaskKind::IO => self.io_queue.push_back(task),
            TaskKind::CPU => self.cpu_queue.push_back(task),
        }
    }

    pub fn select_next_task(&mut self) -> Option<Task> {
        match self.policy {
            SchedulingPolicy::FIFO => self.select_fifo(),
            SchedulingPolicy::Optimized => self.select_optimized(),
        }
    }

    fn select_fifo(&mut self) -> Option<Task> {
        if !self.io_queue.is_empty() {
            self.io_queue.pop_front()
        } else if !self.cpu_queue.is_empty() {
            self.cpu_queue.pop_front()
        } else {
            None
        }
    }
     fn select_optimized(&mut self) -> Option<Task> {
        let cpu_usage = *self.global_cpu_usage.lock().unwrap();
        let workers = *self.worker_available.lock().unwrap();
        
        if workers == 0 {
            return None;
        }

        let can_add_cpu_task = cpu_usage + 0.35 <= self.max_cpu_usage;
        let can_add_io_task = cpu_usage + 0.10 <= self.max_cpu_usage;
        
        if can_add_cpu_task && !self.cpu_queue.is_empty() {
            self.cpu_queue.pop_front()
        } else if can_add_io_task && !self.io_queue.is_empty() {
            self.io_queue.pop_front()
        } else if workers >= 2 && !self.io_queue.is_empty() {
            self.io_queue.pop_front()
        } else if !self.cpu_queue.is_empty() {
            self.cpu_queue.pop_front()
        } else {
            self.io_queue.pop_front()
        }
    }
     pub fn io_queue_len(&self) -> usize {
        self.io_queue.len()
    }

    pub fn cpu_queue_len(&self) -> usize {
        self.cpu_queue.len()
    }

    pub fn push_task_front(&mut self, task: Task) {
        match task.kind {
            TaskKind::IO => self.io_queue.push_front(task),
            TaskKind::CPU => self.cpu_queue.push_front(task),
        }
    }
}