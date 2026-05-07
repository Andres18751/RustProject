use std::time::{Duration, Instant};
#[derive(Debug, Clone)]
pub enum TaskKind {
    IO,
    CPU,
}
#[derive(Debug, Clone)]
pub struct Task {
    pub id: usize,
    pub arrival_time: Instant,
    pub kind: TaskKind,
    pub duration: Duration,
    pub cpu_usage: f64, 
}
impl Task {
    pub fn new(id: usize, kind: TaskKind, arrival_time: Instant) -> Self {
        let (duration, cpu_usage) = match kind {
            TaskKind::IO => (Duration::from_millis(200), 0.10),
            TaskKind::CPU => (Duration::from_millis(200), 0.35),
        };
        
        Task {
            id,
            arrival_time,
            kind,
            duration,
            cpu_usage,
        }
    }
}