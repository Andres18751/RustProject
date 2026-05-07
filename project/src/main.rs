mod task;
mod generator;
mod manager;
mod worker;
mod monitor;
mod simulation; 

use simulation::{Simulation, SimulationConfig};

fn main() {
    println!("Task Dispatcher Simulation");
    
    run_experiment("FIFO", 700, 300);
    run_experiment("Optimized", 700, 300);
    
    run_experiment("FIFO", 800, 200);
    run_experiment("Optimized", 800, 200);
}

fn run_experiment(policy: &str, io_tasks: usize, cpu_tasks: usize) {
    let config = SimulationConfig {
        num_workers: 8,
        io_count: io_tasks,
        cpu_count: cpu_tasks ,
        scheduling_policy: policy.to_string(),
    };
    
    let simulation = Simulation::new(config);
    simulation.run();
}