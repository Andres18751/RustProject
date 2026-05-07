# RustProject
Final Project for class 3334-01


About this project:

    This project is a concurrent dispatcher simulation that is ran in Rust. It focuses a lot on tasks that are being ran in the CPU bound and I/O bound, they are both ran in different queues that are dispatched to 8 workers that are focused in executing tasks. These queues are ran in FIFO and Optimized styles of queues which are then ran in different experiments which are then recorded in metrics that are printed in the end of the program. The purpose of the program is to understand the complexities of concurrencies in various threads as well as understand how queues function under different scenarios and compare their metrics in their circumstances.

How to run the project:

    Build:
        cargo build --release
    Run:
        cargo run --release

Sources used to help create this project:

I used ChatGPT as a way to help create the architechure/layout of the project

https://docs.rs/queues/latest/queues/

https://doc.rust-lang.org/rust-by-example/std_misc/channels.html

https://doc.rust-lang.org/reference/procedural-macros.html#r-macro.proc.derive

https://doc.rust-lang.org/rust-by-example/scope/move.html

Sites used to help understand the concepts and tools used in this project

Advice Accepted:

-Module separation: to keep the project organized

-Using Mutex: Was a great answer in dealing threads that have to use data and prevents dead locks

-Use VecDeque: Effective in a FIFO style Queue

-Spin loop: simulates CPU work without having to block the scheduler.

Advice Rejected:

-Crossbeam: was initially used for multi channel producers but it was not a standard library primitive

-Async/Await: async would increase difficulty and blocking channels was a simpler fix given the nature of the project