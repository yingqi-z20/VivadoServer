use std::io::{self, BufRead, Write};

fn main() {
    let ignore_exit = std::env::args().any(|arg| arg == "--ignore-exit");
    println!("fake Vivado ready");
    io::stdout().flush().unwrap();

    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        let trimmed = line.trim();
        if trimmed == "exit" {
            if ignore_exit {
                println!("ignoring exit");
                io::stdout().flush().unwrap();
                continue;
            }
            println!("fake Vivado exiting");
            io::stdout().flush().unwrap();
            break;
        }

        println!("echo: {trimmed}");
        io::stdout().flush().unwrap();
    }
}
