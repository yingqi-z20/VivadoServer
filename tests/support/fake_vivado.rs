use std::io::{self, BufRead, Write};

fn main() {
    println!("fake Vivado ready");
    io::stdout().flush().unwrap();

    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        let trimmed = line.trim();
        if trimmed == "exit" {
            println!("fake Vivado exiting");
            io::stdout().flush().unwrap();
            break;
        }

        println!("echo: {trimmed}");
        io::stdout().flush().unwrap();
    }
}
