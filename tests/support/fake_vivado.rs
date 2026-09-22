//! Standalone Linux process fixture, compiled by the integration harness with rustc.
use std::{
    fs,
    io::{self, BufRead, Write},
    process::{self, Command},
    thread,
    time::Duration,
};

unsafe extern "C" {
    fn signal(number: i32, handler: usize) -> usize;
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let has = |flag: &str| args.iter().any(|arg| arg == flag);
    if has("--ignore-term") || has("--descendant") {
        // SIG_IGN is 1 on Linux. This fixture deliberately requires escalation to SIGKILL.
        unsafe {
            signal(15, 1);
        }
    }
    if has("--descendant") {
        loop {
            thread::sleep(Duration::from_secs(60));
        }
    }
    if has("--spawn-child") {
        let child = Command::new(std::env::current_exe().unwrap())
            .arg("--descendant")
            .spawn()
            .unwrap();
        fs::write(".fake-child-pid", child.id().to_string()).unwrap();
        // The supervisor, rather than this fixture, owns descendant cleanup.
        drop(child);
    }
    println!("fake Vivado ready");
    io::stdout().flush().unwrap();
    if has("--no-stdin") {
        loop {
            thread::sleep(Duration::from_secs(60));
        }
    }
    let exit_code = args
        .iter()
        .find_map(|arg| arg.strip_prefix("--exit-code="))
        .map(|code| code.parse::<i32>().unwrap())
        .unwrap_or(0);
    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        let text = line.trim();
        if text == "exit" {
            if has("--ignore-exit") {
                println!("ignoring exit");
            } else {
                println!("final output before exit");
                io::stdout().flush().unwrap();
                process::exit(exit_code);
            }
        } else if let Some(arguments) = text.strip_prefix("write ") {
            let (path, contents) = arguments.split_once(' ').unwrap();
            if let Some(parent) = std::path::Path::new(path).parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(path, contents).unwrap();
            println!("wrote {path}");
        } else if let Some(arguments) = text.strip_prefix("artifact ") {
            let (path, size) = arguments.split_once(' ').unwrap();
            fs::write(path, vec![42_u8; size.parse().unwrap()]).unwrap();
            println!("wrote {path}");
        } else {
            println!("echo: {text}");
        }
        io::stdout().flush().unwrap();
    }
}
