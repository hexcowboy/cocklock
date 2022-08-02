use std::thread::sleep;
use std::time::Duration;

use cocklock::{errors::CockLockError::NotAvailable, CockLock};

fn my_task() {
    println!("Doing my task.");
}

fn main() {
    let mut locker = CockLock::builder()
        .with_connection_strings(vec!["postgres://user:pass@localhost:5432/db"])
        .build()
        .unwrap();

    loop {
        match locker.lock("task", 10_000) {
            Ok(_) => my_task(),
            Err(NotAvailable) => println!("Someone else is doing my task!"),
            Err(err) => println!("Uh oh, some other error occurred: {err}"),
        };
        sleep(Duration::from_millis(1_000));
    }
}
