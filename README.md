# Cock Lock (Cockroach Locks)

> There is no better name for this project

A Decentralized Locking System (DLS) made for CockroachDB and Postgres written in pure Rust. Motivation for this project was to create a DLS that didn't use the Redis Redlock protocol ([since I saw some nerds arguing about it on the internet](https://news.ycombinator.com/item?id=11065933)). This project also contains more features than traditional Redlock implementations.

<hr />

- [Install](#Install)
- [Usage](#Usage)
- [License](#License)

<hr />

⚠️ This project is not released and I haven't written tests yet. Please don't use, I promise I will finish it soon.

<hr />

## Install

Not published to crates.io yet, check back later.

<hr />

## Usage

```rs
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
```

<hr />

## License

Haven't picked a license yet but when I do it will be very permissive.