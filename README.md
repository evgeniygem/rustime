# Rustime

**Rustime** is a minimalistic async runtime for Rust, built from scratch.

It demonstrates how modern asynchronous runtimes work (heavily inspired by Tokio), using `mio` for non-blocking I/O and `crossbeam-deque` for task scheduling.

## Features

- Multi-threaded work-stealing executor
- Asynchronous TCP server and client support
- Custom I/O reactor built on top of `mio`
- Full `async/await` support
- Clean and educational codebase

## Example

```rust
use rustime::rt::Runtime;
use rustime::net::{TcpListener, TcpStream};

async fn server_run(mut stream: TcpStream) {
    let mut buf = [0u8; 1024];

    loop {
        let n = stream.read(&mut buf).await.unwrap_or(0);
        if n == 0 {
            return;
        }

        println!("Message: {:?}", &buf[..n]);
        let _ = stream.write(&buf[..n]).await;
    }
}

fn main() {
    let runtime = Runtime::new();
    runtime.run();

    let rt = runtime.clone();

    let handle = rt.spawn(async move {
        let mut listener = TcpListener::new(
            mio::net::TcpListener::bind("127.0.0.1:8080".parse().unwrap()).unwrap()
        ).unwrap();

        loop {
            let stream = listener.accept().await.unwrap();
            runtime.spawn(server_run(TcpStream::new(stream).unwrap()));
        }
    });

    rustime::rt::util::block_on(handle);
    runtime.shutdown();
}
```

## Project Structure

```
src/
├── main.rs          # Example TCP echo server
├── net.rs           # Async TcpListener / TcpStream wrappers
├── io/
│   ├── mod.rs
│   ├── reactor.rs   # mio-based reactor
│   ├── poll.rs      # PollEvented abstraction
│   └── sheduled.rs  # ScheduledIo
└── rt/
    ├── mod.rs
    ├── runtime.rs
    ├── executor.rs  # Multi-threaded work-stealing executor
    └── task.rs      # Task, JoinHandle, custom Waker
```

## Quick Start

```bash
git clone https://github.com/evgeniygem/rustime.git
cd rustime
cargo run
```

The echo server will start on `127.0.0.1:8080`.

## Goals

- Educational — to show how async runtimes work under the hood
- Experimental — testing ideas in the field of high-performance concurrency

## License
MIT © 2026 evgeniygem