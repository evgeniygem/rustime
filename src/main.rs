mod io;
mod net;
mod rt;

async fn server_run(mut stream: net::TcpStream) {
    let mut buf = [0u8; 1024];

    loop {
        let n = stream
            .read(&mut buf)
            .await
            .inspect_err(|e| eprintln!("failed to read from socket; err = {:?}", e))
            .unwrap_or(0);

        if n == 0 {
            return;
        }

        println!("Message: {:?}", buf);

        let _ = stream.write(&buf[..n]).await;
    }
}

fn main() {
    let runtime = rt::Runtime::new();

    runtime.run();

    let rt = runtime.clone();

    let handle = rt.spawn(async move {
        let mut listener = net::TcpListener::new(
            mio::net::TcpListener::bind("127.0.0.1:8080".parse().unwrap()).unwrap(),
        )
        .unwrap();

        loop {
            let stream = listener.accept().await.unwrap();
            runtime.spawn(server_run(net::TcpStream::new(stream).unwrap()));
        }
    });

    rt::util::block_on(handle);

    runtime.shutdown();
}
