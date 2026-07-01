use crate::io::{PollEvented, ScheduledIo};
use mio::net;
use std::future::Future;
use std::io::{self, ErrorKind, Read, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct AsyncAccept<'a> {
    io: &'a net::TcpListener,
    scheduled_io: Arc<ScheduledIo>,
}

pub struct AsyncRead<'a> {
    io: &'a mut net::TcpStream,
    scheduled_io: Arc<ScheduledIo>,
    buf: &'a mut [u8],
}

pub struct AsyncWrite<'a> {
    io: &'a mut net::TcpStream,
    scheduled_io: Arc<ScheduledIo>,
    buf: &'a [u8],
    written: usize,
}

impl PollEvented<net::TcpStream> {
    pub fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> AsyncRead<'a> {
        AsyncRead {
            io: &mut self.io,
            scheduled_io: Arc::clone(&self.scheduled_io),
            buf,
        }
    }

    pub fn write<'a>(&'a mut self, buf: &'a [u8]) -> AsyncWrite<'a> {
        AsyncWrite {
            io: &mut self.io,
            scheduled_io: Arc::clone(&self.scheduled_io),
            buf,
            written: 0,
        }
    }
}

impl PollEvented<net::TcpListener> {
    pub fn accept(&mut self) -> AsyncAccept<'_> {
        AsyncAccept {
            io: &mut self.io,
            scheduled_io: Arc::clone(&self.scheduled_io),
        }
    }
}

impl<'a> Future for AsyncRead<'a> {
    type Output = io::Result<usize>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        loop {
            if let Poll::Pending = this.scheduled_io.poll_readable(cx) {
                return Poll::Pending;
            }

            match this.io.read(this.buf) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                    this.scheduled_io.clear_readiness(crate::io::READABLE);
                }
                Err(err) => return Poll::Ready(Err(err)),
            }
        }
    }
}

impl<'a> Future for AsyncWrite<'a> {
    type Output = io::Result<usize>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        loop {
            if let Poll::Pending = this.scheduled_io.poll_writable(cx) {
                return Poll::Pending;
            }

            match this.io.write(&this.buf[this.written..]) {
                Ok(0) => return Poll::Ready(Ok(this.written)),
                Ok(n) => {
                    this.written += n;
                    if this.written == this.buf.len() {
                        return Poll::Ready(Ok(this.written));
                    }
                }
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                    this.scheduled_io.clear_readiness(crate::io::WRITABLE);
                }
                Err(err) => return Poll::Ready(Err(err)),
            }
        }
    }
}

impl<'a> Future for AsyncAccept<'a> {
    type Output = io::Result<net::TcpStream>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        loop {
            if let Poll::Pending = this.scheduled_io.poll_readable(cx) {
                return Poll::Pending;
            }

            match this.io.accept() {
                Ok((stream, _)) => return Poll::Ready(Ok(stream)),
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                    this.scheduled_io.clear_readiness(crate::io::READABLE);
                }
                Err(err) => return Poll::Ready(Err(err)),
            }
        }
    }
}

// Пример высокоуровневой обертки
pub struct TcpListener {
    inner: PollEvented<net::TcpListener>,
}

impl TcpListener {
    pub fn new(listener: net::TcpListener) -> io::Result<TcpListener> {
        // Здесь PollEvented берет IoHandle из thread_local и регистрирует сокет
        let inner = PollEvented::new(listener)?;
        Ok(Self { inner })
    }
    pub async fn accept(&mut self) -> io::Result<net::TcpStream> {
        self.inner.accept().await
    }
}

pub struct TcpStream {
    inner: PollEvented<net::TcpStream>,
}
impl TcpStream {
    pub fn new(stream: net::TcpStream) -> io::Result<TcpStream> {
        let inner = PollEvented::new(stream)?;
        Ok(Self { inner })
    }

    pub async fn read<'a>(&mut self, buf: &'a mut [u8]) -> io::Result<usize> {
        self.inner.read(buf).await
    }

    pub async fn write<'a>(&mut self, buf: &'a [u8]) -> io::Result<usize> {
        self.inner.write(buf).await
    }
}
