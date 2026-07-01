use mio::event::Source;
use mio::{Interest, Token};
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::IoReactor;
use super::ScheduledIo;
use super::reactor::IoHandle;

pub struct PollEvented<E: Source> {
    pub io: E,
    pub scheduled_io: Arc<ScheduledIo>,
    handle: IoHandle,
    token: Token,
}

impl<S: Source> PollEvented<S> {
    pub fn new(mut io: S) -> io::Result<Self> {
        let handle = IoReactor::current();

        let token = Token(handle.inner.next_token.fetch_add(1, Ordering::Relaxed));
        let scheduled_io = ScheduledIo::new();

        // Регистрируем
        handle
            .inner
            .sources
            .lock()
            .unwrap()
            .insert(token, scheduled_io.clone());
        handle
            .inner
            .registry
            .register(&mut io, token, Interest::READABLE | Interest::WRITABLE)?;

        Ok(Self {
            io,
            scheduled_io,
            handle,
            token,
        })
    }
}

impl<S: Source> Drop for PollEvented<S> {
    fn drop(&mut self) {
        let _ = self.handle.inner.registry.deregister(&mut self.io);

        let mut sources = self.handle.inner.sources.lock().unwrap();
        sources.remove(&self.token);
    }
}
