use super::ScheduledIo;
use mio::{Events, Poll, Token};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::READABLE;
use super::WRITABLE;

const WAKER_TOKEN: Token = Token(usize::MAX);

pub(super) struct Inner {
    pub(super) registry: mio::Registry,
    pub(super) sources: Mutex<HashMap<Token, Arc<ScheduledIo>>>,
    pub(super) next_token: AtomicUsize,
}

#[derive(Clone)]
pub struct IoHandle {
    pub(super) inner: Arc<Inner>,
}

pub struct IoReactor {
    poll: Poll,
    inner: Arc<Inner>,
}

thread_local! {
    static REACTOR: RefCell<Option<IoHandle>> = RefCell::new(None);
}

impl IoReactor {
    pub fn new() -> io::Result<(Self, IoHandle)> {
        let poll = Poll::new()?;
        let inner = Arc::new(Inner {
            registry: poll.registry().try_clone()?,
            sources: Mutex::new(HashMap::new()),
            next_token: AtomicUsize::new(0),
        });

        Ok((
            IoReactor {
                poll,
                inner: inner.clone(),
            },
            IoHandle { inner },
        ))
    }

    pub fn run(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        let mut events = Events::with_capacity(1024);

        self.poll.poll(&mut events, timeout)?;

        let sources = self.inner.sources.lock().unwrap();

        for event in events.iter() {
            let token = event.token();

            if token == WAKER_TOKEN {
                continue; // Просто проснулись по сигналу планировщика
            }

            if let Some(scheduled_io) = sources.get(&token) {
                let mut ready = 0;
                if event.is_readable() {
                    ready |= READABLE;
                }
                if event.is_writable() {
                    ready |= WRITABLE;
                }

                scheduled_io.set_readiness(ready);
            }
        }

        Ok(())
    }

    pub fn waker(&self) -> io::Result<mio::Waker> {
        mio::Waker::new(self.poll.registry(), WAKER_TOKEN)
    }

    pub fn set_handle(handle: IoHandle) {
        REACTOR.with(|r| *r.borrow_mut() = Some(handle));
    }

    pub fn current() -> IoHandle {
        REACTOR.with(|r| r.borrow().as_ref().unwrap().clone())
    }
}
