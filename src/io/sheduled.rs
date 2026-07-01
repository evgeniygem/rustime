use super::READABLE;
use super::WRITABLE;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll as TaskPoll;
use std::task::{Context, Waker};

struct Waiters {
    reader: Option<Waker>,
    writer: Option<Waker>,
}

pub struct ScheduledIo {
    readiness: AtomicUsize,
    waiters: Mutex<Waiters>,
}

impl ScheduledIo {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            readiness: AtomicUsize::new(0),
            waiters: Mutex::new(Waiters {
                reader: None,
                writer: None,
            }),
        })
    }

    pub fn set_readiness(&self, ready: usize) {
        self.readiness.fetch_or(ready, Ordering::Release);
        let mut lock = self.waiters.lock().unwrap();

        // Используем побитовое сравнение
        if ready & READABLE != 0 {
            if let Some(waker) = lock.reader.take() {
                waker.wake();
            }
        }
        if ready & WRITABLE != 0 {
            if let Some(waker) = lock.writer.take() {
                waker.wake();
            }
        }
    }

    pub fn clear_readiness(&self, mask: usize) {
        self.readiness.fetch_and(!mask, Ordering::Release);
    }

    pub fn poll_readable(&self, cx: &Context<'_>) -> std::task::Poll<()> {
        if self.readiness.load(Ordering::Acquire) & READABLE != 0 {
            return TaskPoll::Ready(());
        }

        let mut lock = self.waiters.lock().unwrap();

        if self.readiness.load(Ordering::Acquire) & READABLE != 0 {
            return TaskPoll::Ready(());
        }

        match &mut lock.reader {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => lock.reader = Some(cx.waker().clone()),
        }

        TaskPoll::Pending
    }

    pub fn poll_writable(&self, cx: &Context<'_>) -> std::task::Poll<()> {
        if self.readiness.load(Ordering::Acquire) & WRITABLE != 0 {
            return TaskPoll::Ready(());
        }

        let mut lock = self.waiters.lock().unwrap();
        if self.readiness.load(Ordering::Acquire) & WRITABLE != 0 {
            return TaskPoll::Ready(());
        }

        match &mut lock.writer {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => lock.writer = Some(cx.waker().clone()),
        }

        TaskPoll::Pending
    }
}
