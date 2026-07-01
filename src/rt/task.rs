use super::Executor;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

mod inner {
    use crate::rt::task::Task;
    use std::mem::ManuallyDrop;
    use std::sync::Arc;
    use std::task::{RawWaker, RawWakerVTable};

    pub(super) static WAKER_VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone, wake, wake_by_ref, std::mem::drop);

    /// A RawWaker that holds an Arc<Task>. When woken, it schedules the task into the executor's injector
    unsafe fn clone(ptr: *const ()) -> RawWaker {
        let this = unsafe { Arc::from_raw(ptr as *const Task) };
        let task = this.clone();
        let _ = ManuallyDrop::new(this);
        RawWaker::new(Arc::into_raw(task) as *const (), &WAKER_VTABLE)
    }

    unsafe fn wake(ptr: *const ()) {
        let this = unsafe { Arc::from_raw(ptr as *const Task) };
        let executor = this.executor.clone();
        executor.schedule(this);
    }

    unsafe fn wake_by_ref(ptr: *const ()) {
        let this = unsafe { Arc::from_raw(ptr as *const Task) };
        let task = this.clone();
        let _ = ManuallyDrop::new(this);

        let executor = task.executor.clone();
        executor.schedule(task);
    }

    #[allow(unused)]
    unsafe fn drop(ptr: *const ()) {
        let _ = unsafe { Arc::from_raw(ptr as *const Task) };
    }
}

fn get_task_waker(task: Arc<Task>) -> Waker {
    unsafe { Waker::new(Arc::into_raw(task) as *const (), &inner::WAKER_VTABLE) }
}

pub(super) struct Task {
    pub(super) future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>>,
    pub(super) executor: Arc<Executor>,
}

pub(super) struct TaskResult<T> {
    value: Mutex<Option<T>>,
    wakers: Mutex<Vec<Waker>>,
}

impl Task {
    pub fn poll(self: &Arc<Self>) {
        // Take the future to poll, leaving None while executing
        let mut fut = {
            let mut guard = self.future.lock().unwrap();
            if guard.is_none() {
                // Nothing to do (already completed)
                return;
            }

            guard.take().unwrap()
        };

        let waker = get_task_waker(self.clone());
        let mut cx = Context::from_waker(&waker);

        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(()) => {
                // finished, nothing to do
            }
            Poll::Pending => {
                // Put it back so it can be polled later
                let mut guard = self.future.lock().unwrap();
                *guard = Some(fut);
            }
        }
    }
}

impl<T> TaskResult<T> {
    pub fn new() -> Self {
        Self {
            value: Mutex::new(None),
            wakers: Mutex::new(Vec::new()),
        }
    }

    pub fn set(&self, value: T) {
        *self.value.lock().unwrap() = Some(value);

        let mut wakers = self.wakers.lock().unwrap();

        for waker in wakers.drain(..) {
            waker.wake();
        }
    }

    pub fn poll(&self, cx: &mut Context<'_>) -> Poll<T>
    where
        T: Clone,
    {
        {
            let guard = self.value.lock().unwrap();
            if let Some(value) = guard.as_ref() {
                return Poll::Ready(value.clone());
            }
        }

        // Register waker
        let mut wakers = self.wakers.lock().unwrap();
        wakers.push(cx.waker().clone());
        Poll::Pending
    }
}

pub struct JoinHandle<T> {
    inner: Arc<TaskResult<T>>,
}

impl<T> JoinHandle<T> {
    pub(super) fn new(value: Arc<TaskResult<T>>) -> Self {
        Self { inner: value }
    }
}

impl<T> Future for JoinHandle<T>
where
    T: Clone + 'static + Send,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        self.inner.poll(cx)
    }
}
