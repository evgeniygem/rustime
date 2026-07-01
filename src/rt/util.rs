use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::thread;
use std::thread::Thread;

static THREAD_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

unsafe fn clone(ptr: *const ()) -> RawWaker {
    RawWaker::new(ptr, &THREAD_WAKER_VTABLE)
}

unsafe fn wake(ptr: *const ()) {
    unsafe { wake_by_ref(ptr) }
}

unsafe fn wake_by_ref(ptr: *const ()) {
    unsafe {
        let ref thread = *(ptr as *const Thread);
        thread.unpark();
    }
}

unsafe fn drop(_ptr: *const ()) {}

fn get_thread_waker(thread: &Thread) -> Waker {
    unsafe { Waker::new(thread as *const Thread as *const (), &THREAD_WAKER_VTABLE) }
}

pub fn block_on<F, T>(future: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    // Put future into a task-like wrapper
    let mut fut = Box::pin(future);

    let thread = thread::current();
    let waker = get_thread_waker(&thread);
    let mut cx = Context::from_waker(&waker);

    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => {
                thread::park();
            }
        }
    }
}
