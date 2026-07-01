use super::task::Task;
use crate::io::IoReactor;
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

struct RoundRobinWakers {
    wakers: Vec<Arc<mio::Waker>>,
    next_waker: AtomicUsize,
}

impl RoundRobinWakers {
    fn new(wakers: Vec<Arc<mio::Waker>>) -> Self {
        Self {
            wakers,
            next_waker: AtomicUsize::new(0),
        }
    }

    fn next_waker(&self) -> Arc<mio::Waker> {
        let index = self.next_waker.fetch_add(1, Ordering::Relaxed);
        self.wakers[index % self.wakers.len()].clone()
    }
}

impl<'a> IntoIterator for &'a RoundRobinWakers {
    type Item = &'a Arc<mio::Waker>;
    type IntoIter = std::slice::Iter<'a, Arc<mio::Waker>>;

    fn into_iter(self) -> Self::IntoIter {
        self.wakers.iter()
    }
}

pub(crate) struct Executor {
    injector: Injector<Arc<Task>>,
    stealers: Mutex<Vec<Stealer<Arc<Task>>>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    wakers: OnceLock<RoundRobinWakers>,
    shutdown: AtomicBool,
}

fn work(
    executor: Arc<Executor>,
    mut io_reactor: IoReactor,
    worker: Worker<Arc<Task>>,
    stealers: Vec<Stealer<Arc<Task>>>,
) {
    loop {
        // Try local
        if let Some(task) = worker.pop() {
            task.poll();
            continue;
        }

        // Try injector
        match executor.injector.steal() {
            Steal::Success(task) => {
                task.poll();
                continue;
            }
            Steal::Retry => continue,
            Steal::Empty => {
                // Try stealing from other workers
                let mut found = None;

                for stealer in &stealers {
                    match stealer.steal() {
                        Steal::Success(task) => {
                            found = Some(task);
                            break;
                        }
                        Steal::Retry => continue,
                        Steal::Empty => {}
                    }
                }

                if let Some(task) = found {
                    task.poll();
                    continue;
                }

                // No work: park (wait on condvar)
                if executor.shutdown.load(Ordering::Acquire) {
                    break;
                }

                io_reactor
                    .run(Some(Duration::from_millis(500)))
                    .expect("Failed to run IO reactor");
            }
        }
    }
}

impl Executor {
    pub fn new() -> Self {
        Self {
            injector: Injector::new(),
            stealers: Mutex::new(Vec::new()),
            threads: Mutex::new(Vec::new()),
            wakers: OnceLock::new(),
            shutdown: AtomicBool::new(false),
        }
    }

    /// Spawn a multi-threaded pool with `n_threads`
    pub fn run(self: &Arc<Self>, threads: usize) {
        // Create local workers and their stealers first
        let mut workers = Vec::with_capacity(threads);
        let mut stealers = Vec::with_capacity(threads);
        let mut wakers = Vec::with_capacity(threads);

        for _ in 0..threads {
            let worker = Worker::new_fifo();
            stealers.push(worker.stealer());
            workers.push(worker);
        }

        // Publish stealers to executor (shared)
        {
            let mut stealers_guard = self.stealers.lock().unwrap();
            *stealers_guard = stealers.clone();
        }

        // Spawn threads, move each worker into its thread
        let threads_slot = &mut self.threads.lock().unwrap();
        for i in 0..threads {
            let worker = workers.pop().unwrap();

            let executor = self.clone();

            let worker_stealers = stealers.clone();

            let (io_reactor, io_handle) = IoReactor::new().expect("failed to create io reactor");

            let waker = io_reactor.waker().expect("failed to waking io thread");
            wakers.push(Arc::new(waker));

            let handle = thread::Builder::new()
                .name(format!("worker-{}", i))
                .spawn(move || {
                    IoReactor::set_handle(io_handle);
                    work(executor, io_reactor, worker, worker_stealers);
                })
                .unwrap();

            threads_slot.push(handle);
        }

        self.wakers
            .get_or_init(move || RoundRobinWakers::new(wakers));
    }

    pub fn schedule(&self, task: Arc<Task>) {
        self.injector.push(task);

        // Wake up one sleeping thread if any
        if let Some(wakers) = self.wakers.get() {
            wakers
                .next_waker()
                .wake()
                .expect("Failed to wake up thread")
        }
    }

    #[allow(unused)]
    pub fn spawn<F>(self: &Arc<Self>, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Arc::new(Task {
            future: Mutex::new(Some(Box::pin(future))),
            executor: Arc::clone(self),
        });

        self.schedule(task);
    }

    #[allow(unused)]
    pub fn shutdown(self: &Arc<Self>) {
        self.shutdown.store(true, Ordering::Release);

        if let Some(wakers) = self.wakers.get() {
            for waker in wakers.into_iter() {
                waker.wake().expect("Failed to wake up thread");
            }
        }

        let mut threads = self.threads.lock().unwrap();
        while let Some(thread) = threads.pop() {
            thread.join().unwrap();
        }
    }
}
