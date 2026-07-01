use super::Executor;
use super::task::{JoinHandle, Task, TaskResult};

use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Clone)]
pub struct Runtime {
    executor: Arc<Executor>,
}

impl Runtime {
    pub fn new() -> Self {
        Self {
            executor: Arc::new(Executor::new()),
        }
    }

    pub fn run(&self) {
        self.executor
            .run(thread::available_parallelism().unwrap().get())
    }

    pub fn spawn<F, T>(&self, fut: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Clone + Send + 'static,
    {
        let shared = Arc::new(TaskResult::<T>::new());
        let shared_clone = shared.clone();

        let future = Box::pin(async move {
            let result = fut.await;
            shared.set(result);
        });

        let task = Arc::new(Task {
            future: Mutex::new(Some(future)),
            executor: self.executor.clone(),
        });

        self.executor.schedule(task);

        JoinHandle::new(shared_clone)
    }

    pub fn shutdown(&self) {
        self.executor.shutdown();
    }
}
