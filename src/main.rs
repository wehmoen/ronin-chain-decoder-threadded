use std::{thread};
use std::collections::HashMap;

use futures::stream::StreamExt;

use crate::mongodb::{Database, Transaction};
use crate::roninrest::{Adapter, RRDecodedTransaction};

mod mongodb;
mod roninrest;

type WorkerId = usize;

struct Worker {
    handle: thread::JoinHandle<()>,
}

impl Worker {
    fn new(handle: thread::JoinHandle<()>) -> Self {
        Worker {
            handle
        }
    }
}

struct DecodeParameter {
    tx: Transaction,
    rr: Adapter,
    db: Database
}

struct ThreadPool {
    max_threads: usize,
    worker: HashMap<WorkerId, Worker>,
    current_thread_id: usize,
}

impl ThreadPool {
    fn new(max_threads: usize) -> Self {
        ThreadPool {
            max_threads,
            worker: HashMap::new(),
            current_thread_id: 0,
        }
    }

    fn size(&self) -> usize {
        self.worker.len()
    }

    fn check_completion(&mut self) {
        let mut threads_to_remove: Vec<WorkerId> = vec![];
        for worker in &self.worker {
            if worker.1.handle.is_finished() {
                threads_to_remove.push(*worker.0);
            }
        }

        for thread in threads_to_remove {
            self.worker.remove(&thread);
        }

        println!("Running threads: {:?}", self.worker.keys());
    }

    fn can_spawn_new(&mut self) -> bool {
        self.check_completion();
        self.worker.len() < self.max_threads
    }

    fn finish(&mut self) {
        loop {
            self.check_completion();
            if self.size() == 0 {
                break;
            }
        }
    }

    fn spawn(&mut self, parameter: DecodeParameter, logic: fn(DecodeParameter)) -> Option<WorkerId> {
        if self.can_spawn_new() {
            let worker_id: WorkerId = self.current_thread_id;
            let thread = thread::Builder::new().name(format!("thread-decoder-{}", parameter.tx.hash)).spawn(move || {
                logic(parameter)
            }).unwrap();

            self.worker.insert(worker_id, Worker::new(thread));
            self.current_thread_id += 1;
            return Some(worker_id);
        }
        None
    }
}

#[tokio::main]
async fn main() {
    let db = Database::new("mongodb://127.0.0.1:27017", Some("ronin")).await;
    let mut rr = Adapter::new();
    rr.host = "http://localhost:3000".into();

    let last_block = db.last_block().await;

    let mut txs = db.transactions(last_block).await.expect("Failed to create transaction cursor");

    let max_threads = thread::available_parallelism().unwrap().get();
    let mut pool = ThreadPool::new(max_threads);

    loop {
        if pool.can_spawn_new() {
            if let Some(tx) = txs.next().await {
                let tx = tx.unwrap();
                pool.spawn(
                    DecodeParameter { tx, rr: rr.clone(), db: Database::new("mongodb://127.0.0.1:27017", Some("ronin")).await },
                    |p| {
                        _ = async {
                            let decoded: RRDecodedTransaction = RRDecodedTransaction {
                                from: p.tx.from,
                                to: p.tx.to,
                                hash: p.tx.hash.clone(),
                                block_number: p.tx.block as u64,
                                input: Some(p.rr.decode_method(&p.tx.hash).await),
                                output: Some(p.rr.decode_receipt(&p.tx.hash).await)
                            };

                            p.db.insert_decoded(&vec![decoded]).await.expect("Failed to insert tx to db!");
                        };
                    }
                );
            }
        }

        if pool.size() == 0 {
            break;
        }
    }

    pool.finish();

    println!("DONE")
}
