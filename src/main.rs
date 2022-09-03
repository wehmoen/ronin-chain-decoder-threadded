use std::collections::HashMap;
use std::thread;

use aws_config::meta::region::RegionProviderChain;
use aws_config::profile::Property;
use aws_config::SdkConfig;
use aws_sdk_s3::{Client as S3Client, Region, types::ByteStream};
use futures::stream::StreamExt;
use structopt::StructOpt;
use tokio::task::JoinHandle;

use crate::mongodb::{Database, Transaction};
use crate::roninrest::{Adapter, RRDecodedTransaction};

mod mongodb;
mod roninrest;

struct DecodeParameter {
    tx: Transaction,
    shared_config: SdkConfig,
    local: bool,
}

#[derive(Debug, StructOpt)]
struct Opt {
    /// Use localhost
    #[structopt(short, long)]
    local: Option<bool>,

}

async fn thread_work(params: DecodeParameter) {
    let s3_client = S3Client::new(&params.shared_config);

    let key = &params.tx.hash.clone()[2..];

    let mut rr = Adapter::new();

    if params.local {
        rr.host = "http://localhost:3000".into();
    }

    let decoded = RRDecodedTransaction {
        from: params.tx.from,
        to: params.tx.to,
        block_number: params.tx.block as u64,
        input: Some(rr.decode_method(&params.tx.hash).await),
        output: Some(rr.decode_receipt(&params.tx.hash).await),
        hash: params.tx.hash,
    };

    let json_string = serde_json::to_string(&decoded).unwrap();

    s3_client
        .put_object()
        .bucket("ronindecode")
        .key(key)
        .body(ByteStream::from(json_string.into_bytes()))
        .send()
        .await.expect("Failed to upload file");

    drop(s3_client);
    drop(rr);
    drop(decoded);

    println!("DONE: {}", key);
}

#[tokio::main]
async fn main() {
    let Opt { local } = Opt::from_args();


    let shared_config = aws_config::load_from_env().await;


    let db = Database::new("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.5.4", Some("ronin")).await;

    let last_block = db.last_block().await;

    // let mut txs = db.transactions(last_block).await.expect("Failed to create transaction cursor");

    // let max_threads = thread::available_parallelism().unwrap().get();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(4)
        .enable_io().enable_time()
        .thread_name("decoder-thread")
        .build().unwrap();

    let mut index: i32 = 0;
    let limit: usize = 100;

    let mut things: Vec<JoinHandle<()>> = vec![];
    let mut close_on_loop_end: bool = false;
    loop {
        while things.len()  < limit {
            println!("Pending: {}", things.len());
            if let tx = db.one_transaction(last_block).await.unwrap().unwrap() {
                let task = thread_work(DecodeParameter
                {
                    tx: tx.clone(),
                    shared_config: shared_config.clone(),
                    local: local.unwrap_or(false),
                });

                things.push(rt.spawn(task));
                index += 1;
            } else {
                close_on_loop_end = true
            }
        }

        for thing in things.iter_mut() {
            thing.await.ok();
        }

        things.clear();

        if close_on_loop_end || index > 200 {
            break;
        }

    }

    // while let Some(tx) = txs.next().await {
    //     let tx = tx.unwrap();
    //     let task = thread_work(DecodeParameter
    //     {
    //         tx,
    //         shared_config: shared_config.clone(),
    //         local: local.unwrap_or(false),
    //     });
    //
    //     rt.spawn(task);
    // }


    println!("DONE")
}
