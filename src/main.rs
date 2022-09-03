
use std::thread;

use aws_config::meta::region::RegionProviderChain;
use aws_config::SdkConfig;
use aws_sdk_s3::{Client as S3Client, Region, types::ByteStream};
use futures::stream::StreamExt;
use structopt::StructOpt;

use crate::mongodb::{Database, Transaction};
use crate::roninrest::{Adapter, RRDecodedTransaction};

mod mongodb;
mod roninrest;

struct DecodeParameter {
    tx: Transaction,
    shared_config: SdkConfig,
    local: bool
}

#[derive(Debug, StructOpt)]
struct Opt {
    /// The AWS Region.
    #[structopt(short, long)]
    region: Option<String>,

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

    println!("DONE: {}", key);
}

#[tokio::main]
async fn main() {
    let Opt { region, local } = Opt::from_args();

    let region_provider = RegionProviderChain::first_try(region.map(Region::new))
        .or_default_provider()
        .or_else(Region::new("eu-central-1"));

    let shared_config = aws_config::from_env()
        .credentials_provider(
            aws_config::profile::ProfileFileCredentialsProvider::builder()
                .profile_name("ronin")
                .build()
        )
        .region(region_provider)
        .load()
        .await;


    let db = Database::new("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.5.4", Some("ronin")).await;

    let last_block = db.last_block().await;

    let mut txs = db.transactions(last_block).await.expect("Failed to create transaction cursor");

    // let max_threads = thread::available_parallelism().unwrap().get();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io().enable_time()
        .thread_name("decoder-thread")
        .build().unwrap();

    while let Some(tx) = txs.next().await {
        let tx = tx.unwrap();
        let task = thread_work(DecodeParameter
        {
            tx,
            shared_config: shared_config.clone(),
            local: local.unwrap_or(false)
        });

        rt.spawn(task);
    }


    println!("DONE")
}
