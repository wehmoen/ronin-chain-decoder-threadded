use structopt::StructOpt;
use tokio::task::JoinHandle;

use crate::mongodb::{Database, Transaction};
use crate::roninrest::{Adapter, RRDecodedTransaction};

use indicatif::{ProgressBar, ProgressStyle};

mod mongodb;
mod roninrest;

struct DecodeParameter {
    tx: Transaction,
    // shared_config: SdkConfig,
    local: bool,
}

#[derive(Debug, StructOpt)]
struct Opt {
    /// Use localhost
    #[structopt(short, long)]
    local: Option<bool>,

    ///DB Name
    #[structopt(short, long, default_value="ronin")]
    db_name: String
}

async fn thread_work(params: DecodeParameter) {
    let out_path = ["out", &params.tx.block.to_string()].join("/");
    let out_path_str = out_path.clone();
    let out_path = std::path::Path::new(&out_path);
    if !out_path.exists() {
        std::fs::create_dir_all(&out_path).unwrap();
    }

    let key = [
        out_path_str,
        params.tx.hash.clone()[2..].into(),
    ].join("/");
    let key = [
        key,
        "json".into()
    ].join(".");
    let key = key.as_str();

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

    std::fs::write(&key, json_string).unwrap();

    drop(rr);
    drop(decoded);
}

#[tokio::main]
async fn main() {
    let Opt { local, db_name } = Opt::from_args();

    let out_path = std::path::Path::new("out");
    if !out_path.exists() {
        std::fs::create_dir_all(&out_path).unwrap();
    }

    let db = Database::new("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.5.4", Some(&db_name)).await;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io().enable_time()
        .thread_name("decoder-thread")
        .build().unwrap();

    let tx_in_db = db.estimate_remaining().await.unwrap();
    let limit: usize = 500;

    let pb = ProgressBar::new(tx_in_db);

    pb.set_style(
        ProgressStyle::default_spinner().template("{spinner}{bar:80.cyan/blue} {percent:>3}% | [{eta_precise}][{elapsed_precise}] ETA/Elapsed | {msg}{pos:>12}/{len:12}").unwrap()
    );

    let mut things: Vec<JoinHandle<()>> = vec![];
    let mut close_on_loop_end: bool = false;
    loop {
        while things.len() < limit {
            if let Some(tx) = db.one_transaction().await.unwrap() {
                let task = thread_work(DecodeParameter
                {
                    tx: tx.clone(),
                    local: local.unwrap_or(false),
                });

                things.push(rt.spawn(task));
                pb.set_message(tx.hash);
                pb.inc(1);
            } else {
                close_on_loop_end = true
            }
        }

        for thing in things.iter_mut() {
            thing.await.ok();
        }

        things.clear();

        if close_on_loop_end {
            break;
        }
    }

    pb.finish_with_message("DONE!!!!");
}
