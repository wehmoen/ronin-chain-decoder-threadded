use mongodb::{Client, Collection, Cursor};
use mongodb::bson::{DateTime, doc};
use mongodb::options::{FindOneOptions, FindOptions};
use mongodb::results::{InsertManyResult};
use serde::{Deserialize, Serialize};
use crate::roninrest::RRDecodedTransaction;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Transaction {
    pub from: String,
    pub to: String,
    pub hash: String,
    pub block: u32,
    pub created_at: Option<DateTime>,
}


#[derive(Clone)]
pub struct Database {
    transactions: Collection<Transaction>,
    decoded_transactions: Collection<RRDecodedTransaction>,
}

impl Database {
    pub async fn new(db_uri: &str, db_name: Option<&str>) -> Database {
        let client = Client::with_uri_str(db_uri).await.unwrap();
        let database = client.database(&db_name.unwrap_or("ronin".into()));
        let transactions = database.collection::<Transaction>("transactions");
        let decoded_transactions = database.collection::<RRDecodedTransaction>("decoded_transactions");
        Database {
            transactions,
            decoded_transactions,
        }
    }

    pub async fn last_block(&self) -> u64 {
        let options = FindOneOptions::builder().sort(doc! {
            "blockNumber": -1
        }).build();
        match self.decoded_transactions.find_one(None, options).await.unwrap() {
            None => 0,
            Some(tx) => tx.block_number
        }
    }

    pub async fn one_transaction(&self, last_block: u64) -> mongodb::error::Result<Option<Transaction>> {
        // let options = FindOptions::builder()
        //     .no_cursor_timeout(Some(true))
        //     .batch_size(Some(100u32))
        //     .sort(doc! {
        //         "block": 1i64
        //     })
        //     .build();
        self.transactions.find_one(doc! {
            "block": {
                "$gt": last_block as i64
            }
        }, None).await
    }

    pub async fn transactions(&self, last_block: u64) -> mongodb::error::Result<Cursor<Transaction>> {
        let options = FindOptions::builder()
            .no_cursor_timeout(Some(true))
            .batch_size(Some(100u32))
            .sort(doc! {
                "block": 1i64
            })
            .build();
        self.transactions.find(doc! {
            "block": {
                "$gt": last_block as i64
            }
        }, options).await
    }

    pub async fn insert_decoded(&self, decoded: &Vec<RRDecodedTransaction>) -> mongodb::error::Result<InsertManyResult> {
        self.decoded_transactions.insert_many(decoded, None).await
    }
}