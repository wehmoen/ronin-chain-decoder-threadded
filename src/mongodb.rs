use mongodb::{Client, Collection};
use mongodb::bson::{DateTime, doc};
use mongodb::options::{FindOneOptions};
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
        let database = client.database(db_name.unwrap_or("ronin"));
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
        self.transactions.find_one_and_delete(doc! {
            "block": {
                "$gt": last_block as i64
            }
        }, None).await
    }
}