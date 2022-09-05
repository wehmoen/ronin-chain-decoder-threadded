use mongodb::{Client, Collection};
use mongodb::bson::{DateTime, doc};
use serde::{Deserialize, Serialize};

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
}

impl Database {
    pub async fn new(db_uri: &str, db_name: Option<&str>) -> Database {
        let client = Client::with_uri_str(db_uri).await.unwrap();
        let database = client.database(db_name.unwrap_or("ronin"));
        let transactions = database.collection::<Transaction>("transactions");
        Database {
            transactions,
        }
    }

    pub async fn one_transaction(&self) -> mongodb::error::Result<Option<Transaction>> {
        self.transactions.find_one_and_delete(doc!{}, None).await
    }

    pub async fn estimate_remaining(&self) -> mongodb::error::Result<u64> {
        self.transactions.estimated_document_count(None).await
    }
}