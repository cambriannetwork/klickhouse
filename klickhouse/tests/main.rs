pub mod test;
pub mod test_bytes;
pub mod test_decimal;
pub mod test_flatten;
#[cfg(feature = "geo-types")]
pub mod test_geo;
pub mod test_into;
pub mod test_lock;
pub mod test_nested;
pub mod test_ordering;
pub mod test_raw_string;
pub mod test_serialize;
#[cfg(feature = "connection")]
pub mod test_connection;

use klickhouse::{Client, ClientOptions};

pub async fn get_client() -> Client {
    let mut options = ClientOptions::default();

    if let Ok(user) = std::env::var("KLICKHOUSE_TEST_USER") {
        options.username = user;
    }

    if let Ok(password) = std::env::var("KLICKHOUSE_TEST_PASSWORD") {
        options.password = password;
    }
    
    let address = std::env::var("KLICKHOUSE_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".into());

    let client = Client::connect(address, options).await.unwrap();

    let database = std::env::var("KLICKHOUSE_TEST_DATABASE").unwrap_or_else(|_| "klickhouse_test".into());

    client.execute(format!("CREATE DATABASE IF NOT EXISTS {database}"))
        .await
        .unwrap();

    client.execute(format!("USE {database}"))
        .await
        .unwrap();

    client
}
/// Drop the table if it exists, and create it with the given structure.
/// Make sure to use distinct table names across tests to avoid conflicts between tests executing
/// simultaneously.
pub async fn prepare_table(table_name: &str, table_struct: &str, client: &Client) {
    client
        .execute(format!("DROP TABLE IF EXISTS {table_name}"))
        .await
        .unwrap();
    client
        .execute(format!(
            "CREATE TABLE {table_name} ({table_struct}) ENGINE = Memory;"
        ))
        .await
        .unwrap();
}
