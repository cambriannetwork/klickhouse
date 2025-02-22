use futures_util::StreamExt;
use klickhouse::connection::TcpConnection;
use klickhouse::{connection::connect, ClientOptions};
use klickhouse::{DateTime, Row};

async fn get_connection() -> TcpConnection {
    connect("localhost:9000", ClientOptions{
        username: "default".to_string(),
        password: "default".to_string(),
        default_database: "dev_mainnet".to_string(),
        tcp_nodelay: true,
    }).await.unwrap()
}

/// Drop the table if it exists, and create it with the given structure.
/// Make sure to use distinct table names across tests to avoid conflicts between tests executing
/// simultaneously.
pub async fn prepare_table(table_name: &str, table_struct: &str, orderby:&str, connection: &mut TcpConnection) {
    connection
        .execute(format!("DROP TABLE IF EXISTS {}", table_name))
        .await
        .unwrap();
    connection
        .execute(format!(
            "CREATE TABLE {} ({}) ENGINE = MergeTree ORDER BY ({});",
            table_name, table_struct, orderby
        ))
        .await
        .unwrap();
}



#[derive(Row, Debug, Default, PartialEq, Clone)]
pub struct TestType {
    s: u64,
    n: String,
}


const SQL:&str = "SELECT t.generate_series as s,'keeper' as n FROM generate_series(1, 39332) as t";

#[tokio::test]
async fn test_select_blocks() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    let mut conn = get_connection().await;

    let stream = conn.query_raw(SQL).await.unwrap();

    futures_util::pin_mut!(stream); // Can not get rid of this due to !Unpin on TcpStream

    let mut count = 0;

    while let Some(block) = stream.next().await { 
        let block = block.unwrap();
        count += block.rows;
    }

    assert_eq!(count, 39332);

}

#[tokio::test]
async fn test_select_rows() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    let mut conn = get_connection().await;

    let stream = conn.query::<TestType>(SQL).await.unwrap();

    futures_util::pin_mut!(stream); // Can not get rid of this due to !Unpin on TcpStream

    let count = stream.count().await;

    assert_eq!(count, 39332);
}


#[tokio::test]
async fn test_insert_rows() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    let mut conn = get_connection().await;

    prepare_table(
        "test_insert_rows",
        r"
        s UInt64,
        n String",
        "s",
        &mut conn,
    )
    .await;

    let data:Vec<TestType> = (0..1000000).map(|i| TestType { s: i, n: "keeper".to_string() }).collect();

    conn.insert_vec("INSERT INTO test_insert_rows FORMAT NATIVE", data).await.unwrap();

}

#[derive(Row, Debug, Default, PartialEq, Clone)]
pub struct TestType2 {
    s: u64,
    d: klickhouse::DateTime,
    n: String,
}



#[tokio::test]
async fn test_multi_insert() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
  
    let mut conn = get_connection().await;

    prepare_table(
        "test_insert_rows",
        r"
        s UInt64,
        n String",
        "s",
        &mut conn,
    )
    .await;

    prepare_table(
        "test_insert_rows2",
        r"
        s UInt64,
        d DateTime,
        n String",
        "s",
        &mut conn,
    )
    .await;


    let data:Vec<TestType> = (0..1000000).map(|i| TestType { s: i, n: "keeper".to_string() }).collect();

    conn.insert_vec("INSERT INTO test_insert_rows FORMAT Native", data).await.unwrap();


    let data:Vec<TestType2> = (0..1000000).map(|i| TestType2 { s: i, d: DateTime::try_from(chrono::Utc::now()).unwrap(), n: "keeper".to_string() }).collect();

    conn.insert_vec("INSERT INTO test_insert_rows2 FORMAT Native", data).await.unwrap();

}
