use futures_util::StreamExt;
use klickhouse::connection::TcpConnection;
use klickhouse::{connection::connect, ClientOptions};
use klickhouse::{DateTime, Progress, Row, Type, Value};
use tokio::select;
use uuid::Uuid;

async fn get_connection() -> TcpConnection {

    let mut options = ClientOptions::default();

    options.tcp_nodelay = true;

    if let Ok(user) = std::env::var("KLICKHOUSE_TEST_USER") {
        options.username = user;
    }

    if let Ok(password) = std::env::var("KLICKHOUSE_TEST_PASSWORD") {
        options.password = password;
    }

    let address = std::env::var("KLICKHOUSE_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".into());

    let mut connection = connect(address, options).await.unwrap();

    let database = std::env::var("KLICKHOUSE_TEST_DATABASE").unwrap_or_else(|_| "klickhouse_test".into());

    connection.execute(format!("CREATE DATABASE IF NOT EXISTS {database}"))
        .await
        .unwrap();

    connection.execute(format!("USE {database}"))
        .await
        .unwrap();
        
    connection

}

/// Drop the table if it exists, and create it with the given structure.
/// Make sure to use distinct table names across tests to avoid conflicts between tests executing
/// simultaneously.
pub async fn prepare_table(table_name: &str, table_struct: &str, connection: &mut TcpConnection) {
    connection
        .execute(format!("DROP TABLE IF EXISTS {}", table_name))
        .await
        .unwrap();
    connection
        .execute(format!(
            "CREATE TABLE {} ({}) ENGINE = Memory;",
            table_name, table_struct
        ))
        .await
        .unwrap();
}



#[derive(Row, Debug, Default, PartialEq, Clone)]
pub struct TestType {
    s: u64,
    n: String,
}

#[derive(Row, Debug, Default, PartialEq, Clone)]
pub struct TestType2 {
    s: u64,
    d: klickhouse::DateTime,
    n: String,
}

const SQL:&str = "SELECT t.generate_series as s,'keeper' as n FROM generate_series(1, 39332) as t";

/// Test that the connection can be established and the ping-pong works.
#[tokio::test]
async fn test_ping_pong() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    let mut conn = get_connection().await;

    assert!(conn.ping_pong().await.is_ok());

}


/// Test that we can recieve raw blocks from the server
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

/// Test that we can recieve rows (typed values) from the server
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


/// Test that we can insert rows into the server and execute a simple aggeragtion query
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
        &mut conn,
    )
    .await;

    let data:Vec<TestType> = (0..1000000).map(|i| TestType { s: i, n: "keeper".to_string() }).collect();

    conn.insert_vec("INSERT INTO test_insert_rows FORMAT NATIVE", data).await.unwrap();


    let (name,ty,val) = conn.execute("SELECT count(*) as c FROM test_insert_rows").await.unwrap().unwrap();

    assert_eq!(name, "c");
    assert_eq!(ty, Type::UInt64);
    assert_eq!(val, Value::UInt64(1000000));

}


/// Test that we can sequantially insert rows into different tables on same connection.
#[tokio::test]
async fn test_multi_insert() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
  
    let mut conn = get_connection().await;

    prepare_table(
        "test_multi_insert_rows",
        r"
        s UInt64,
        n String",
        &mut conn,
    )
    .await;

    prepare_table(
        "test_multi_insert_rows_2",
        r"
        s UInt64,
        d DateTime,
        n String",
        &mut conn,
    )
    .await;


    let data:Vec<TestType> = (0..1000000).map(|i| TestType { s: i, n: "keeper".to_string() }).collect();

    conn.insert_vec("INSERT INTO test_multi_insert_rows FORMAT Native", data).await.unwrap();


    let data:Vec<TestType2> = (0..1000000).map(|i| TestType2 { s: i, d: DateTime::try_from(chrono::Utc::now()).unwrap(), n: "keeper".to_string() }).collect();

    conn.insert_vec("INSERT INTO test_multi_insert_rows_2 FORMAT Native", data).await.unwrap();

}

#[tokio::test]
async fn test_progress() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    let mut conn = get_connection().await;

    prepare_table(
        "test_progress",
        r"
        s UInt64,
        n String",
        &mut conn,
    )
    .await;

    let rs = conn.subscribe_progress();

    select!(
        () = async {
            let data:Vec<TestType> = (0..1000000).map(|i| TestType { s: i, n: "keeper".to_string() }).collect();
            conn.insert_vec("INSERT INTO test_progress FORMAT Native", data).await.unwrap();
        
        } => {},
        () = async {
            let mut rs = rs;

            let mut progress_total = Progress::default();

            while let Ok(progress) = rs.recv().await {
                progress_total += progress;
                println!(
                    "Progress: {}/{} {:.2}%",
                    progress_total.read_rows,
                    progress_total.new_total_rows_to_read,
                    100.0 * progress_total.read_rows as f64
                        / progress_total.new_total_rows_to_read as f64
                );
            }
        } => {},
    )

}

/// Test that we can execute a query with a query ID and check if it is logged in the system.query_log
/// This test is useful to ensure that the query ID functionality works as expected.
#[tokio::test]
async fn test_query_id() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    
        // Execution

        let mut conn = get_connection().await;

        let id = Uuid::new_v4().to_string();

        // Enable query logging
        conn.execute("SET log_queries=1").await.unwrap();

        let _ = conn.execute((&id,SQL)).await.unwrap();

        // Wait for the query to be logged
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        let query = format!("SELECT 1 FROM system.query_log WHERE initial_query_id = '{}'", id);
    
        let log = conn.execute(query).await.unwrap();

        assert!(log.is_some(), "Query ID not found in system.query_log");
    
}
