use chrono::Utc;
use klickhouse::*;

#[derive(Row, Debug, Default)]
pub struct MyUserData {
    id: Uuid,
    user_data: String,
    created_at: DateTime,
}

#[tokio::main]
async fn main() {
    env_logger::Builder::new()
        .parse_env(env_logger::Env::default().default_filter_or("info"))
        .init();

    // Create a TcpConnectionManager for raw connection pooling
    let manager = ConnectionManager::new("127.0.0.1:9000", ClientOptions::default())
        .await
        .unwrap();

    // Build the connection pool
    let pool = bb8::Pool::builder()
        .max_size(10)
        .build(manager)
        .await
        .unwrap();

    // Get a connection from the pool
    let mut conn = pool.get().await.unwrap();

    // Subscribe to progress events
    let progress_rx = conn.subscribe_progress();
    let progress_task = tokio::task::spawn(async move {
        let mut progress_rx = progress_rx;
        while let Ok(progress) = progress_rx.recv().await {
            println!(
                "Progress: read {} rows, {} bytes read",
                progress.read_rows, progress.read_bytes
            );
        }
    });

    // Prepare table
    conn.execute("DROP TABLE IF EXISTS tcp_pool_example")
        .await
        .unwrap();
    
    conn.execute(
        "CREATE TABLE tcp_pool_example (
            id UUID,
            user_data String,
            created_at DateTime('UTC')
        ) Engine=Memory;"
    )
    .await
    .unwrap();

    // Insert some data
    let rows = (0..5)
        .map(|i| MyUserData {
            id: Uuid::new_v4(),
            user_data: format!("user data {}", i),
            created_at: Utc::now().try_into().unwrap(),
        })
        .collect();

    conn.insert_vec("INSERT INTO tcp_pool_example FORMAT native", rows)
        .await
        .unwrap();

    // Query data back
    let results: Vec<MyUserData> = conn
        .query_vec("SELECT * FROM tcp_pool_example ORDER BY user_data")
        .await
        .unwrap();

    println!("Retrieved {} rows:", results.len());
    for row in results {
        println!("  ID: {}, Data: {}", row.id, row.user_data);
    }

    // Test connection reuse - return connection to pool and get a new one
    drop(conn);
    let mut conn2 = pool.get().await.unwrap();
    
    // Verify we can still query with the new connection
    let count: Vec<UnitValue<u64>> = conn2
        .query_vec("SELECT count(*) FROM tcp_pool_example")
        .await
        .unwrap();
    
    println!("Row count from reused connection: {}", count[0].0);

    // Clean up
    conn2.execute("DROP TABLE tcp_pool_example").await.unwrap();
    drop(conn2);

    // Stop progress task
    progress_task.abort();
    
    println!("TcpConnection pooling example completed successfully!");
}