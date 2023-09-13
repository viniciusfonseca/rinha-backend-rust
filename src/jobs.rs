use crate::db::*;
use deadpool_postgres::Pool;
use std::{sync::Arc, time::Duration};

pub async fn db_clean_warmup(pool_async_async: Pool) {
    tokio::time::sleep(Duration::from_secs(5)).await;
    pool_async_async
        .get()
        .await
        .unwrap()
        .execute("DELETE FROM PESSOAS WHERE APELIDO LIKE 'WARMUP%';", &[])
        .await
        .unwrap();
}

pub async fn db_warmup() {
    tokio::time::sleep(Duration::from_secs(3)).await;
    let http_client = reqwest::Client::new();
    let nginx_url = "http://localhost:9999/pessoas";
    let mount_body = |n: u16| {
        format!("{{\"apelido\":\"WARMUP::vaf{n}\",\"nascimento\":\"1999-01-01\",\"nome\":\"VAF\"}}")
    };
    let mut f = vec![];
    let v = vec![0, 1, 2, 1, 0];
    for i in 0..511 {
        for j in &v {
            f.push(
                http_client
                    .post(nginx_url)
                    .body(mount_body(j + i))
                    .header("Content-Type", "application/json")
                    .send(),
            );
        }
    }
    futures::future::join_all(f).await;
}

pub async fn db_flush_queue(pool_async: Pool, queue_async: Arc<AppQueue>) {
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let queue = queue_async.clone();
        if queue.len() == 0 {
            continue;
        }
        batch_insert(pool_async.clone(), queue).await;
    }
}
