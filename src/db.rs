use actix_web::web;
use deadpool_postgres::{GenericClient, Pool};
use serde::{Deserialize, Serialize};
use sql_builder::{quote, SqlBuilder};
use std::time::Duration;
use std::{collections::HashSet, sync::Arc};
use tokio_postgres::Row;

pub type AsyncVoidResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
pub type QueueEvent = (String, web::Json<CriarPessoaDTO>, Option<String>);
pub type AppQueue = deadqueue::unlimited::Queue<QueueEvent>;

#[derive(Deserialize)]
pub struct CriarPessoaDTO {
    pub apelido: String,
    pub nome: String,
    pub nascimento: String,
    pub stack: Option<Vec<String>>,
}

#[derive(Deserialize, Serialize)]
pub struct PessoaDTO {
    pub id: String,
    pub apelido: String,
    pub nome: String,
    pub nascimento: String,
    pub stack: Option<Vec<String>>,
}

impl PessoaDTO {
    pub fn from(row: &Row) -> PessoaDTO {
        // COLUMNS: ID, APELIDO, NOME, NASCIMENTO, STACK
        let stack: Option<String> = row.get(4);
        let stack = match stack {
            None => None,
            Some(s) => Some(s.split(' ').map(|s| s.to_string()).collect()),
        };
        PessoaDTO {
            id: row.get(0),
            apelido: row.get(1),
            nome: row.get(2),
            nascimento: row.get(3),
            stack,
        }
    }
}

#[derive(Deserialize)]
pub struct ParametrosBusca {
    pub t: String,
}

pub async fn batch_insert(pool: Pool, queue: Arc<AppQueue>) {
    let mut apelidos = HashSet::<String>::new();
    let mut sql = String::new();
    while queue.len() > 0 {
        let (id, payload, stack) = queue.pop().await;
        if apelidos.contains(&payload.apelido) {
            continue;
        }
        apelidos.insert(payload.apelido.clone());
        let mut sql_builder = SqlBuilder::insert_into("PESSOAS");
        sql_builder
            .field("ID")
            .field("APELIDO")
            .field("NOME")
            .field("NASCIMENTO")
            .field("STACK");
        sql_builder.values(&[
            &quote(id),
            &quote(&payload.apelido),
            &quote(&payload.nome),
            &quote(&payload.nascimento),
            &quote(stack.unwrap_or("".into())),
        ]);
        let mut this_sql = match sql_builder.sql() {
            Ok(x) => x,
            Err(_) => continue,
        };
        this_sql.pop();
        this_sql.push_str("ON CONFLICT DO NOTHING;");
        sql.push_str(&this_sql.as_str());
    }
    {
        let mut conn = match pool.get().await {
            Ok(x) => x,
            Err(_) => return,
        };
        let transaction = match conn.transaction().await {
            Ok(x) => x,
            Err(_) => return,
        };
        match transaction.batch_execute(&sql).await {
            Ok(_) => (),
            Err(_) => return,
        };
        let _ = transaction.commit().await;
    }
}

pub async fn db_clean_warmup(pool_async_async: Pool) {
    tokio::time::sleep(Duration::from_secs(2)).await;
    pool_async_async
        .get()
        .await
        .unwrap()
        .execute("DELETE FROM PESSOAS WHERE APELIDO LIKE 'WARMUP%';", &[])
        .await
        .unwrap();
}

pub async fn db_warmup(pool_async: Pool) {
    tokio::time::sleep(Duration::from_secs(3)).await;
    {
        let http_client = reqwest::Client::new();
        let nginx_url = "http://localhost:9999/pessoas";
        let mount_body = |n: u16| {
            format!(
                "{{\"apelido\":\"WARMUP::vaf{n}\",\"nascimento\":\"1999-01-01\",\"nome\":\"VAF\"}}"
            )
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
        let pool_async_async = pool_async.clone();
        tokio::spawn(async move { db_clean_warmup(pool_async_async) });
    }
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
