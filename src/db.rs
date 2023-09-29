use actix_web::web;
use deadpool_postgres::{GenericClient, Pool};
use serde::{Deserialize, Serialize};
use sql_builder::{quote, SqlBuilder};
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

pub async fn db_count(conn: &deadpool_postgres::Client) -> Result<i64, Box<dyn std::error::Error>> {
    let rows = conn
        .query(
            "SELECT COUNT(1) FROM PESSOAS WHERE APELIDO NOT LIKE 'WARMUP%';",
            &[],
        )
        .await?;
    let count: i64 = rows[0].get(0);
    Ok(count)
}

pub async fn db_search(
    conn: &deadpool_postgres::Client,
    t: String,
) -> Result<Vec<PessoaDTO>, Box<dyn std::error::Error>> {
    let stmt = conn
        .prepare_cached(
            "
            SELECT ID, APELIDO, NOME, NASCIMENTO, STACK
            FROM PESSOAS P
            WHERE P.BUSCA_TRGM LIKE $1
            LIMIT 50;
        ",
        )
        .await?;
    let rows = conn.query(&stmt, &[&t]).await?;
    let result = rows
        .iter()
        .map(|row| PessoaDTO::from(row))
        .collect::<Vec<PessoaDTO>>();
    Ok(result)
}

pub async fn db_get_pessoa_dto(
    conn: &deadpool_postgres::Client,
    id: &String,
) -> Result<Option<PessoaDTO>, Box<dyn std::error::Error>> {
    let rows = conn
        .query(
            "
            SELECT ID, APELIDO, NOME, NASCIMENTO, STACK
            FROM PESSOAS P
            WHERE P.ID = $1;
        ",
            &[&id],
        )
        .await?;
    if rows.len() == 0 {
        return Ok(None);
    }
    Ok(Some(PessoaDTO::from(&rows[0])))
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
