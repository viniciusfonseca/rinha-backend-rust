use std::{time::Duration, sync::Arc, collections::HashSet};
use actix_web::{HttpServer, App, web, http::KeepAlive, HttpResponse};
use chrono::NaiveDate;
use deadpool_postgres::{Config, Runtime, PoolConfig, Pool, GenericClient, Timeouts};
use deadpool_redis::{ConnectionInfo, RedisConnectionInfo, ConnectionAddr};
use tokio_postgres::{NoTls, Row};
use serde::{Deserialize, Serialize};
use sql_builder::{SqlBuilder, quote};

#[derive(Deserialize)]
struct CriarPessoaDTO {
    apelido: String,
    nome: String,
    nascimento: String,
    stack: Option<Vec<String>>
}

#[derive(Deserialize, Serialize)]
struct PessoaDTO {
    id: String,
    apelido: String,
    nome: String,
    nascimento: String,
    stack: Option<Vec<String>>
}

impl PessoaDTO {
    fn from(row: &Row) -> PessoaDTO {
        // COLUMNS: ID, APELIDO, NOME, NASCIMENTO, STACK
        let stack: Option<String> = row.get(4);
        let stack = match stack {
            None => None,
            Some(s) => Some(s.split(' ').map(|s| s.to_string()).collect())
        };
        PessoaDTO {
            id: row.get(0),
            apelido: row.get(1),
            nome: row.get(2),
            nascimento: row.get(3),
            stack
        }
    }
}

type APIResult = Result<HttpResponse, Box<dyn std::error::Error>>;
type AsyncVoidResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

type QueueEvent = (String, web::Json<CriarPessoaDTO>, Option<String>);
type AppQueue = deadqueue::unlimited::Queue::<QueueEvent>;

#[actix_web::post("/pessoas")]
async fn criar_pessoa(
    redis_pool: web::Data<deadpool_redis::Pool>,
    payload: web::Json<CriarPessoaDTO>,
    queue: web::Data<Arc<AppQueue>>
) -> APIResult {
    let redis_key = format!("a/{}", payload.apelido.clone());
    let mut redis_conn = redis_pool.get().await?;
    match deadpool_redis::redis::cmd("GET").arg(&[redis_key.clone()]).query_async::<_, String>(&mut redis_conn).await {
        Ok(_) => return Ok(HttpResponse::UnprocessableEntity().finish()),
        Err(_) => ()    
    };
    if payload.nome.len() > 100 {
        return Ok(HttpResponse::BadRequest().finish());
    }
    if payload.apelido.len() > 32 {
        return Ok(HttpResponse::BadRequest().finish());
    }
    if NaiveDate::parse_from_str(&payload.nascimento, "%Y-%m-%d").is_err() {
        return Ok(HttpResponse::BadRequest().finish());
    }
    if let Some(stack) = &payload.stack {
        for element in stack.clone() {
            if element.len() > 32 {
                return Ok(HttpResponse::BadRequest().finish());
            }
        }
    }
    let _ = deadpool_redis::redis::cmd("SET").arg(&[redis_key.clone(), "0".into()]).query_async::<_, ()>(&mut redis_conn).await;
    let id = uuid::Uuid::new_v4().to_string();
    let stack = match &payload.stack {
        Some(v) => Some(v.join(" ")),
        None => None
    };
    let apelido = payload.apelido.clone();
    let nome = payload.nome.clone();
    let nascimento = payload.nascimento.clone();
    let stack_vec = payload.stack.clone();
    let dto = PessoaDTO {
        id: id.clone(),
        apelido,
        nome,
        nascimento,
        stack: stack_vec
    };
    let body = serde_json::to_string(&dto)?;
    deadpool_redis::redis::cmd("SET")
        .arg(&[id.clone(), body.clone()]).query_async::<_, ()>(&mut redis_conn)
        .await?;
    queue.push((id.clone(), payload, stack));
    // tokio::spawn(store_row(pool, id.clone(), payload, stack));
    
    Ok(
        HttpResponse::Created()
            .append_header(("Location", format!("/pessoas/{id}")))
            .finish()
    )
}

#[actix_web::get("/pessoas/{id}")]
async fn consultar_pessoa(id: web::Path<String>, pool: web::Data<Pool>, redis_pool: web::Data<deadpool_redis::Pool>) -> APIResult {
    let id = id.to_string();
    {
        let mut redis_conn = redis_pool.get().await?;
        match deadpool_redis::redis::cmd("GET").arg(&[id.clone()]).query_async::<_, String>(&mut redis_conn).await {
            Err(_) => (),
            Ok(bytes) => return Ok(HttpResponse::Ok().body(bytes))
        };
    }
    let dto = {
        let conn = pool.get().await?;
        let rows = conn.query("SELECT ID, APELIDO, NOME, NASCIMENTO, STACK FROM PESSOAS P WHERE P.ID = $1;", &[&id]).await?;
        if rows.len() == 0 {
            return Ok(HttpResponse::NotFound().finish());
        }
        let row = &rows[0];
        PessoaDTO::from(row)
    };
    let body = serde_json::to_string(&dto)?;
    let body_async = body.clone();
    tokio::spawn(async move {
        let mut redis_conn = redis_pool.get().await.expect("error getting redis conn");
        let _ = deadpool_redis::redis::cmd("SET")
            .arg(&[id.clone(), body_async]).query_async::<_, ()>(&mut redis_conn)
            .await;
    });
    Ok(HttpResponse::Ok().body(body))
}

#[derive(Deserialize)]
struct ParametrosBusca {
    t: String
}

#[actix_web::get("/pessoas")]
async fn buscar_pessoas(parametros: web::Query<ParametrosBusca>, pool: web::Data<Pool>) -> APIResult {

    // let t = format!("{}:*", parametros.t.replace(' ', "*"));
    let t = format!("%{}%", parametros.t.to_lowercase().replace(' ', "*"));
    let result = {
        let conn = pool.get().await?;
        let rows = conn.query(
            // "SELECT ID, APELIDO, NOME, NASCIMENTO, STACK FROM PESSOAS P WHERE TO_TSQUERY('BUSCA', $1) @@ BUSCA LIMIT 50;", &[&t]
            "SELECT ID, APELIDO, NOME, NASCIMENTO, STACK FROM PESSOAS P WHERE P.BUSCA_TRGM LIKE $1 LIMIT 50;", &[&t]
        ).await?;
        rows.iter().map(|row| PessoaDTO::from(row)).collect::<Vec<PessoaDTO>>()
    };
    let body = serde_json::to_string(&result)?;
    Ok(HttpResponse::Ok().body(body))
}

#[actix_web::get("/contagem-pessoas")]
async fn contar_pessoas(pool: web::Data<Pool>) -> APIResult {
    tokio::time::sleep(Duration::from_secs(2)).await;
    let conn = pool.get().await?;
    let rows = &conn.query("SELECT COUNT(1) FROM PESSOAS;", &[]).await?;
    let count: i64 = rows[0].get(0);
    Ok(HttpResponse::Ok().body(count.to_string()))
}

async fn batch_insert(pool: Pool, queue: Arc<AppQueue>) {
    let mut sql_builder = SqlBuilder::insert_into("PESSOAS");
    sql_builder
        .field("ID")
        .field("APELIDO")
        .field("NOME")
        .field("NASCIMENTO")
        .field("STACK");
    let mut apelidos = HashSet::<String>::new();
    while queue.len() > 0 {
        let (id, payload, stack) = queue.pop().await;
        if apelidos.contains(&payload.apelido) { continue }
        apelidos.insert(payload.apelido.clone());
        sql_builder.values(&[
            &quote(id),
            &quote(&payload.apelido),
            &quote(&payload.nome),
            &quote(&payload.nascimento),
            &quote(stack.unwrap_or("".into()))
        ]);
    }
    {
        let mut conn = match pool.get().await {
            Ok(x) => x,
            Err(_) => return
        };
        let mut sql = match sql_builder.sql() {
            Ok(x) => x,
            Err(_) => return
        };
        sql.pop();
        sql.push_str("ON CONFLICT DO NOTHING;");
        let transaction = match conn.transaction().await {
            Ok(x) => x,
            Err(_) => return
        };
        match transaction.batch_execute(&sql).await {
            Ok(_) => (),
            Err(_) => return
        };
        let _ = transaction.commit().await;
    }
}

#[tokio::main]
async fn main() -> AsyncVoidResult {
    
    let mut cfg = Config::new();
    cfg.host = Some("db".to_string());
    cfg.dbname = Some("rinhadb".to_string());
    cfg.user = Some("root".to_string());
    cfg.password = Some("1234".to_string());
    let pc = PoolConfig::new(30);
    cfg.pool = pc.into();
    println!("creating postgres pool...");
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
    println!("postgres pool succesfully created");

    let mut cfg = deadpool_redis::Config::default();
    cfg.connection = Some(ConnectionInfo {
        addr: ConnectionAddr::Tcp("172.17.0.1".into(), 6379),
        redis: RedisConnectionInfo {
            db: 0,
            username: None,
            password: None
        }
    });
    cfg.pool = Some(PoolConfig {
        max_size: 12,
        timeouts: Timeouts {
            wait: Some(Duration::from_secs(2)),
            create: None,
            recycle: Some(Duration::from_secs(2))
        }
    });
    println!("creating redis pool...");
    let redis_pool = cfg.create_pool(Some(Runtime::Tokio1))?;
    println!("redis pool succesfully created");
    let pool_async = pool.clone();

    let queue = Arc::new(AppQueue::new());
    let queue_async = queue.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        {
            let http_client = reqwest::Client::new();
            let nginx_url = "http://172.17.0.1:9999/pessoas";
            let mount_body = |n: u16| format!("{{\"apelido\":\"WARMUP::vaf{n}\",\"nascimento\":\"1999-01-01\",\"nome\":\"VAF\"}}");
            let mut f = vec![];
            let v = vec![0, 1, 2, 1, 0];
            for i in 0..511 {
                for j in &v {
                    f.push(http_client.post(nginx_url)
                        .body(mount_body(j + i))
                        .header("Content-Type", "application/json")
                        .send());
                }
            }
            futures::future::join_all(f).await;
            let pool_async_async = pool_async.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                pool_async_async.get().await.unwrap().execute("DELETE FROM PESSOAS WHERE APELIDO LIKE 'WARMUP%';", &[]).await.unwrap();
            });
        }
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let queue = queue_async.clone();
            if queue.len() == 0 { continue }
            batch_insert(pool_async.clone(), queue).await;
        }
    });

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::new(redis_pool.clone()))
            .app_data(web::Data::new(queue.clone()))
            .service(criar_pessoa)
            .service(consultar_pessoa)
            .service(buscar_pessoas)
            .service(contar_pessoas)
    })
    .keep_alive(KeepAlive::Os)
    .client_request_timeout(Duration::from_secs(0))
    .bind("0.0.0.0:80")?
    .run()
    .await?;

    Ok(())
}