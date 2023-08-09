use std::time::Duration;
use actix_web::{HttpServer, App, web, http::KeepAlive, HttpResponse};
use chrono::NaiveDate;
use deadpool_postgres::{Config, Runtime, PoolConfig, Pool};
use tokio_postgres::{NoTls, Row};
use serde::{Deserialize, Serialize};

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
            Some(s) => Some(s.split(',').map(|s| s.to_string()).collect())
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

async fn store_redis(redis_pool: web::Data<deadpool_redis::Pool>, id: String, body: String) -> AsyncVoidResult {
    let mut redis_conn = redis_pool.get().await?;
    deadpool_redis::cmd("SET").arg(&[id.clone(), body.clone()]).execute_async(&mut redis_conn).await?;
    Ok(())
}

#[actix_web::post("/pessoas")]
async fn criar_pessoa(pool: web::Data<Pool>, redis_pool: web::Data<deadpool_redis::Pool>, payload: web::Json<CriarPessoaDTO>) -> APIResult {
    let conn = pool.get().await?;
    let id = uuid::Uuid::new_v4().to_string();
    let stack = match &payload.stack {
        Some(v) => Some(v.join(",")),
        None => None
    };
    if NaiveDate::parse_from_str(&payload.nascimento, "%Y-%m-%d").is_err() {
        return Ok(HttpResponse::BadRequest().body("{\"error\": \"DATA INVALIDA\"}"))
    }
    if conn.execute("INSERT INTO PESSOAS (ID, APELIDO, NOME, NASCIMENTO, STACK) VALUES ($1, $2, $3, $4, $5);", &[
        &id, &payload.apelido, &payload.nome, &payload.nascimento, &stack
    ]).await.is_err() {
        return Ok(HttpResponse::UnprocessableEntity().finish())
    };
    let dto = PessoaDTO {
        id: id.clone(),
        apelido: payload.apelido.clone(),
        nome: payload.nome.clone(),
        nascimento: payload.nascimento.clone(),
        stack: payload.stack.clone()
    };
    let body = serde_json::to_string(&dto)?;
    tokio::spawn(store_redis(redis_pool, id.clone(), body));
    Ok(
        HttpResponse::Created()
            .append_header(("Location", format!("/pessoas/{id}")))
            .finish()
    )
}

#[actix_web::get("/pessoas/{id}")]
async fn consultar_pessoa(id: web::Path<String>, pool: web::Data<Pool>, redis_pool: web::Data<deadpool_redis::Pool>) -> APIResult {
    let id = id.to_string();
    let mut redis_conn = redis_pool.get().await?;
    match deadpool_redis::cmd("GET").arg(&[id.clone()]).query_async::<String>(&mut redis_conn).await {
        Err(_) => (),
        Ok(bytes) => return Ok(HttpResponse::Ok().body(bytes))
    };
    let conn = pool.get().await?;
    let rows = conn.query("SELECT ID, APELIDO, NOME, NASCIMENTO, STACK FROM PESSOAS P WHERE P.ID = $1;", &[&id]).await?;
    if rows.len() == 0 {
        return Ok(HttpResponse::NotFound().finish());
    }
    let row = &rows[0];
    let dto = PessoaDTO::from(row);
    let body = serde_json::to_string(&dto)?;
    tokio::spawn(store_redis(redis_pool, id.clone(), body.clone()));
    Ok(HttpResponse::Ok().body(body))
}

#[derive(Deserialize)]
struct ParametrosBusca {
    t: String
}

#[actix_web::get("/pessoas")]
async fn buscar_pessoas(parametros: web::Query<ParametrosBusca>, pool: web::Data<Pool>) -> APIResult {

    let conn = pool.get().await?;
    let t = parametros.t.to_uppercase();
    let apelido = format!("%{t}%");
    let nome = format!("%{t}%");
    let stack = format!("%,{t},%");
    let rows = conn.query(
        "SELECT ID, APELIDO, NOME, NASCIMENTO, STACK FROM PESSOAS P WHERE UPPER(P.APELIDO) LIKE $1 OR UPPER(P.NOME) LIKE $2 OR UPPER(',' || P.STACK || ',') LIKE $3 LIMIT 50;", &[
            &apelido, &nome, &stack
        ]).await?;
    let result = rows.iter().map(|row| PessoaDTO::from(row)).collect::<Vec<PessoaDTO>>();
    let body = serde_json::to_string(&result)?;
    Ok(HttpResponse::Ok().body(body))
}

#[actix_web::get("/contagem-pessoas")]
async fn contar_pessoas(pool: web::Data<Pool>) -> APIResult {
    let conn = pool.get().await?;
    let rows = &conn.query("SELECT COUNT(*) FROM PESSOAS;", &[]).await?;
    let count: i64 = rows[0].get(0);
    Ok(HttpResponse::Ok().body(count.to_string()))
}

#[tokio::main]
async fn main() -> AsyncVoidResult {
    
    let mut cfg = Config::new();
    cfg.host = Some("db".to_string());
    cfg.dbname = Some("rinhadb".to_string());
    cfg.user = Some("root".to_string());
    cfg.password = Some("1234".to_string());
    let pc = PoolConfig::new(50);
    cfg.pool = pc.into();
    println!("creating postgres pool...");
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
    println!("postgres pool succesfully created");

    {
        let _ = pool.get().await?.execute(
            "CREATE TABLE IF NOT EXISTS PESSOAS (
                ID CHAR(32) CONSTRAINT ID_PK PRIMARY KEY,
                APELIDO VARCHAR(32),
                NOME VARCHAR(100),
                NASCIMENTO CHAR(10),
                STACK VARCHAR(1024),
                CONSTRAINT APELIDO_UNIQUE UNIQUE (APELIDO)
            );",
        &[]).await;
    }

    let mut cfg = deadpool_redis::Config::default();
    cfg.url = Some("redis://172.17.0.1:6379".into());
    println!("creating redis pool...");
    let redis_pool = cfg.create_pool()?;
    println!("redis pool succesfully created");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::new(redis_pool.clone()))
            .service(criar_pessoa)
            .service(consultar_pessoa)
            .service(buscar_pessoas)
            .service(contar_pessoas)
    })
    .keep_alive(KeepAlive::Os)
    .client_request_timeout(Duration::from_secs(0))
    .backlog(1024)
    .bind("0.0.0.0:80")?
    .run()
    .await?;

    Ok(())
}