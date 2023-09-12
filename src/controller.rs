use crate::db::*;
use actix_web::{web, HttpResponse};
use chrono::NaiveDate;
use deadpool_postgres::{GenericClient, Pool};
use std::{sync::Arc, time::Duration};

pub type APIResult = Result<HttpResponse, Box<dyn std::error::Error>>;

#[actix_web::post("/pessoas")]
pub async fn criar_pessoa(
    redis_pool: web::Data<deadpool_redis::Pool>,
    payload: web::Json<CriarPessoaDTO>,
    queue: web::Data<Arc<AppQueue>>,
) -> APIResult {
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
    let redis_key = format!("a/{}", payload.apelido.clone());
    let mut redis_conn = redis_pool.get().await?;
    match deadpool_redis::redis::cmd("GET")
        .arg(&[redis_key.clone()])
        .query_async::<_, String>(&mut redis_conn)
        .await
    {
        Ok(_) => return Ok(HttpResponse::UnprocessableEntity().finish()),
        Err(_) => (),
    };
    let id = uuid::Uuid::new_v4().to_string();
    let stack = match &payload.stack {
        Some(v) => Some(v.join(" ")),
        None => None,
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
        stack: stack_vec,
    };
    let body = serde_json::to_string(&dto)?;
    deadpool_redis::redis::cmd("MSET")
        .arg(&[id.clone(), body.clone(), redis_key.clone(), "0".into()])
        .query_async::<_, ()>(&mut redis_conn)
        .await?;
    // tokio::spawn(async move {
    //     let conn = pool.get().await.expect("error getting conn");
    //     let _ = conn.execute("
    //         INSERT INTO PESSOAS (ID, APELIDO, NOME, NASCIMENTO, STACK)
    //         VALUES ($1, $2, $3, $4, $5)
    //         ON CONFLICT DO NOTHING;
    //     ", &[&dto.id, &dto.apelido, &dto.nome, &dto.nascimento, &stack]).await;
    // });
    queue.push((id.clone(), payload, stack));

    Ok(HttpResponse::Created()
        .append_header(("Location", format!("/pessoas/{id}")))
        .finish())
}

#[actix_web::get("/pessoas/{id}")]
pub async fn consultar_pessoa(
    id: web::Path<String>,
    pool: web::Data<Pool>,
    redis_pool: web::Data<deadpool_redis::Pool>,
) -> APIResult {
    let id = id.to_string();
    {
        let mut redis_conn = redis_pool.get().await?;
        match deadpool_redis::redis::cmd("GET")
            .arg(&[id.clone()])
            .query_async::<_, String>(&mut redis_conn)
            .await
        {
            Err(_) => (),
            Ok(bytes) => return Ok(HttpResponse::Ok().body(bytes)),
        };
    }
    let dto = {
        let conn = pool.get().await?;
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
            return Ok(HttpResponse::NotFound().finish());
        }
        PessoaDTO::from(&rows[0])
    };
    let body = serde_json::to_string(&dto)?;
    let body_async = body.clone();
    // tokio::spawn(async move
    {
        let mut redis_conn = redis_pool.get().await.expect("error getting redis conn");
        let _ = deadpool_redis::redis::cmd("SET")
            .arg(&[id.clone(), body_async])
            .query_async::<_, ()>(&mut redis_conn)
            .await;
    }
    // );
    Ok(HttpResponse::Ok().body(body))
}

#[actix_web::get("/pessoas")]
pub async fn buscar_pessoas(
    parametros: web::Query<ParametrosBusca>,
    pool: web::Data<Pool>,
) -> APIResult {
    let t = format!("%{}%", parametros.t.to_lowercase());
    let result = {
        let conn = pool.get().await?;
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
        rows.iter()
            .map(|row| PessoaDTO::from(row))
            .collect::<Vec<PessoaDTO>>()
    };
    let body = serde_json::to_string(&result)?;
    Ok(HttpResponse::Ok().body(body))
}

#[actix_web::get("/contagem-pessoas")]
pub async fn contar_pessoas(pool: web::Data<Pool>) -> APIResult {
    tokio::time::sleep(Duration::from_secs(3)).await;
    let conn = pool.get().await?;
    // filtering out warmup elements, just in case they weren't deleted by Tokio
    let rows = &conn.query("SELECT COUNT(1) FROM PESSOAS WHERE APELIDO NOT ILIKE 'WARMUP%';", &[]).await?;
    let count: i64 = rows[0].get(0);
    Ok(HttpResponse::Ok().body(count.to_string()))
}
