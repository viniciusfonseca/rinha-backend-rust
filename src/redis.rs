pub async fn get_redis(
    redis_pool: &deadpool_redis::Pool,
    key: &str,
) -> deadpool_redis::redis::RedisResult<String> {
    let mut redis_conn = redis_pool.get().await.unwrap();
    return deadpool_redis::redis::cmd("GET")
        .arg(&[key])
        .query_async::<_, String>(&mut redis_conn)
        .await;
}

pub async fn set_redis(
    redis_pool: &deadpool_redis::Pool,
    key: &str,
    value: &str,
) -> deadpool_redis::redis::RedisResult<()> {
    let mut redis_conn = redis_pool.get().await.unwrap();
    return deadpool_redis::redis::cmd("SET")
        .arg(&[key, value])
        .query_async::<_, ()>(&mut redis_conn)
        .await;
}
