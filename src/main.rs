extern crate chrono;
extern crate futures;
extern crate sqlx;

use chrono::prelude::*;
use futures::executor;
use sqlx::MySqlPool;
use std::env;

#[derive(Debug)]
struct Raw {
    uid: u64,
    sid: u32,
    device_id: String,
    new_device: i8,
    act: i8,
    rc: String,
}

async fn do_statistics(sql: &String, min: i64, max: i64) -> Result<(), sqlx::Error> {
    let pool = MySqlPool::new(sql).await?;
    let raws = sqlx::query_as!(
        Raw,
        "
SELECT uid, sid, device_id, new_device, act, rc 
FROM t_statistics_raw 
WHERE id >= ? and id <= ?
        ",
        min,
        max
    )
    .fetch_all(&pool) // -> Vec<Raw>
    .await?;

    for raw in raws {
        println!("{:?}", raw);
    }
    //1-30天前注册留存

    Ok(())
}

const TS_START: i64 = 1546272000;
const SECS_PER_DAY: i64 = 24 * 60 * 60;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!(
            "{} mysql://<user>[:<password>]@<host>[:<port>]/<database>",
            args[0]
        );
        return;
    }
    let dt = Local::now();
    println!("dt: {}", dt);
    let ts0 = dt.timestamp() - dt.time().num_seconds_from_midnight() as i64;
    println!("ts0: {}, dt: {}", ts0, Utc.timestamp(ts0, 0));
    let ts0 = ts0 - TS_START;
    let max = (ts0 << 32) + std::u32::MAX as i64;
    let min = (ts0 - 30 * SECS_PER_DAY) << 32;
    let sql = &args[1];
    println!("{} min: {}, max: {}", sql, min, max);
    match executor::block_on(do_statistics(sql, min, max)) {
        Err(e) => println!("{:?}", e),
        _ => (),
    }
}
