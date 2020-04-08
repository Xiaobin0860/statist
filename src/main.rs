extern crate chrono;
extern crate futures;
extern crate sqlx;

use chrono::prelude::*;
use chrono::DateTime;
use futures::executor;
use sqlx::MySqlPool;
use std::collections::HashSet;
use std::env;

const TS_START: i64 = 1546272000;
const SECS_PER_DAY: i64 = 24 * 60 * 60;

type IdSet = HashSet<u64>;

#[derive(Debug)]
struct Raw {
    id: u64,
    uid: u64,
    sid: u32,
    device_id: String,
    new_device: i8,
    act: i8,
    rc: String,
}

async fn do_statistics(sql: &String, min: i64, max: i64, id_stoday: i64) -> anyhow::Result<()> {
    let pool = MySqlPool::new(sql).await?;
    let raws = sqlx::query_as!(
        Raw,
        "
SELECT id, uid, sid, device_id, new_device, act, rc 
FROM t_statistics_raw 
WHERE id >= ? and id <= ? ORDER BY id DESC;
        ",
        min,
        max
    )
    .fetch_all(&pool) // -> Vec<Raw>
    .await?;

    let mut day_reg_uids = Vec::<IdSet>::with_capacity(31);
    let mut day_log_uids = Vec::<IdSet>::with_capacity(31);
    for _ in 0..30 {
        day_reg_uids.push(IdSet::new());
        day_log_uids.push(IdSet::new());
    }

    println!("raws size: {}", raws.len());
    for i in 0..30 {
        let start_day = id_stoday - (i * SECS_PER_DAY << 32);
        let end_day = start_day + (SECS_PER_DAY << 32) + std::u32::MAX as i64;
        for raw in &raws {
            let id = raw.id as i64;
            if id >= start_day && id <= end_day {
                println!("{} {:?}", i, raw);
                day_log_uids.get_mut(i as usize).unwrap().insert(raw.uid);
                if raw.act == 1 {
                    day_reg_uids.get_mut(i as usize).unwrap().insert(raw.uid);
                }
            }
        }
    }
    println!("day_reg_uids: {:?}", day_reg_uids);
    println!("day_log_uids: {:?}", day_log_uids);
    //1-30天前注册留存

    Ok(())
}

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
    let local_offset = *dt.offset();
    println!("dt: {}", dt);
    //统计日24点
    let end_ts = dt.timestamp() - dt.time().num_seconds_from_midnight() as i64;
    let end_dt =
        DateTime::<Local>::from_utc(NaiveDateTime::from_timestamp(end_ts - 1, 0), local_offset);
    let start_ts = end_ts - 31 * SECS_PER_DAY;
    let start_dt =
        DateTime::<Local>::from_utc(NaiveDateTime::from_timestamp(start_ts, 0), local_offset);
    let stoday_ts = end_ts - SECS_PER_DAY;
    let stoday_dt =
        DateTime::<Local>::from_utc(NaiveDateTime::from_timestamp(stoday_ts, 0), local_offset);
    println!("start_ts: {}, start_dt: {}", start_ts, start_dt);
    println!("end_ts: {}, end_dt: {}", end_ts, end_dt);
    println!("stoday_ts: {}, stoday_dt: {}", stoday_ts, stoday_dt);
    let ts = end_ts - TS_START;
    let id_max = (ts << 32) + std::u32::MAX as i64;
    let id_min = (ts - 31 * SECS_PER_DAY) << 32;
    let id_stoday = (ts - SECS_PER_DAY) << 32;
    let sql = &args[1];
    println!(
        "{} id_min: {}, id_max: {}, id_stoday: {}",
        sql, id_min, id_max, id_stoday
    );
    match executor::block_on(do_statistics(sql, id_min, id_max, id_stoday)) {
        Err(e) => println!("{:?}", e),
        _ => (),
    }
}
