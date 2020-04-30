extern crate chrono;
extern crate futures;
extern crate sqlx;

use chrono::prelude::*;
use chrono::DateTime;
use futures::executor;
use sqlx::MySqlPool;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;

const TS_START: i64 = 1546272000;
const SECS_PER_DAY: i64 = 24 * 60 * 60;

type IdSet = HashSet<u64>;
type ChannleStayMap = HashMap<u32, Stay>;

#[derive(Debug)]
struct Stay {
    reg_uids: Vec<IdSet>,
    log_uids: Vec<IdSet>,
    regd: i32,
}

impl Stay {
    fn new() -> Self {
        Stay {
            reg_uids: vec![IdSet::new(); 31],
            log_uids: vec![IdSet::new(); 31],
            regd: 0,
        }
    }
}

#[derive(Debug)]
struct Raw {
    id: u64,
    uid: u64,
    sid: u32,
    device_id: String,
    new_device: i8,
    act: i8,
    rc: u32,
}

async fn do_statistics(
    day: &String,
    sql: &String,
    min: i64,
    max: i64,
    id_stoday: i64,
    day30ago: &String,
) -> anyhow::Result<()> {
    let pool = MySqlPool::new(sql).await?;
    let raws = sqlx::query_as!(
        Raw,
        "
SELECT id, uid, sid, device_id, new_device, act, rc 
FROM t_statistics_raw 
WHERE id>=? and id<=?
        ",
        min,
        max
    )
    .fetch_all(&pool) // -> Vec<Raw>
    .await?;

    // sid => rc => Stay
    let mut all_stay_map = HashMap::<u32, ChannleStayMap>::new();
    println!("raws size: {}", raws.len());

    for i in 0..30 {
        let start_day = id_stoday - (i * SECS_PER_DAY << 32);
        let end_day = start_day + (SECS_PER_DAY << 32) + std::u32::MAX as i64;
        for raw in &raws {
            let id = raw.id as i64;
            if id >= start_day && id <= end_day {
                //println!("{} {:?}", i, raw);
                if !all_stay_map.contains_key(&raw.sid) {
                    all_stay_map.insert(raw.sid, ChannleStayMap::new());
                }
                let rc_stay_map = all_stay_map.get_mut(&raw.sid).unwrap();
                if !rc_stay_map.contains_key(&raw.rc) {
                    rc_stay_map.insert(raw.rc, Stay::new());
                }
                let stay = rc_stay_map.get_mut(&raw.rc).unwrap();
                stay.log_uids.get_mut(i as usize).unwrap().insert(raw.uid);
                if raw.act == 1 {
                    stay.reg_uids.get_mut(i as usize).unwrap().insert(raw.uid);
                    if raw.new_device == 1 {
                        stay.regd += 1;
                    }
                }
            }
        }
    }
    //println!("rc_stay_map: {:?}", rc_stay_map);
    //1-30天前注册留存
    for (sid, rc_stay_map) in &all_stay_map {
        for (rc, stay) in rc_stay_map {
            println!(
                "{} {} {} reg={}, log={}, regd={}",
                day,
                sid,
                rc,
                stay.reg_uids[0].len(),
                stay.log_uids[0].len(),
                stay.regd
            );
            let reg = stay.reg_uids[0].len() as u32;
            let log = stay.log_uids[0].len() as u32;
            sqlx::query("INSERT INTO t_statistics_daily(dt,sid,rc,register,login,reg_device) VALUES(?,?,?,?,?,?)")
            .bind(day).bind(sid).bind(rc).bind(reg).bind(log).bind(stay.regd).execute(&pool).await?;
            let mut stays = vec![0; 31];
            for u in &stay.log_uids[0] {
                for i in 1..30 {
                    if stay.reg_uids[i].contains(u) {
                        stays[i] += 1;
                    }
                }
            }
            println!("{} {} {} stay={:?}", day, sid, rc, stays);
            let mut feilds = String::from("dt,sid,rc");
            let mut values = std::format!("'{}',{},'{}'", day, sid, rc);
            for i in 1..30 {
                feilds.push_str(std::format!(",stay{}", i).as_str());
                values.push_str(std::format!(",{}", stays[i]).as_str());
            }
            let sql = std::format!("INSERT INTO t_stay_daily({}) VALUES({})", feilds, values);
            sqlx::query(&sql).execute(&pool).await?;
        }
    }

    //删1月前原始统计数据.
    sqlx::query("DELETE FROM t_statistics_raw WHERE id<?")
        .bind(min)
        .execute(&pool)
        .await?;
    //删1月前留存数据.
    sqlx::query("DELETE FROM t_stay_daily WHERE dt<?")
        .bind(day30ago)
        .execute(&pool)
        .await?;

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
    let day = format!(
        "{}-{:02}-{:02}",
        end_dt.year(),
        end_dt.month(),
        end_dt.day()
    );
    let day30ago = format!(
        "{}-{:02}-{:02}",
        start_dt.year(),
        start_dt.month(),
        start_dt.day()
    );
    println!("day: {}", day);
    let ts = end_ts - TS_START;
    let id_max = (ts << 32) + std::u32::MAX as i64;
    let id_min = (ts - 31 * SECS_PER_DAY) << 32;
    let id_stoday = (ts - SECS_PER_DAY) << 32;
    let sql = &args[1];
    println!(
        "{} id_min: {}, id_max: {}, id_stoday: {}",
        sql, id_min, id_max, id_stoday
    );
    match executor::block_on(do_statistics(
        &day, sql, id_min, id_max, id_stoday, &day30ago,
    )) {
        Err(e) => println!("{:?}", e),
        _ => (),
    }
}
