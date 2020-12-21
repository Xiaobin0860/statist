use chrono::prelude::*;
use chrono::DateTime;
use sqlx::prelude::MySqlQueryAs;
use sqlx::MySqlPool;
use std::collections::HashMap;

const SECS_PER_DAY: i64 = 24 * 60 * 60;

#[derive(Debug, sqlx::FromRow)]
struct RareLog {
    uid: u64,
    cid: String,
    nick: String,
    pos: u32,
    item: u32,
    gap: i32,
}

pub type ItemCount = HashMap<u32, u64>;
type UserItem = HashMap<u64, ItemCount>;
type UserLog<'a> = HashMap<u64, &'a RareLog>;

pub async fn do_statistics(sql: &String, rare: &ItemCount, run_ts: i64) -> anyhow::Result<()> {
    let cur_dt = Local::now();
    let local_offset = *cur_dt.offset();
    //统计日24点
    let end_ts;
    if run_ts > 0 {
        let run_dt =
            DateTime::<Local>::from_utc(NaiveDateTime::from_timestamp(run_ts, 0), local_offset);
        end_ts = run_dt.timestamp() - run_dt.time().num_seconds_from_midnight() as i64;
    } else {
        println!("cur_dt: {}", cur_dt);
        end_ts = cur_dt.timestamp() - cur_dt.time().num_seconds_from_midnight() as i64;
    }
    let end_dt =
        DateTime::<Local>::from_utc(NaiveDateTime::from_timestamp(end_ts - 1, 0), local_offset);
    let start_ts = end_ts - 1 * SECS_PER_DAY;
    let ref day = format!(
        "{}-{:02}-{:02}",
        end_dt.year(),
        end_dt.month(),
        end_dt.day()
    );
    println!("report day: {}", day);

    let pool = MySqlPool::new(sql).await?;
    let tbls = sqlx::query!("SHOW TABLES").fetch_all(&pool).await?;
    let mut rare_tbls: Vec<String> = Vec::new();
    for tbl in tbls {
        let t = tbl.Tables_in_logs;
        if t.starts_with("t_rare_item_log") {
            rare_tbls.push(t);
        }
    }
    // println!("{:?}", rare_tbls);
    for tbl in &rare_tbls {
        //遍历每个区的稀有道具表，检查日产出上限
        let logs = sqlx::query_as::<_, RareLog>(&format!(
            "SELECT uid, cid, nick, position as pos, item, gap from {} WHERE ts>=? and ts<=? and gap>0",
            tbl
        ))
        .bind(start_ts)
        .bind(end_ts)
        .fetch_all(&pool)
        .await?;
        println!("{} count={}", tbl, logs.len());
        //uid => [{item_id, item_ct}]
        let mut user_item = UserItem::new();
        //uid => RareLog(user info)
        let mut ulogs = UserLog::new();
        for log in &logs {
            if rare.get(&log.item).is_none() {
                continue;
            }
            if ulogs.get(&log.uid).is_none() {
                ulogs.insert(log.uid, log);
            }
            if let Some(item_count) = user_item.get_mut(&log.uid) {
                if let Some(ct) = item_count.get_mut(&log.item) {
                    *ct += log.gap as u64;
                } else {
                    item_count.insert(log.item, log.gap as u64);
                }
            } else {
                let mut item_count = ItemCount::new();
                item_count.insert(log.item, log.gap as u64);
                user_item.insert(log.uid, item_count);
            }
        }
        // println!("user_item={:?}", user_item);
        for (uid, ic) in &user_item {
            for (item_id, item_ct) in ic {
                if item_ct > &rare[item_id] {
                    println!("alarm {} {}:{}", uid, item_id, item_ct);
                    let u = ulogs[uid];
                    sqlx::query!("INSERT INTO t_alarm_user(dt,uid,item_id,cid,nick,item_ct) VALUES(?,?,?,?,?,?)", day, uid, item_id, u.cid, u.nick, item_ct)
                        .execute(&pool)
                        .await?;
                }
            }
        }
    }

    Ok(())
}
