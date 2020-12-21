use chrono::prelude::*;
use chrono::DateTime;
use futures::executor;
use std::env;
use std::fs;

mod stats;
use stats::{stats_rare, stats_stay, ItemCount};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!(
            "{} mysql://<user>[:<password>]@<host>[:<port>]/<database> rare_items.json [timestamp]",
            args[0]
        );
        return;
    }
    let sql = &args[1];
    let json_str = String::from_utf8(fs::read(&args[2]).expect("can't read rare_items!")).unwrap();
    let rare_items: Vec<serde_json::Value> = serde_json::from_str(json_str.as_str()).unwrap();
    let mut rare = ItemCount::new();
    for ri in &rare_items {
        if let Some(alrm) = ri.get("alrm") {
            let id = ri.get("id").unwrap().as_u64().unwrap() as u32;
            let ct = alrm.as_u64().unwrap();
            rare.insert(id, ct);
        }
    }
    let mut run_ts: i64 = 0;
    if args.len() > 3 {
        run_ts = args[3].parse().unwrap();
        let cur_dt = Local::now();
        let run_dt =
            DateTime::<Local>::from_utc(NaiveDateTime::from_timestamp(run_ts, 0), *cur_dt.offset());
        println!("cur_dt={}, run_dt={}", cur_dt, run_dt);
    }
    // println!("{:?}", rare);
    match executor::block_on(stats_stay(sql, run_ts)) {
        Err(e) => println!("{:?}", e),
        _ => (),
    }
    match executor::block_on(stats_rare(sql, &rare, run_ts)) {
        Err(e) => println!("{:?}", e),
        _ => (),
    }
}
