use futures::executor;
use std::env;
use std::fs;

mod stats;
use stats::{stats_rare, stats_stay, ItemCount};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!(
            "{} mysql://<user>[:<password>]@<host>[:<port>]/<database> rare_items.json",
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
    // println!("{:?}", rare);
    match executor::block_on(stats_stay(sql)) {
        Err(e) => println!("{:?}", e),
        _ => (),
    }
    match executor::block_on(stats_rare(sql, &rare)) {
        Err(e) => println!("{:?}", e),
        _ => (),
    }
}
