extern crate chrono;
extern crate futures;
extern crate sqlx;

use futures::executor;
use std::env;

mod stats;
use stats::stats_stay;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!(
            "{} mysql://<user>[:<password>]@<host>[:<port>]/<database>",
            args[0]
        );
        return;
    }
    let sql = &args[1];
    match executor::block_on(stats_stay(sql)) {
        Err(e) => println!("{:?}", e),
        _ => (),
    }
}
