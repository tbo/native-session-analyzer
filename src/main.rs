extern crate csv;

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::process;
use std::time::Instant;

struct Entry {
    key: String,
    value: i64,
}

struct Session {
    user: String,
    date: String,
    attributes: HashMap<String, Entry>,
}

type Record = (String, i64, String, String, String);

#[derive(Debug)]
struct User {
    colors: HashMap<String, i64>,
    shapes: HashMap<String, i64>,
}

fn run() -> Result<(), Box<dyn Error>> {
    let file_path = get_first_arg()?;
    let file = File::open(file_path)?;
    let mut rdr = csv::Reader::from_reader(file);
    // let mut stack: Vec<Session> = Vec::new();
    let mut list = Vec::new();
    // let mut currentSession: Option<Session> = None;
    for result in rdr.deserialize() {
        let record: Record = result?;
        list.push(record);
        // if let Some(ref session) = currentSession {
        //     if session.date.eq(&record.2) {
        //         println!("{:?}", &record.0);
        //         continue;
        //     } else {
        //         stack.push(session);
        //     }
        // }
        // currentSession = Some(Session {
        //     user: record.4,
        //     date: record.2,
        //     attributes: HashMap::new(),
        // })
    }
    let start = Instant::now();
    let mut users = HashMap::new();
    for e in list {
        let user = users.entry(e.4).or_insert_with(|| User {
            shapes: HashMap::new(),
            colors: HashMap::new(),
        });
        if e.3.eq("color") {
            *(user.colors.entry(e.0).or_insert_with(|| 0)) += e.1;
        } else if e.3.eq("shape") {
            *(user.shapes.entry(e.0).or_insert_with(|| 0)) += e.1;
        }
    }
    println!("{}ms", start.elapsed().as_millis());
    // println!("{:?}", &users);
    Ok(())
}

/// Returns the first positional argument sent to this process. If there are no
/// positional arguments, then this returns an error.
fn get_first_arg() -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(1) {
        None => Err(From::from("expected 1 argument, but got none")),
        Some(file_path) => Ok(file_path),
    }
}

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        process::exit(1);
    }
}
