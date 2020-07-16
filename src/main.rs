extern crate csv;
extern crate num_cpus;

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::process;
use std::slice::Chunks;
use std::thread;
use std::time::Instant;

type Record = (String, i64, String, String, String);

#[derive(Debug)]
struct User {
    colors: HashMap<&'static String, i64>,
    shapes: HashMap<&'static String, i64>,
}

fn run() -> Result<(), Box<dyn Error>> {
    let file_path = get_first_arg()?;
    let file = File::open(file_path)?;
    let mut rdr = csv::Reader::from_reader(file);
    static mut LIST: Vec<Record> = Vec::new();
    unsafe {
        for result in rdr.deserialize() {
            let record: Record = result?;
            LIST.push(record);
        }
    }

    let number_of_cores = num_cpus::get();

    // Getting colors and shapes
    unsafe {
        let mut children = Vec::new();
        let chunk_size = LIST.len() / number_of_cores;
        let start = Instant::now();
        let chunks: Chunks<Record> = LIST.chunks(chunk_size);
        // Map phase
        for chunk in chunks.into_iter() {
            children.push(thread::spawn(move || {
                let mut colors = HashMap::new();
                let mut shapes = HashMap::new();
                for e in chunk {
                    if e.3.eq("color") {
                        *(colors.entry(&e.0).or_insert_with(|| 0)) += e.1;
                    } else if e.3.eq("shape") {
                        *(shapes.entry(&e.0).or_insert_with(|| 0)) += e.1;
                    }
                }
                return (colors, shapes);
            }));
        }
        // Reduce phase
        let mut colors: HashMap<&String, i64> = HashMap::new();
        let mut shapes: HashMap<&String, i64> = HashMap::new();
        for child in children {
            let chunk_result = child.join().unwrap();

            for (key, value) in chunk_result.0.iter() {
                *(colors.entry(key).or_insert_with(|| 0)) += value;
            }
            for (key, value) in chunk_result.1.iter() {
                *(shapes.entry(key).or_insert_with(|| 0)) += value;
            }
        }
        println!(
            "Getting colors and shapes: {}ms",
            start.elapsed().as_millis()
        );
        // Uncomment this to see the results:
        // println!("{:?}", &colors);
        // println!("{:?}", &shapes);
    }

    unsafe {
        let mut children = Vec::new();
        let chunk_size = LIST.len() / number_of_cores;
        let start = Instant::now();
        let chunks: Chunks<Record> = LIST.chunks(chunk_size);
        // Map phase
        for chunk in chunks.into_iter() {
            children.push(thread::spawn(move || {
                let mut users = HashMap::new();
                for e in chunk {
                    let user = users.entry(&e.4).or_insert_with(|| User {
                        shapes: HashMap::new(),
                        colors: HashMap::new(),
                    });
                    if e.3.eq("color") {
                        *(user.colors.entry(&e.0).or_insert_with(|| 0)) += e.1;
                    } else if e.3.eq("shape") {
                        *(user.shapes.entry(&e.0).or_insert_with(|| 0)) += e.1;
                    }
                }
                return users;
            }));
        }
        // Reduce phase
        let mut users = HashMap::new();
        for child in children {
            let chunk_result = child.join().unwrap();
            for (username, user_entry) in chunk_result {
                let user = users.entry(username).or_insert_with(|| User {
                    shapes: HashMap::new(),
                    colors: HashMap::new(),
                });
                for (key, value) in &user_entry.colors {
                    *(user.colors.entry(key).or_insert_with(|| 0)) += value;
                }
                for (key, value) in &user_entry.shapes {
                    *(user.shapes.entry(key).or_insert_with(|| 0)) += value;
                }
            }
        }

        println!(
            "Getting colors and shapes per user: {}ms",
            start.elapsed().as_millis()
        );
        // Uncomment this to see the results:
        // println!("{:?}", users);
    }
    Ok(())
}

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
