/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

extern crate getopts;

use getopts::Options;
use std::io::Error;
use std::fs;
use std::env;
use std::process;

struct Config {
    num_datanodes: u32,
    num_inodes: u32,
    out_dir: String,
    repl: u16,
    seed: u64,
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    println!("fsgen: Generates an HDFS fsimage.\n");
    println!("By using the fsgen tool, you can quickly generate large HDFS");
    println!("fsimages for testing purposes.  fsgen has two main outputs,");
    println!("an fsimage.xml file and a block directory.");
    print!("{}", opts.usage(&brief));
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut opts = Options::new();
    let program = args[0].clone();

    opts.optopt("d", "num_datanodes", "set the number of datanodes to generate", "NUM_DATANODES");
    opts.optflag("h", "help", "print this help menu");
    opts.optopt("n", "num_inodes", "set the number of inodes to generate", "NUM_INODES");
    opts.optopt("o", "out", "set the output directory", "NAME");
    opts.optopt("r", "repl", "set the replication factor to use", "REPL_FACTOR");
    opts.optopt("s", "seed", "set the random seed to use", "RAND_SEED");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { panic!(f.to_string()) }
    };
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }
    let num_datanodes = match matches.opt_str("d") {
        None => 4 as u32,
        Some(val) => val.parse::<u32>().unwrap(),
    };
    let num_inodes = match matches.opt_str("n") {
        None => 10000 as u32,
        Some(val) => val.parse::<u32>().unwrap(),
    };
    let out_dir = matches.opt_str("o").unwrap_or("".to_owned());
    if out_dir == "" {
        println!("You must specify an output directory with -o.  -h for help.");
        process::exit(1);
    };
    let repl = match matches.opt_str("r") {
        None => 3 as u16,
        Some(val) => val.parse::<u16>().unwrap(),
    };
    let seed = match matches.opt_str("s") {
        None => 0xdeadbeef as u64,
        Some(val) => val.parse::<u64>().unwrap(),
    };
    // The statements here will be executed when the compiled binary is called
    // Print text to the console
    let config = Config{num_datanodes: num_datanodes, num_inodes: num_inodes,
        out_dir: out_dir, repl: repl, seed:seed};
    println!("** fsgen: Generating fsimage with num_datanodes={}, num_inodes={}, \
        out_dir={}, repl={}, seed={}.", config.num_datanodes, config.num_inodes,
        config.out_dir, config.repl, config.seed);
    match run(&config) {
        Ok(_) => println!("** Done."),
        Err(err) => {
            println!("** ERROR: {:?}", err);
            process::exit(1);
        }
    }
} 

fn run(config: &Config) -> Result<(), std::io::Error> {
    if fs::metadata(config.out_dir.to_owned()).is_ok() {
        try!(fs::remove_dir_all(config.out_dir.to_owned()));
        println!("** deleted existing output directory {}", config.out_dir)
    }
    return Result::Ok(());
}

//fn writeImageXml() -> Result<(), io::Error> {
//    let mut f = try!(File::create("fsimage.xml"));
//}
