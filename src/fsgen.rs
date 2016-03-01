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
        None => 4 as u64,
        Some(val) => val.parse::<u64>().unwrap(),
    };
    let num_inodes = match matches.opt_str("n") {
        None => 10000 as u64,
        Some(val) => val.parse::<u64>().unwrap(),
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
    println!("** fsgen: Generating fsimage with num_datanodes={}, num_inodes={}, \
        out_dir={}, repl={}, seed={}.", num_datanodes, num_inodes,
        out_dir, repl, seed);
    match run(&out_dir) {
        Ok(_) => println!("** Done."),
        Err(err) => {
            println!("** ERROR: {:?}", err);
            process::exit(1);
        }
    }
} 

fn run(out_dir: &String) -> Result<(), std::io::Error> {
    if fs::metadata(out_dir).is_ok() {
        try!(fs::remove_dir_all(out_dir));
        println!("** deleted existing output directory {}", out_dir)
    }
    return Result::Ok(());
}

//fn writeImageXml() -> Result<(), io::Error> {
//    let mut f = try!(File::create("fsimage.xml"));
//}
