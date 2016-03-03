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
use std::collections::HashMap;
use std::env;
use std::fs::OpenOptions;
use std::fs;
use std::io::BufWriter;
use std::io::Error;
use std::io::Write;
use std::process;
use std::vec::Vec;

// Namespace ID of generated fsimage
// TODO: make configurable
const NAMESPACE_ID : u64 = 397694258;

// Cluster ID of generated fsimage
// TODO: make configurable
static CLUSTER_ID : &'static str = "CID-4d05b066-8649-49c7-80cf-49ed7eac011c";

// Block pool ID of generated fsimage
// TODO: make configurable
const BLOCK_POOL_ID : &'static str = "BP-113955101-127.0.0.1-1455743472614";

// The layout version of the generated fsimage
const LAYOUT_VERSION : i32 = -64;

// See SequentialBlockIdGenerator#LAST_RESERVED_BLOCK_ID
const FIRST_BLOCK_ID : u64 = 1073741825;

// See INodeId#ROOT_INODE_ID
const ROOT_INODE_ID : u64 = 32768;

// TODO: make this configurable
const FILES_PER_DIR : u32 = 10;

// TODO: make this configurable
const DIRS_PER_DIR : u32 = 5;

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
    let config = Config{num_datanodes: num_datanodes, num_inodes: num_inodes,
        out_dir: out_dir, repl: repl, seed:seed};
    println!("** fsgen: Generating fsimage with num_datanodes={}, num_inodes={}, \
        out_dir={}, repl={}, seed={}.", config.num_datanodes, config.num_inodes,
        config.out_dir, config.repl, config.seed);
    match run_main(&config) {
        Ok(_) => println!("** Done."),
        Err(err) => {
            println!("** ERROR: {:?}", err);
            process::exit(1);
        }
    }
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    println!("fsgen: Generates an HDFS fsimage.\n");
    println!("By using the fsgen tool, you can quickly generate large HDFS");
    println!("fsimages for testing purposes.  fsgen has two main outputs,");
    println!("an fsimage.xml file and a block directory.");
    print!("{}", opts.usage(&brief));
}

fn run_main(config: &Config) -> Result<(), std::io::Error> {
    let output_dir = OutputDir::new(&config.out_dir);
    try!(output_dir.setup());
//    let fsimage = FSImage::new(config);
//    try!(fsimage.writeXml(config.get_fsimage_xml_name()))
//    for datanodeIdx in 0..config.num_datanodes {
//        try!(fsimage.generate_datanode_dir(datanodeIdx))
//    }
    return Result::Ok(());
}

// Represents an fsgen configuration.
struct Config {
    num_datanodes: u32,
    num_inodes: u32,
    out_dir: String,
    repl: u16,
    seed: u64,
}

// Represents an output directory where we will generate some files.
struct OutputDir {
    path: String,
}

impl OutputDir {
    fn new(path: &str) -> OutputDir {
        return OutputDir {
            path: path.to_owned(),
        }
    }

    // Delete the output directory and all its contents, if it exists.
    fn delete_if_exists(&self) -> Result<(), std::io::Error> {
        if fs::metadata(self.path.to_owned()).is_ok() {
            try!(fs::remove_dir_all(self.path.to_owned()));
            println!("** deleted existing output directory {}", self.path)
        }
        return Result::Ok(());
    }

    // Set up the output directory.
    fn setup(&self) -> Result<(), std::io::Error> {
        try!(self.delete_if_exists());
        try!(fs::create_dir_all(self.path.clone() + "/name/current"));
        try!(self.write_version_file());
        try!(self.write_seen_txid_file());
        try!(self.write_edits_file());
        return Result::Ok(());
    }

    // Write the VERSION file which identifies the version of this fsimage.
    fn write_version_file(&self) -> Result<(), std::io::Error> {
        let file = try!(OpenOptions::new().
            read(false).
            write(true).
            create(true).
            open(self.path.clone() + "/name/current/VERSION"));
        let mut w = BufWriter::new(&file);
        try!(write!(w, "#Thu Feb 18 11:20:35 PST 2016\n"));
        try!(write!(w, "namespaceID={}\n", NAMESPACE_ID));
        try!(write!(w, "clusterID={}\n", CLUSTER_ID));
        try!(write!(w, "cTime=1455743472614\n"));
        try!(write!(w, "storageType=NAME_NODE\n"));
        try!(write!(w, "blockpoolID={}\n", BLOCK_POOL_ID));
        try!(write!(w, "layoutVersion={}\n", LAYOUT_VERSION));
        return Result::Ok(());
    }

    // Write the seen_txid file in the namenode storage directory.  This file
    // identifies the highest transaction id (txid) that we've seen.
    fn write_seen_txid_file(&self) -> Result<(), std::io::Error> {
        let file = try!(OpenOptions::new().
            read(false).
            write(true).
            create(true).
            open(self.path.clone() + "/name/current/seen_txid"));
        let mut w = BufWriter::new(&file);
        try!(write!(w, "1\n"));
        return Result::Ok(());
    }

    // Write an HDFS edit log file with just the 4-byte layout version and
    // the 4-byte feature flags int (which is always 0).
    // TODO: use the version constant above to write this.
    // TODO: write OP_EDIT_LOG_START?
    fn write_edits_file(&self) -> Result<(), std::io::Error> {
        let file = try!(OpenOptions::new().
            read(false).
            write(true).
            create(true).
            open(self.path.clone() +
                 "/name/current/edits_inprogress_0000000000000000001"));
        let mut w = BufWriter::new(&file);
        let arr = [ 0xffu8, 0xffu8, 0xffu8, 0xc0u8,
            0x00u8, 0x00u8, 0x00u8, 0x00u8 ];
        try!(w.write(&arr));
        return Result::Ok(());
    }

    // Get the name which we should use for our fsimage XML.
    fn get_fsimage_xml_name(&self) -> String {
        return self.path.clone() + "/fsimage_0000000000000000001.xml";
    }
}

//// Represents the FSImage which we will be writing to disk.
//struct FSImage {
//    // The FSGen configuration.
//    config: Config,
//
//    // Maps inode ID to inode information.
//    inodeMap: HashMap<u32, INode>,
//
//    // Maps inode ID to inode children.
//    children: HashMap<u32, Vec<u32>>,
//}
//
//impl FSImage {
//    fn new(config: Config) -> FSImage {
//        return FSImage {
//            config: config,
//        }
//    }
//
//    // Write the FSImage XML.
//    pub fn writeXml(&self, path: &str) -> Result<(), std::io::Error> {
//        let file = OpenOptions::new().
//            read(false).
//            write(true).
//            create(true).
//            open(path);
//        print("<?xml version="1.0"?>");
//        print("<fsimage>");
//        try!(self.writeVersionSection());
//        try!(self.writeNameSection());
//        try!(self.writeFsImageHeader());
//        try!(self.writeINodeSection());
//        print("<INodeReferenceSection></INodeReferenceSection>");
//        print("<SnapshotSection></SnapshotSection>");
//        try!(writeINodeDirectorySection());
//        print("<FileUnderConstructionSection></FileUnderConstructionSection>");
//        print("<SnapshotDiffSection></SnapshotDiffSection>");
//        try!(writeSecretManagerSection());
//        print("<CacheManagerSection></CacheManagerSection>");
//        print("</fsimage>");
//    }
//
//    // Write the FSImage XML.
//    fn writeVersionSection(&self) -> Result<(), std::io::Error> {
//        print("<version>");
//        print("<layoutVersion>-64</layoutVersion>");
//        print("<onDiskVersion>1</onDiskVersion>");
//        print("<oivRevision>545bbef596c06af1c3c8dca1ce29096a64608478</oivRevision>");
//        print("</version>\n");
//    }
//
//    fn writeNameSection(&self) -> Result<(), std::io::Error> {
//        print("<NameSection>");
//        print("<namespaceId>397694258</namespaceId>");
//        print("<genstampV1>1000</genstampV1>");
//        print("<genstampV2>1012</genstampV2>");
//        print("<genstampV1Limit>0</genstampV1Limit>");
//        print("<lastAllocatedBlockId>1073741836</lastAllocatedBlockId>"); // TODO: what to do about this?
//        print("<txid>79</txid>");
//        print("</NameSection>");
//    }
//
//    fn writeINodeSection(&self) -> Result<(), std::io::Error> {
//        print("<INodeSection>");
//        print("</INodeSection>");
//    }
//
//    fn writeINodeDirectorySection(&self) -> Result<(), std::io::Error> {
//        print("<INodeDirectorySection>");
//        print("</INodeDirectorySection>");
//    }
//
//    fn writeSecretManagerSection(&self) -> Result<(), std::io::Error> {
//        print("<SecretManagerSection>");
//        print("<currentId>2</currentId>"); // ???
//        print("<tokenSequenceNumber>1</tokenSequenceNumber>"); // ???
//        print("</SecretManagerSection>");
//    }
//
//    // The datanode layout looks like this:
//    //
//    // data
//    // data/current
//    // data/current/VERSION
//    // data/current/BP-113955101-127.0.0.1-1455743472614
//    // data/current/BP-113955101-127.0.0.1-1455743472614/tmp [empty dir]
//    // data/current/BP-113955101-127.0.0.1-1455743472614/current
//    // data/current/BP-113955101-127.0.0.1-1455743472614/current/VERSION
//    // data/current/BP-113955101-127.0.0.1-1455743472614/current/rbw [empty dir]
//    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized
//    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0
//    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0/subdir0
//    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0/subdir0/blk_1073741825
//    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0/subdir0/blk_1073741826
//    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0/subdir0/blk_1073741828_1004.meta
//    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0/subdir0/blk_1073741828
//    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0/subdir0/blk_1073741825_1001.meta
//    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0/subdir0/blk_1073741826_1002.meta
//    //
//    // Note that block files must be placed based on their IDs. 
//    fn generate_datanode_dir(&self) -> Result<(), std::io::Error> {
//        return Result::Ok(());
//    }
//}
//
//// Represents an HDFS INode (directory or file)
//struct INode {
//    id: u32,
//    name: String,
//    isDir: bool,
//}
//
//impl INode {
//    fn get_type_name(&self) -> String {
//        if self.isDir {
//            return "DIRECTORY";
//        } else {
//            return "FILE";
//        }
//    }
//
//    pub fn get_inode_section_xml(&self) -> String {
//        return format!("<inode>\
//            <id>{}</id>\
//            <type>{}</type>\
//            <name>{}</name>\
//            <mtime>0</mtime>\
//            <permission>cmccabe:supergroup:0755</permission>\
//            <nsquota>-1</nsquota>\
//            <dsquota>-1</dsquota>\
//            </inode>",
//            self.id, self.get_type_name(), self.name);
//    }
//}
