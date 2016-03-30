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

extern crate crossbeam;
extern crate getopts;
extern crate rand;
extern crate uuid;

use getopts::Options;
use rand::ChaChaRng;
use rand::Rng;
use std::char;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::env;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs;
use std::io::BufWriter;
use std::io::ErrorKind;
use std::io::Write;
use std::process;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::vec::Vec;
use uuid::Uuid;

// Namespace ID of generated fsimage
// TODO: make configurable
const NAMESPACE_ID : u64 = 397694258;

// Cluster ID of generated fsimage
// TODO: make configurable
static CLUSTER_ID : &'static str = "CID-4d05b066-8649-49c7-80cf-49ed7eac011c";

// Block pool ID of generated fsimage
// TODO: make configurable
const BLOCK_POOL_ID : &'static str = "BP-113955101-127.0.0.1-1455743472614";

// The cluster creation time.  This must be the same on NN and DNs.
// TODO: make configurable
const CLUSTER_CTIME : u64 = 1455743472614;

// The preferred block size used by files in this fsimage.
// TODO: make configurable
const PREFERRED_BLOCK_SIZE : u32 = 134217728;

// The default datanode layout version of the generated fsimage
const DEFAULT_DATANODE_LAYOUT_VERSION : i32 = -56;

// The default namenode layout version of the generated fsimage
const DEFAULT_NAMENODE_LAYOUT_VERSION : i32 = -60;

// The first generation stamp to use for blocks.
const FIRST_GENSTAMP : u32 = 1001;

// See SequentialBlockIdGenerator#LAST_RESERVED_BLOCK_ID
const FIRST_BLOCK_ID : u32 = 1073741825;

// See INodeId#ROOT_INODE_ID
const ROOT_INODE_ID : u32 = 16385;

// TODO: make this configurable
const ENTRIES_PER_DIR : usize = 6;

// TODO: make this configurable
const DIRS_PER_DIR : usize = 3;

// The last transaction ID we saw.
const LAST_TXID : u64 = 1;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut opts = Options::new();
    let program = args[0].clone();

    opts.optopt("d", "num_datanodes", "set the number of datanodes to generate", "NUM_DATANODES");
    opts.optflag("h", "help", "print this help menu");
    opts.optopt("n", "num_inodes", "set the number of inodes to generate", "NUM_INODES");
    opts.optopt("o", "out", "set the output directory", "NAME");
    opts.optopt("r", "repl", "set the replication factor to use", "REPL_FACTOR");
    opts.optopt("S", "storage_dirs_per_dn", "set the number of storage directories per datanode", "NUM_STORAGE_DIRS_PER_DN");
    opts.optopt("s", "seed", "set the random seed to use", "RAND_SEED");
    opts.optopt("t", "num_threads", "set the number of worker threads to use", "NUM_THREADS");
    opts.optopt("L", "namenode_layout_version", "set the NameNode layout version to use", "VERSION");
    opts.optopt("l", "datanode_layout_version", "set the DataNode layout version to use", 
                "VERSION");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { panic!(f.to_string()) }
    };
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }
    let num_datanodes = match matches.opt_str("d") {
        None => 4 as u16,
        Some(val) => val.parse::<u16>().unwrap(),
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
    let num_storage_dirs_per_dn = match matches.opt_str("S") {
        None => 20 as u16,
        Some(val) => val.parse::<u16>().unwrap(),
    };
    let seed = match matches.opt_str("s") {
        None => 0xdeadbeef as u64,
        Some(val) => val.parse::<u64>().unwrap(),
    };
    let num_threads = match matches.opt_str("t") {
        None => 16 as u32,
        Some(val) => val.parse::<u32>().unwrap(),
    };
    let dn_layout_version = match matches.opt_str("l") {
        None => DEFAULT_DATANODE_LAYOUT_VERSION,
        Some(val) => val.parse::<i32>().unwrap(),
    };
    let nn_layout_version = match matches.opt_str("L") {
        None => DEFAULT_NAMENODE_LAYOUT_VERSION,
        Some(val) => val.parse::<i32>().unwrap(),
    };
    if dn_layout_version >= 0 {
        println!("The datanode layout version must be less than 0.");
        process::exit(1);
    }
    if nn_layout_version >= 0 {
        println!("The namenode layout version must be less than 0.");
        process::exit(1);
    }
    if num_datanodes < repl {
        println!("You specified {}x replication, but only {} datanodes.",
                 repl, num_datanodes);
        process::exit(1);
    }
    let config = Config{num_datanodes: num_datanodes, num_inodes: num_inodes,
        out_dir: out_dir, repl: repl, num_storage_dirs_per_dn: num_storage_dirs_per_dn,
        seed: seed, num_threads: num_threads,
        dn_layout_version: dn_layout_version,
        nn_layout_version: nn_layout_version};
    println!("** fsgen: Generating fsimage with num_datanodes={}, num_inodes={}, \
        out_dir={}, repl={}, num_storage_dirs_per_dn={}, seed={}, num_threads={}",
        config.num_datanodes, config.num_inodes, config.out_dir, config.repl,
        config.num_storage_dirs_per_dn, config.seed, config.num_threads);
    let mut rng = ChaChaRng::new_unseeded();
    rng.set_counter(config.seed, config.seed);
    match run_main(&config, &mut rng) {
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

fn run_main(config: &Config, rng: &mut Rng) -> Result<(), std::io::Error> {
    let output_dir = OutputDir::new(&config.out_dir);
    try!(output_dir.delete_if_exists());
    let fsimage = FSImage::new(config, rng);
    println!("** generated fsimage...");
    let current_path = &(output_dir.path.clone() + "/name/current");
    try!(fs::create_dir_all(current_path));
    println!("** created {}", current_path);
    let version_path = &(output_dir.path.clone() + "/name/current/VERSION");
    try!(fsimage.write_namenode_version_file(version_path));
    println!("** wrote namenode version file {}", version_path);
    let seen_txid_path = &(output_dir.path.clone() + "/name/current/seen_txid");
    try!(fsimage.write_seen_txid_file(seen_txid_path, LAST_TXID));
    println!("** wrote seen_txid file {}", seen_txid_path);
    let edits_path = &(output_dir.path.clone() +
            "/name/current/edits_inprogress_0000000000000000001");
    try!(fsimage.write_edits_file(edits_path));
    println!("** wrote edits file {}", edits_path);
    let fsimage_path = &(output_dir.path.clone() +
            "/fsimage_0000000000000000001.xml");
    try!(fsimage.write_xml(fsimage_path));
    println!("** wrote fsimage file {}", fsimage_path);
    for datanode_idx in 0..config.num_datanodes {
        try!(fsimage.generate_datanode_dir(&output_dir.path, datanode_idx));
    }
    try!(fsimage.generate_block_files(&output_dir.path));
    return Result::Ok(());
}

// Represents an fsgen configuration.
struct Config {
    num_datanodes: u16,
    num_inodes: u32,
    out_dir: String,
    repl: u16,
    num_storage_dirs_per_dn: u16,
    seed: u64,
    num_threads: u32,
    dn_layout_version: i32,
    nn_layout_version: i32,
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
}

// Represents the FSImage which we will be writing to disk.
struct FSImage<'a> {
    // The FSGen configuration.
    config: & 'a Config,

    // Maps inode ID to inode information.
    inode_map: HashMap<u32, INode>,

    // Maps inode ID to inode children.
    children: HashMap<u32, Vec<u32>>,

    // The next inode ID to use.
    next_inode_id: u32,

    // The next ID which could be an empty directory.
    next_possible_empty_dir_id: u32,

    // The number of inodes
    num_inodes: u32,

    // The next block genstamp to use.
    next_genstamp: u32,

    // The next block id to use.
    next_block_id: u32,

    // Information about the datanodes.
    datanode_info: Vec<DatanodeInfo>,
}

struct DatanodeInfo {
    // The UUID of this DataNode.
    datanode_uuid: String,

    // The IDs of each storage in this DataNode.
    storage_ids: Vec<String>,
}

fn random_str(rng: &mut Rng, len: u32) -> String {
    let mut ret = String::new();
    for _ in 0..len {
        let val = rng.next_u32() % 26;
        ret.push(char::from_u32(val + 0x61).unwrap());
    }
    return ret;
}

fn generate_dn_info(config: &Config) -> Vec<DatanodeInfo> {
    let mut datanodes : Vec<DatanodeInfo> = vec![];
    for _ in 0..config.num_datanodes {
        let mut storage_ids : Vec<String> = vec![];
        for _ in 0..config.num_storage_dirs_per_dn {
            storage_ids.push(
                "DS-".to_owned() + &Uuid::new_v4().to_hyphenated_string());
        }
        let datanode = DatanodeInfo {
            datanode_uuid: Uuid::new_v4().to_hyphenated_string(),
            storage_ids: storage_ids,
        };
        datanodes.push(datanode);
    }
    return datanodes;
}

impl<'a> FSImage<'a> {
    fn new(config: &'a Config, rng: &mut Rng) -> FSImage<'a> {
        let mut fs_image = FSImage {
            config: config,
            inode_map: HashMap::new(),
            children: HashMap::new(),
            next_inode_id: (ROOT_INODE_ID + 1),
            next_possible_empty_dir_id: ROOT_INODE_ID,
            num_inodes: 0,
            next_genstamp: FIRST_GENSTAMP,
            next_block_id: FIRST_BLOCK_ID,
            datanode_info: generate_dn_info(config),
        };
        fs_image.generate(rng);
        return fs_image;
    }

    fn generate(&mut self, rng: &mut Rng) {
        let root_inode = INode {
            id: ROOT_INODE_ID,
            name: "".to_owned(),
            is_dir: true,
            blocks: vec![],
        };
        let mut parents : LinkedList<u32> = LinkedList::new();
        self.inode_map.insert(ROOT_INODE_ID, root_inode);
        parents.push_back(ROOT_INODE_ID);
        self.num_inodes = self.num_inodes + 1;
        loop {
            let parent_id = self.find_shallowest_incomplete_dir();
            for i in 0..ENTRIES_PER_DIR {
                if self.num_inodes > self.config.num_inodes {
                    return;
                }
                let id = self.next_inode_id;
                self.next_inode_id = self.next_inode_id + 1;
                {
                    let mut children = self.children.get_mut(&parent_id).unwrap();
                    children.push(id);
                }
                let name = format!("{}{}", (0x61 + i), random_str(rng, 3));
                let is_dir = i < DIRS_PER_DIR;
                if is_dir {
                    let inode = INode {
                        id: id,
                        name: name,
                        is_dir: true,
                        blocks: vec![],
                    };
                    self.inode_map.insert(id, inode);
                    self.children.insert(id, vec![]);
                } else {
                    let inode = INode {
                        id: id,
                        name: format!("{}{}", (0x61 + i), random_str(rng, 3)),
                        is_dir: false,
                        blocks: vec![self.generate_random_block(rng)],
                    };
                    self.inode_map.insert(id, inode);
                }
                self.num_inodes = self.num_inodes + 1;
            }
        }
    }

    fn find_shallowest_incomplete_dir(&mut self) -> u32 {
        loop {
            let id = self.next_possible_empty_dir_id;
            let inode = self.inode_map.get(&id).unwrap();
            if inode.is_dir {
                let children = self.children.entry(id).or_insert(vec![]);
                if children.len() < ENTRIES_PER_DIR {
                    return id;
                }
            }
            self.next_possible_empty_dir_id = self.next_possible_empty_dir_id + 1;
        }
    }

    fn generate_random_block(&mut self, rng: &mut Rng) -> Block {
        let mut datanodes : Vec<u16> = Vec::new();
        for i in 0..self.config.repl {
            let range = (self.config.num_datanodes - i) as u32;
            let mut val = (rng.next_u32() % range) as u16;
            {
                for datanode in &datanodes {
                    if val >= *datanode {
                        val = val + 1;
                    }
                }
            }
            datanodes.push(val);
        }
        let id = self.next_block_id;
        self.next_block_id = self.next_block_id + 1;
        let genstamp = self.next_genstamp;
        self.next_genstamp = self.next_genstamp + 1;
        return Block {
            id: id,
            genstamp: genstamp,
            datanodes: datanodes,
        };
    }

    // Write the VERSION file which identifies the version of this fsimage.
    pub fn write_namenode_version_file(&self,
                        path: &str) -> Result<(), std::io::Error> {
        let file = try!(OpenOptions::new().
            read(false).
            write(true).
            create(true).
            open(path));
        let mut w = BufWriter::new(&file);
        try!(write!(w, "#Thu Feb 18 11:20:35 PST 2016\n"));
        try!(write!(w, "namespaceID={}\n", NAMESPACE_ID));
        try!(write!(w, "clusterID={}\n", CLUSTER_ID));
        try!(write!(w, "cTime={}\n", CLUSTER_CTIME));
        try!(write!(w, "storageType=NAME_NODE\n"));
        try!(write!(w, "blockpoolID={}\n", BLOCK_POOL_ID));
        try!(write!(w, "layoutVersion={}\n", self.config.nn_layout_version));
        return Result::Ok(());
    }

    // Write the seen_txid file in the namenode storage directory.  This file
    // identifies the highest transaction id (txid) that we've seen.
    pub fn write_seen_txid_file(&self, path: &str,
                               seen_txid: u64) -> Result<(), std::io::Error> {
        let file = try!(OpenOptions::new().
            read(false).
            write(true).
            create(true).
            open(path));
        let mut w = BufWriter::new(&file);
        try!(write!(w, "{}\n", seen_txid));
        return Result::Ok(());
    }

    // Write an HDFS edit log file with just the 4-byte layout version and
    // the 4-byte feature flags int (which is always 0).
    // TODO: use the version constant above to write this.
    // TODO: write OP_EDIT_LOG_START?
    pub fn write_edits_file(&self,
                    path: &str) -> Result<(), std::io::Error> {
        let file = try!(OpenOptions::new().
            read(false).
            write(true).
            create(true).
            open(path));
        let mut w = BufWriter::new(&file);
        let arr = [ 0xffu8, 0xffu8, 0xffu8, 0xc0u8,
            0x00u8, 0x00u8, 0x00u8, 0x00u8 ];
        try!(w.write(&arr));
        return Result::Ok(());
    }

    // Write the FSImage XML.
    pub fn write_xml(&self, path: &str) -> Result<(), std::io::Error> {
        let file = try!(OpenOptions::new().
            read(false).
            write(true).
            create(true).
            open(path));
        let mut w = BufWriter::new(&file);
        try!(write!(w, "<?xml version=\"1.0\"?>"));
        try!(write!(w, "<fsimage>"));
        try!(self.write_version_section(&mut w));
        try!(self.write_name_section(&mut w));
        try!(self.write_inode_section(&mut w));
        try!(write!(w, "<INodeReferenceSection></INodeReferenceSection>"));
        try!(self.write_snapshot_section(&mut w));
        try!(self.write_inode_directory_section(&mut w));
        try!(write!(w, "<FileUnderConstructionSection></FileUnderConstructionSection>"));
        try!(self.write_snapshot_diff_section(&mut w));
        try!(self.write_secret_manager_section(&mut w));
        try!(self.write_cache_manager_section(&mut w));
        try!(write!(w, "</fsimage>"));
        return Result::Ok(());
    }

    fn write_snapshot_section(&self, w: &mut BufWriter<&File>) -> Result<(), std::io::Error> {
        try!(write!(w, "<SnapshotSection>"));
        try!(write!(w, "<snapshotCounter>0</snapshotCounter>"));
        try!(write!(w, "<numSnapshots>0</numSnapshots>"));
        try!(write!(w, "</SnapshotSection>\n"));
        return Result::Ok(());
    }

    fn write_snapshot_diff_section(&self, w: &mut BufWriter<&File>) -> Result<(), std::io::Error> {
        try!(write!(w, "<SnapshotDiffSection>"));
        try!(write!(w, "<dirDiffEntry><inodeId>16385</inodeId><count>0</count></dirDiffEntry>"));
        try!(write!(w, "</SnapshotDiffSection>\n"));
        return Result::Ok(());
    }

    pub fn generate_block_files(&self, base_path: &str) -> Result<(), std::io::Error> {
        let files_processed = Arc::new(AtomicUsize::new(0));
        let mut threads = vec![];
        let inode_map = Arc::new(&self.inode_map);
        let num_threads = self.config.num_threads;
        let num_storage_dirs_per_dn = self.config.num_storage_dirs_per_dn;
        {
            for thread_idx in 0..self.config.num_threads {
                let inode_map_ref = inode_map.clone();
                let files_processed_ref = files_processed.clone();
                // This is actually safe, but the compiler can't prove it to be so.
                // We join all these threads at the end of the function.
                //
                // TODO: it would be nice to avoid this unsafe block.  Probably
                // the best way to do that would be to move to a channel-based
                // architecture where worker threads received paths of block
                // files to create.  Might also be able to do something with
                // scoped threads?
                unsafe {
                    let thread = crossbeam::spawn_unsafe(move|| {
                        for (_, inode) in inode_map_ref.iter() {
                            if inode.is_dir {
                                continue;
                            }
                            if inode.id % num_threads != thread_idx {
                                continue;
                            }
                            for block in &inode.blocks {
                                match block.generate_block_files(
                                    base_path, num_storage_dirs_per_dn) {
                                    Ok(()) => (),
                                    Err(e) => {
                                        println!("Thread {} failed to create block {}: {}",
                                                 thread_idx, block.id, e);
                                        panic!(e);
                                    }
                                };
                            }
                            let p = files_processed_ref.fetch_add(1, Ordering::Relaxed);
                            if (p != 0) && (p % 10000) == 0 {
                                println!("Created {} blocks on disk...", p);
                            }
                        }
                    });
                    threads.push(thread);
                }
            }
            for i in threads {
                let _ = i.join();
            }
        }
        println!("** generate_block_files: processed about {} files.",
                 files_processed.load(Ordering::Relaxed));
        return Result::Ok(());
    }

    // Write the FSImage XML.
    fn write_version_section(&self, w: &mut BufWriter<&File>) -> Result<(), std::io::Error> {
        try!(write!(w, "<version>"));
        try!(write!(w, "<layoutVersion>{}</layoutVersion>", self.config.nn_layout_version));
        try!(write!(w, "<onDiskVersion>1</onDiskVersion>"));
        try!(write!(w, "<oivRevision>545bbef596c06af1c3c8dca1ce29096a64608478</oivRevision>"));
        try!(write!(w, "</version>\n"));
        return Result::Ok(());
    }

    fn write_name_section(&self, w: &mut BufWriter<&File>) -> Result<(), std::io::Error> {
        try!(write!(w, "<NameSection>"));
        try!(write!(w, "<namespaceId>{}</namespaceId>", NAMESPACE_ID));
        try!(write!(w, "<genstampV1>1000</genstampV1>"));
        try!(write!(w, "<genstampV2>{}</genstampV2>", self.next_genstamp));
        try!(write!(w, "<genstampV1Limit>0</genstampV1Limit>"));
        try!(write!(w, "<lastAllocatedBlockId>{}</lastAllocatedBlockId>",
                    self.next_block_id - 1));
        try!(write!(w, "<txid>{}</txid>", LAST_TXID));
        try!(write!(w, "</NameSection>\n"));
        return Result::Ok(());
    }

    fn write_inode_section(&self, w: &mut BufWriter<&File>) -> Result<(), std::io::Error> {
        try!(write!(w, "<INodeSection>"));
        try!(write!(w, "<lastInodeId>{}</lastInodeId>", self.next_inode_id - 1));
        try!(write!(w, "<numInodes>{}</numInodes>", self.num_inodes));
        for (_, inode) in self.inode_map.iter() {
            try!(write!(w, "{}", inode.to_xml().to_owned()));
        }
        try!(write!(w, "</INodeSection>\n"));
        return Result::Ok(());
    }

    fn write_inode_directory_section(&self, w: &mut BufWriter<&File>) -> Result<(), std::io::Error> {
        try!(write!(w, "<INodeDirectorySection>"));
        for (parent_id, id_vec) in self.children.iter() {
            try!(write!(w, "<directory><parent>{}</parent>", parent_id));
            for child_id in id_vec {
                try!(write!(w, "<child>{}</child>", child_id));
            }
            try!(write!(w, "</directory>"));
        }
        try!(write!(w, "</INodeDirectorySection>\n"));
        return Result::Ok(());
    }

    fn write_secret_manager_section(&self, w: &mut BufWriter<&File>) -> Result<(), std::io::Error> {
        try!(write!(w, "<SecretManagerSection>"));
        try!(write!(w, "<currentId>2</currentId>")); // ???
        try!(write!(w, "<tokenSequenceNumber>1</tokenSequenceNumber>")); // ???
        try!(write!(w, "<numDelegationKeys>0</numDelegationKeys>"));
        try!(write!(w, "<numTokens>0</numTokens>"));
        try!(write!(w, "</SecretManagerSection>\n"));
        return Result::Ok(());
    }

    fn write_cache_manager_section(&self, w: &mut BufWriter<&File>) -> Result<(), std::io::Error> {
        try!(write!(w, "<CacheManagerSection>"));
        try!(write!(w, "<nextDirectiveId>1</nextDirectiveId>"));
        try!(write!(w, "<numDirectives>0</numDirectives>"));
        try!(write!(w, "<numPools>0</numPools>"));
        try!(write!(w, "</CacheManagerSection>\n"));
        return Result::Ok(());
    }

    // The datanode layout looks like this:
    //
    // data
    // data/current
    // data/current/VERSION
    // data/current/BP-113955101-127.0.0.1-1455743472614
    // data/current/BP-113955101-127.0.0.1-1455743472614/tmp [empty dir]
    // data/current/BP-113955101-127.0.0.1-1455743472614/current
    // data/current/BP-113955101-127.0.0.1-1455743472614/current/VERSION
    // data/current/BP-113955101-127.0.0.1-1455743472614/current/rbw [empty dir]
    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized
    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0
    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0/subdir0
    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0/subdir0/blk_1073741825
    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0/subdir0/blk_1073741826
    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0/subdir0/blk_1073741828_1004.meta
    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0/subdir0/blk_1073741828
    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0/subdir0/blk_1073741825_1001.meta
    // data/current/BP-113955101-127.0.0.1-1455743472614/current/finalized/subdir0/subdir0/blk_1073741826_1002.meta
    //
    // Note that block files must be placed based on their IDs. 
    fn generate_datanode_dir(&self, base_path: &str, datanode_idx: u16)
            -> Result<(), std::io::Error> {
        println!("** generating datanode dir {} in {}...",
                 datanode_idx + 1, base_path);
        for storage_idx in 0..self.config.num_storage_dirs_per_dn {
            let dir = format!("{}/datanode{:>02}/storage{:>02}/current",
                 base_path, datanode_idx + 1, storage_idx + 1);
            let bp_dir = format!("{}/{}", dir, BLOCK_POOL_ID);
            try!(fs::create_dir_all(&bp_dir));
            try!(self.write_datanode_version_file(&format!("{}/VERSION", dir),
                                                 datanode_idx, storage_idx));

            try!(fs::create_dir(format!("{}/tmp", &bp_dir)));
            let cdir = format!("{}/current", &bp_dir);
            try!(fs::create_dir(&cdir));
            try!(fs::create_dir(format!("{}/rbw", cdir)));
            try!(fs::create_dir(format!("{}/finalized", cdir)));
            try!(self.write_blockpool_version_file(&format!("{}/VERSION", cdir)));
        }
        println!("** finished generating datanode dir {} in {}...",
                 datanode_idx + 1, base_path);
        return Result::Ok(());
    }

    // Write the VERSION file which identifies the version of this datanode storage directory.
    fn write_datanode_version_file(&self, path: &str,
                        datanode_idx: u16, storage_idx: u16)
                        -> Result<(), std::io::Error> {
        let file = try!(OpenOptions::new().
            read(false).
            write(true).
            create(true).
            open(path));
        let mut w = BufWriter::new(&file);
        let dn_info = self.datanode_info.get(datanode_idx as usize).unwrap();
        try!(write!(w, "#Thu Feb 18 11:20:35 PST 2016\n"));
        try!(write!(w, "storageID={}\n",
                    dn_info.storage_ids.get(storage_idx as usize).unwrap()));
        try!(write!(w, "clusterID={}\n", CLUSTER_ID));
        try!(write!(w, "cTime={}\n", CLUSTER_CTIME));
        try!(write!(w, "datanodeUuid={}\n", dn_info.datanode_uuid));
        try!(write!(w, "storageType=DATA_NODE\n"));
        try!(write!(w, "layoutVersion={}\n", self.config.dn_layout_version));
        return Result::Ok(());
    }

    fn write_blockpool_version_file(&self, path: &str)
                        -> Result<(), std::io::Error> {
        let file = try!(OpenOptions::new().
            read(false).
            write(true).
            create(true).
            open(path));
        let mut w = BufWriter::new(&file);
        try!(write!(w, "#Thu Feb 18 11:20:35 PST 2016\n"));
        try!(write!(w, "namespaceID={}\n", NAMESPACE_ID));
        try!(write!(w, "cTime={}\n", CLUSTER_CTIME));
        try!(write!(w, "blockpoolID={}\n", BLOCK_POOL_ID));
        try!(write!(w, "layoutVersion={}\n", self.config.dn_layout_version));
        return Result::Ok(());
    }
}

// Represents an HDFS INode (directory or file)
struct INode {
    id: u32,
    name: String,
    is_dir: bool,
    blocks: Vec<Block>,
}

impl INode {
    fn get_type_name(&self) -> &'static str {
        if self.is_dir {
            return "DIRECTORY";
        } else {
            return "FILE";
        }
    }

    pub fn to_xml(&self) -> String {
        let mut ret = "<inode>".to_owned();
        ret.push_str(&format!("<id>{}</id>", self.id));
        ret.push_str(&format!("<type>{}</type>", self.get_type_name()));
        ret.push_str(&format!("<name>{}</name>", self.name));
        ret.push_str(&format!("<mtime>{}</mtime>", 0));
        if self.is_dir {
            ret.push_str(&format!("<dsquota>{}</dsquota>", -1));
            ret.push_str(&format!("<nsquota>{}</nsquota>", -1));
        } else {
            ret.push_str(&format!("<atime>{}</atime>", 0));
            ret.push_str(&format!("<replication>{}</replication>",
                     self.blocks.len()));
            ret.push_str(&format!("<preferredBlockSize>{}</preferredBlockSize>",
                     PREFERRED_BLOCK_SIZE));
        }
        ret.push_str(&format!("<permission>{}</permission>", "cmccabe:supergroup:0644"));
        if !self.is_dir {
            ret.push_str("<blocks>");
            for block in &self.blocks {
                ret.push_str(&block.to_xml());
            }
            ret.push_str("</blocks>");
        }
        ret.push_str("</inode>");
        return ret;
    }
}

struct Block {
    // The ID of the block
    id: u32,

    // The genstamp of the block
    genstamp: u32,

    // The datanodes which have a replica of this block
    datanodes: Vec<u16>,
}

impl Block {
    pub fn to_xml(&self) -> String {
        let mut ret = "<block>".to_owned();
        ret.push_str(&format!("<id>{}</id>", self.id));
        ret.push_str(&format!("<genstamp>{}</genstamp>", self.genstamp));
        ret.push_str(&format!("<numBytes>{}</numBytes>", 0));
        ret.push_str("</block>");
        return ret;
    }

    pub fn generate_block_files(&self, base_path: &str, num_storage_dirs_per_dn: u16)
            -> Result<(), std::io::Error> {
        for datanode in &self.datanodes {
            let storage_idx = ((self.id as u64) * ((datanode + 1) as u64) * 29) %
                (num_storage_dirs_per_dn as u64);
            let finalized_base = format!(
                "{}/datanode{:>02}/storage{:>02}/current/{}/current/finalized",
                base_path, datanode + 1, storage_idx + 1, BLOCK_POOL_ID);
            match self.generate_meta_and_block_file(&finalized_base) {
                Ok(()) => (),
                Err(e) => {
                    println!("Failed to generate meta and block file in {}: {}",
                             finalized_base, e);
                    return Result::Err(e);
                },
            }
        }
        return Ok(());
    }

    pub fn generate_meta_and_block_file(&self,
                            finalized_base: &str) -> Result<(), std::io::Error> {
        let subdir = format!("{}/subdir{}/subdir{}",
            finalized_base, (self.id >> 16) & 0xff, (self.id >> 8) & 0xff);
        loop {
            match fs::create_dir_all(&subdir) {
                Ok(()) => break,
                // If we get EEXIST, another worker thread already created this
                // directory or one of its parents while we were attempting to
                // do so.  We should retry until we no longer get EEXIST, to
                // ensure that our full path exists (we might have gotten
                // EEXIST creating a parent rather than the directory we
                // wanted).
                Err(ref e) if e.kind() == ErrorKind::AlreadyExists => {
                    println!("Got EEXIST on {} for {}", finalized_base, self.id);
                },
                Err(e) => return Err(e),
            }
        }
        // Write empty block data file 
        {
            let data_path = format!("{}/blk_{}_{}",
                    &subdir, self.id, self.genstamp);
            let file = try!(OpenOptions::new().
                read(false).
                write(true).
                create(true).
                open(&data_path));
            let mut w = BufWriter::new(&file);
            let arr = [ ];
            try!(w.write(&arr));
        }
        // Write block header file
        {
            let meta_path = format!("{}/blk_{}_{}.meta",
                    &subdir, self.id, self.genstamp);
            let file = try!(OpenOptions::new().
                read(false).
                write(true).
                create(true).
                open(&meta_path));
            let arr = [ 0x00u8, 0x01u8, 0x02u8, 0x00u8,
                0x00u8, 0x02u8, 0x00u8, 0x96u8,
                0x26u8, 0x34u8, 0x7bu8 ];
            let mut w = BufWriter::new(&file);
            try!(w.write(&arr));
        }
        return Result::Ok(());
    }
}
