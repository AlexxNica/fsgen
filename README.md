fsgen
===============================================================================
fsgen is an fsimage generator for the Hadoop Distributed FileSystem (HDFS).  By
using this tool, you can quickly generate large HDFS fsimages for testing
purposes.  fsgen is written in Rust.

fsgen has three main outputs:
* an fsimage.xml file 
* a namenode directory
* a set of datanode directories

The fsimage.xml file should be processed by the *hdfs oiv* tool to produce a binary fsimage.
The directories should be copied into the appropriate place for the NameNode and DataNodes.

Build Process
===============================================================================
To build fsgen, install Rust and Cargo using your package manager of choice:
  sudo apt-get install rust cargo

Then build the program using Cargo:
  cd fsgen
  cargo build

Example Usage
===============================================================================
    # Run fsgen
    $ ./target/debug/fsgen -d 4 -o /tmp/foo -r 3 -s 123
    ** fsgen: Generating fsimage with num_datanodes=4, num_inodes=10000, out_dir=/tmp/foo, repl=3, seed=123.
    ** deleted existing output directory /tmp/foo
    ** generated fsimage...
    ** created /tmp/foo/name/current
    ** wrote namenode version file /tmp/foo/name/current/VERSION
    ** wrote seen_txid file /tmp/foo/name/current/seen_txid
    ** wrote edits file /tmp/foo/name/current/edits_inprogress_0000000000000000001
    ** wrote fsimage file /tmp/foo/fsimage_0000000000000000001.xml
    ** generating datanode dir 1 in /tmp/foo...
    ** finished generating datanode dir 1 in /tmp/foo...
    ** generating datanode dir 2 in /tmp/foo...
    ** finished generating datanode dir 2 in /tmp/foo...
    ** generating datanode dir 3 in /tmp/foo...
    ** finished generating datanode dir 3 in /tmp/foo...
    ** generating datanode dir 4 in /tmp/foo...
    ** finished generating datanode dir 4 in /tmp/foo...
    ** Done.

    # Convert the fsimage XML into a binary fsimage
    $ hdfs oiv -i /tmp/foo/fsimage_0000000000000000001.xml -o /tmp/foo/name/current/fsimage_0000000000000000001 -p ReverseXML

    # Copy the HDFS generated name directory into place
    rsync -avi --delete /tmp/foo/name/current/ /r/name1/current/

    # Copy the HDFS generated data directories into place
    rsync -avi --delete /tmp/foo/data1/current/ datanode1:/r/data1/current/
    rsync -avi --delete /tmp/foo/data1/current/ datanode2:/r/data1/current/
    rsync -avi --delete /tmp/foo/data1/current/ datanode3:/r/data1/current/
    rsync -avi --delete /tmp/foo/data1/current/ datanode4:/r/data1/current/
    rsync -avi --delete /tmp/foo/data1/current/ datanode5:/r/data1/current/
     
    # Start HDFS...

License
===============================================================================
fsgen is licensed under the Apache License 2.0.  See LICENSE.txt for details.

Colin P. McCabe
cmccabe@apache.org
