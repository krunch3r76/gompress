# gompress
compress a file over distributed golem nodes

gompress solves the problem of needing to compress a file (such as an archive file) when doing so on the requestor side would be prohibitively time consuming (e.g. on a small virtual server).

at last, you can stop daydreaming on company time while waiting for a compression to finish on your tiny virtual server and get back to work. get excited!

currently, gompress compresses a single file but archiving multiple files is a solution being explored. stay tuned.

# requirements
- yapapi 0.9.1
- a golem requestor installation with yagna client running and app-key exported etc
- python 3.8-3.9

tested on linux and windows. expected to work cross platform.

# video demo

https://user-images.githubusercontent.com/46289600/164893401-08b878db-b068-4925-bb3d-49a0b099cc28.mp4

# self demo
```bash
(gompress) $ wget http://aleph.gutenberg.org/ls-lR # alternatively, download with your browser
(gompress) $ md5sum ls-lR # note this for later
(gompress) $ python3.9 gompress.py --target ls-lR
(gompress) $ cd workdir/hashvaluefromoutput/final
(gompress final) $ xz -d ls-lR.xz
(gompress final) $ md5sum ls-lR # compare with earlier
```

**note**: xz is free for windows, download directly or visit root website at: https://tukaani.org/xz/xz-5.2.5-windows.zip

# MOA
gompress partitions/divides a file into --divisions argument number of separate parts, sending them to golem nodes, where xz is invoked to compress the partitions using the maximum number of cores available. it is parallelism on two levels: one, the order in which nodes finish is not determined, and two, all cores are used on each node in parallel. the parts are asynchronously retrieved and stitched together into a cohesive whole that can be decompressed via xz.

the partitions ranges are tabulated as well as all intermediate work along with checksums. *this enables resuming a compression later*, as when network conditions or prices may be more favorable. **TRY IT by ctrl-c midway and resume**

# USAGE AND TIPS


## adjust the maximum number of workers by changing the number of divisions:
```bash
$ python3 gompress.py --target myfile.raw --divisions 5
```

## improve compression rate by adjusting the xz compression preset level 
```bash
$ python3 gompress.py --target myfile.raw --compression 9e
```

## ask gompress to perform light local compression first to save on file transfer (xfer) time significantly via --xfer-compression-level

```bash
$ python3.9 ./gompress.py --payment-network polygon --subnet-tag public-beta --target myfile.raw --divisions 10 --compression=9e --xfer-compression-level 1
```

## use gompress as a benchmark
since work is more or less evenly divided, gompress log messages with respect to time is indicative of relative performance. make note of the fastest nodes and use them for future work e.g. with gc__filterms. currently, the best way to do this is normalize against the checksum, which is simply the length of the output file expected from each node. group by the task data value to map timing to a specific node name.

## clone gc__filterms into the project root directory
### it just works -- use the environment variables. select a single node (or few) with many cores and set --divisions 1 (or few count)
```bash
$ export GNPROVIDER_BL=fascinated-system
$ export FILTERMSVERBOSE=1
$ python3.9 ./gompress.py --payment-network polygon --subnet-tag public-beta --target myfilelarge.raw --divisions 10 --compression=9e --xfer-compression-level 1
```

## comments
testnet nodes are not high caliber. to get extreme compression on extreme sizes consider being selective of high performance nodes on the mainnet. you may find such nodes via my gc__listoffers application [1]. you may also incorporate my gc__filterms by cloning it or linking from it from the project root directory [2].

expect gompress to evolve with golem and to become more performant accordingly e.g. with improved networking. gompress is continually being optimized within current parameters however. stay tuned.

the default min-cpu-threads argument is treated specially by gompress and is set to 0 by default: which will parallelize compression of each task across all threads on any worker node. i recommend one focus on the --divisions argument and if targeting higher end nodes to use gc__filterms before tweaking min-cpu-threads. the end result, however, of adjusting min-cpu-threads are: 1) only providers with at least that many threads are selected, and 2) the task work is divided in parallel on a single node using just that many threads (as opposed to all available). for more information about threaded compression in xz, see the manpage on the -T argument (in this case the default is -T0).

## todo
project memory requirements to better anticipate node requirements.
heuristics heuristics heurisitcs
may make a single high core node the default (until golem networking speeds are improved)
