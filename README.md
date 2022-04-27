# gompress
compress a file over distributed golem nodes

data is getting big. fortunately, this means more time on the clock waiting for the data to be prepared for sharing with colleagues around the world and getting paid while dreaming of an escape to a tropical island. besides this problem, gompress solves the compression problem of needing to divide the work of the xz compression algorithm to more cores than would normally be possible (i.e. an unlimited number of cores limited only by aggregate number of threads on the golem network).

# requirements
- yapapi 0.9.1
- a golem requestor installation with yagna client running and app-key exported etc
- python 3.8-3.9

tested on linux and windows. expected to work cross platform.

# video demo

https://user-images.githubusercontent.com/46289600/164893401-08b878db-b068-4925-bb3d-49a0b099cc28.mp4

# MOA
gompress partitions/divides a file into separate parts sending them to golem nodes, where xz is invoked to compress the partitions using the maximum number of cores available. it is parallelism on two levels: one, the order in which nodes finish is not determined, and two, all cores are used on each node in parallel. the parts are asynchronously retrieved and stitched together into a cohesive whole that can be decompressed via xz.

the partitions ranges are tabulated as well as all intermediate work along with checksums. *this enables resuming a compression later*, as when network conditions or prices may be more favorable. **TRY IT**

# USAGE AND TIPS

## ask gompress to perform light local compression first to save on file transfer (xfer) time significantly via --xfer-compression-level
20 divisions with at least 20 cores would be appropriate for a large file e.g. +400mb on mainnet
```bash
$ python3.9 ./gompress.py --payment-network polygon --subnet-tag public-beta --target myfile.raw --divisions 20 --compression=9e --xfer-compression-level 3 --min-cpu-threads 20
```

## adjust the maximum number of workers by changing the number of divisions:
```bash
$ python3 gompress.py --target myfile.raw --divisions 5
```

## improve compression rate by adjusting the xz compression preset level 
```bash
$ python3 gompress.py --target myfile.raw --compression 9e
```
## use gompress as a benchmark
since work is more or less evenly divided, gompress log messages with respect to time is indicative of relative performance. make note of the fastest nodes and use them for future work e.g. with gc__filterms. currently, the best way to do this is normalize against the checksum, which is simply the length of the output file expected from each node. group by the task data value to map timing to a specific node name.

## clone gc__filterms into the project root directory
### it just works -- use the environment variables
```bash
$ export GNPROVIDER_BL=fascinated-system
$ export FILTERMSVERBOSE=1
$ python3.9 ./gompress.py --payment-network polygon --subnet-tag public-beta --target myfilelarge.raw --divisions 20 --compression=9e --xfer-compression-level 3 --min-cpu-threads 128
```

## comments
testnet nodes are not high caliber. to get extreme compression on extreme sizes consider being selective of high performance nodes on the mainnet. you may find such nodes via my gc__listoffers application [1]. you may also incorporate my gc__filterms by cloning it or linking from it from the project root directory [2].

expect gompress to evolve with golem and to become more performant accordingly e.g. with improved networking

## todo
project memory requirements to better anticipate node requirements.
heuristics heuristics heurisitcs
