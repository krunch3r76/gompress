# gompress
compress a file over distributed golem nodes

data is getting big. fortunately, this means more time on the clock waiting for the data to be prepared for sharing with colleagues around the world and getting paid while dreaming of an escape to a tropical island. besides this problem, gompress solves the compression problem of needing to divide the work of the xz compression algorithm to more cores than would normally be possible (i.e. an unlimited number of cores limited only by aggregate number of threads on the golem network).

# video demo

https://user-images.githubusercontent.com/46289600/164893401-08b878db-b068-4925-bb3d-49a0b099cc28.mp4

# MOA
gompress partitions/divides a file into separate parts sending them to golem nodes, where xz is invoked to compress the partitions using the maximum number of cores available. it is parallelism on two levels: one, the order in which nodes finish is not determined, and two, all cores are used on each node in parallel. the parts are asynchronously retrieved and stitched together into a cohesive whole that can be decompressed via xz.

the partitions ranges are tabulated as well as all intermediate work along with checksums. this enables resuming a compression later, as when network conditions or prices may be more favorable.

# USAGE
## adjust the maximum number of workers by changing the number of divisions:
```bash
$ python3 gompress.py --target myfile.raw --divisions 5
```

## improve compression rate by adjusting the xz compression preset level 
```bash
$ python3 gompress.py --target myfile.raw --compression 9e
```

## comments
testnet nodes are not high caliber. to get extreme compression on extreme sizes consider being selective of high performance nodes on the mainnet. you may find such nodes via my gc__listoffers application [1]. you may also incorporate my gc__filterms (will be added to this later) [2].

## todo
project memory requirements to better anticipate node requirements.
