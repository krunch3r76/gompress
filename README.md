# gompress
compress a file over distributed golem nodes

gompress solves the problem of needing to compress a file (such as an archive file) when doing so on the requestor side would be prohibitively time consuming (e.g. on a small virtual server).

at last, you can stop daydreaming on company time while waiting for a compression to finish on your tiny virtual server and get back to work. get excited!

currently, gompress compresses a single file (which itself may be an uncompressed archive) but collecting and archiving multiple files is a solution being explored. stay tuned.

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
(gompress) $ python3.9 gompress.py ls-lR
(gompress) $ cd workdir/hashvaluefromoutput/final
(gompress final) $ xz -d ls-lR.xz
(gompress final) $ md5sum ls-lR # compare with earlier
```

**note**: xz is free for windows, download directly or visit root website at: https://tukaani.org/xz/xz-5.2.5-windows.zip

# MOA
gompress partitions/divides a file into measures of 64MiB, sending them to golem nodes, where xz is invoked to compress the partitions. the parts are asynchronously retrieved and stitched together into a cohesive whole that can be decompressed via xz.

the partition ranges are tabulated as well as all intermediate work along with checksums. *this enables resuming a compression later*, as when network conditions or prices may be more favorable. **TRY IT on a file >64MiB by ctrl-c after at least one task has finished and resume**

# ADVANCED USAGE

## ask gompress to perform light local compression first to save on file transfer (xfer) time significantly via --xfer-compression-level

```bash
$ python3.9 ./gompress.py --payment-network polygon --subnet-tag public-beta myfile.raw --xfer-compression-level 1
```

## clone gc__filterms into the project root directory
### it just works -- use the environment variables.
```bash
$ export GNPROVIDER_BL=fascinated-system
$ export FILTERMSVERBOSE=1
$ python3.9 ./gompress.py myfilelarge.raw --payment-network polygon --subnet-tag public-beta
```

## comments
testnet nodes are not high caliber. to get extreme compression on extreme sizes consider being selective of high performance nodes on the mainnet. you may set --min-cpu-threads to a number of threads where you might expect a powerful cpu. alternatively, you may find such nodes via my gc__listoffers application [1]. you may also incorporate my gc__filterms by cloning it or linking from it from the project root directory [2].

expect gompress to evolve with golem and to become more performant accordingly e.g. with improved networking. gompress is continually being optimized within current parameters however. stay tuned.

--min-cpu-threads currently would guide to more modern cpu's but should not improve timing and compression ratio as the work is optimally divided to leverage single cores (regardless of the actual number on a provider) on the golem network. the rationale is primarily that uploads are not subject to a single end-point's bandwidth, as uploads occur as several streams simultaneously in the background. this transitively applies to downloads.

for very large files, utilizing 2 cores may help and this will be a future optimization but is not relevant atm

## todo
project memory requirements to better anticipate node requirements.
