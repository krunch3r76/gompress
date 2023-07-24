# gompress
compress or archive files over distributed golem nodes

gompress solves the problem of needing to compress files when doing so on the requestor side would be prohibitively time consuming (e.g. on a small virtual server).

at last, you can stop daydreaming on company time while waiting for a compression to finish on your tiny virtual server and get back to work. get excited!

when gompress is given a directory or multiple files for the target, it will first prepare a tar file before compressing

curious how golem's nodes can work for you? visit https://golem.network to learn

## REQUIREMENTS
- yapapi 0.10.0
- a golem requestor installation with yagna client running and app-key exported etc
- python 3.9-3.10

tested on linux, windows 10, and mac os

## VIDEO DEMO

https://user-images.githubusercontent.com/46289600/166870523-9f7c6ca2-536d-4c0e-927b-486fdce9c240.mp4


https://user-images.githubusercontent.com/46289600/169424458-81c15e0b-811b-4d0f-8982-68893e109c74.mp4


## SELF DEMO
NOTE: This self demo uses tGLM nodes on public, which tend to be better compatible with non-development versions.
```bash
() $ cd gompress
(gompress) $ git checkout v0.2.1
(gompress) $ wget http://aleph.gutenberg.org/ls-lR # alternatively, download with your browser
(gompress) $ md5sum ls-lR # note this for later
(gompress) $ yagna payment init --sender --network rinkeby
(gompress) $ python3.9 gompress.py ls-lR --subnet-tag public --network rinkeby
(gompress) $ cd workdir/hashvaluefromoutput/final
(gompress final) $ xz -d ls-lR.xz
(gompress final) $ md5sum ls-lR # compare with earlier
```

## ABOUT XZ
xz is a free, well supported, highly effective compressor distributed with virtually every linux distribution. binaries are available to download from the project's web page. additionally, xz can be compiled on Mac OS X with gcc installed (instructions below)

homepage: https://tukaani.org/xz

windows binary (check for latest): https://tukaani.org/xz/xz-5.2.5-windows.zip

# compile xz for use on mac os x command line
```bash
$ tar -xf xz-5.2.5.tar.bz2
$ cd xz-5.2.5
(xz-5.2.5) $ ./configure --prefix=$HOME/.local
(xz-5.2.5) $ make && make install
```

## MOA
gompress partitions/divides a file into measures of 64MiB, sending them to golem nodes, where xz is invoked to compress the partitions. the parts are asynchronously retrieved and stitched together into a cohesive whole that can be decompressed via xz.

the partition ranges are tabulated and all intermediate work retained. *this enables resuming a compression later*, as when network conditions or prices may be more favorable. **TRY IT on a file >64MiB by ctrl-c after at least one task has finished and resume**

## ABOUT ARCHIVING
gompress will tar an input directory and all of its contents, otherwise if multiple files are given, it will change directory to the shared common root of all targets. in the latter case, if all the target files are in the same subdirectory, the tar file will change directory so that upon decompression the files are extracted to the working directory.

## ADVANCED USAGE

### ask gompress to perform light local compression first to save on file transfer (xfer), i.e. upload time, significantly via --xfer-compression-level

```bash
$ python3.9 ./gompress.py --network polygon --subnet-tag public  --xfer-compression-level 1 myfile.raw
```

### clone gc__filterms into the project root directory
#### it just works -- use the environment variables.
```bash
$ export GNPROVIDER_BL=fascinated-system
$ export FILTERMSVERBOSE=1
$ python3.9 ./gompress.py myfilelarge.raw --network polygon --subnet-tag public
```

## COMMENTS
to get extreme compression on extreme sizes consider being selective of high performance nodes on the mainnet. you may find such nodes via my gc__listoffers application [1]. you may also incorporate my gc__filterms by cloning it or linking from it from the project root directory [2].

expect gompress to evolve with golem and to become more performant accordingly e.g. with improved networking. gompress is continually being optimized within current parameters however. stay tuned.

--min-cpu-threads currently might guide to more modern cpu's but will not improve timing nor compression ratio as the work is optimally divided to leverage single cores (regardless of the actual number on a provider) on the golem network. partitioning to several nodes as opposed to one with several cores is more efficient since upload streams can be simultaneous, instead of having to wait on a single stream before starting.

for very large files, utilizing 2 cores may help and this will be a future optimization but is not relevant atm because 1 core is optimized for ≤ 64 MiB pieces (current division model).
