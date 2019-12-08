[![Build Status](https://travis-ci.org/oll3/bita.svg?branch=master)](https://travis-ci.org/oll3/bita)
[![](http://meritbadge.herokuapp.com/bita)](https://crates.io/crates/bita)

## bita

bita strives to make file download less bandwidth heavy by reusing local data when available.

A typical case where bita may provide a significant reduction in bandwidth is OTA software upgrade of IoT/embedded devices. Where we expect that a new release image may contain a lot of data already present on the device being upgraded.

bita provides a simple way to only download the data which actual differ between the new release and the release running on the device, still ensuring that the result is an exact clone of the new release image. In an embedded Linux system with dual (A/B) partition setup we would use the running partition as seed (a file which might contain data to reuse) while writing the new release to the other inactive partition.

No need prepare delta upgrade files for going to/from different releases. No need to run you own release file server.
Just `bita compress` the release image, upload the archive to any regular http hosting site. And `bita clone` the archive using whatever local data is available.


## How it works

![concept](images/concept.png?raw=true)
---

On compression the input file is scanned for chunk boundaries using a rolling hash. With the default setting a suitable boundary should be found every ~64 KiB. A chunk is defined as the data contained between two boundaries. For each chunk a strong hash, using blake2, is generated.
The chunk location (offset and size) in the input file and the strong hash is then stored in the dictionary. If a chunk is unique it's also compressed and inserted into the output archive.

The final archive will contain a dictionary describing the order of chunks in the input file and the compressed chunks necessary to rebuild the input file. The archive will also contain the configuration used when scanning input for chunks.

---
On clone the dictionary and scan configuration is first fetched from the remote archive. Then the given seed file(s) are scanned for chunks present in the dictionary. Scanning is done using the same configuration as when building the archive.
Any chunk found in a seed file will be copied into the output file at the location(s) specified by the dictionary.
When all seeds has been consumed the chunks still missing, if any, is fetched from the remote archive, decompressed and inserted into the output file.

Each chunk, both fetched from seed and from archive, is verified by its strong hash before written to the output.

---
The server serving bita archives can be any http(s) server supporting [range requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests "range requests"), which should be most.


## Install from crates.io
```console
olle@home:~$ cargo install bita
```

## Example usage

Create a compressed archive `file.ext4.cba` from file `file.ext4`:

```console
olle@home:~$ bita compress -i file.ext4 file.ext4.cba
```

Clone file at `https://host/new.tar.cba` using stdin (-) and `another_old.tar` as seed:

```console
olle@home:~$ gunzip -c old.tar.gz | bita clone --seed another_old.tar --seed - https://host/new.tar.cba new.tar
```

Clone using block device `/dev/mmcblk0p1` as seed and `/dev/mmcblk0p2` as target:

```console
upgrader@device:~$ bita clone --seed /dev/mmcblk0p1 https://host/file.ext4.cba /dev/mmcblk0p2
```

## Similar Tools
* [casync](https://github.com/systemd/casync)
* [zchunk](https://github.com/zchunk/zchunk)
* [zsync](http://zsync.moria.org.uk)
* [rsync](https://rsync.samba.org/)
