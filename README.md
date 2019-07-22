## bita
[![Build Status](https://travis-ci.org/oll3/bita.svg?branch=master)](https://travis-ci.org/oll3/bita)
[![](http://meritbadge.herokuapp.com/bita)](https://crates.io/crates/bita)

bita is a tool aiming for fast and low bandwidth file synchronization over http.

The file to synchronize could be any file where data is expected to change partially or completely between updates.
Any local file that might contain data of the source file may be used as seed while cloning.

In a software update system (IoT device/embedded system) with an A/B partition setup bita can be used to update the B partition while using the A partition as seed. The result written to the B parition will be an exact clone of the remote file but the only data fetched from remote is the data which actually differ between the A partition and the remote file.

---

On compression the input is scanned for chunk boundaries using a rolling hash. With the default setting a suitable boundary should be found every ~64 KiB.
Each found chunk is assigned a strong hash using blake2. If the chunk is unique it will be compressed and inserted into the final archive.
A dictionary which describes all chunks by their strong hash and the order for how to rebuild the input file is also attached to the archive.

On clone the dictionary is first fetched from the remote archive. Then bita scans all the given seed files for chunks which are present in the dictionary.
Any matching chunk found in a seed will be inserted into the output file.
When all seeds has been consumed the chunks still missing is fetched from the remote archive, then unpacked and inserted into the output file.

Each chunk of data is verified by the strong hash before written to the output.

---

### Installing from crates.io
```console
olle@home:~$ cargo install bita
```

### Building from source
```console
olle@home:~$ cargo build
```
#### Building in release mode
```console
olle@home:~$ cargo build --release
```

### Running tests
```console
olle@home:~$ cargo test
```


### Example usage

#### Compress

Create a compressed archive file.cba from stdin stream:

```console
olle@host:~$ gunzip -c file.gz | bita compress --compression ZSTD --compression-level 9 file.cba
```

Create an compressed archive file.ext4.cba from file file.ext4:

```console
olle@host:~$ bita compress file.ext4 file.ext4.cba
```

#### Clone

Clone file at http://host/new.tar.cba using seed another_old.tar and stdin (-):

```console
olle@device:~$ gunzip -c old.tar.gz | bita clone --seed another_old.tar --seed - http://host/new.tar.cba new.tar
```

Clone using block device /dev/mmcblk0p1 as seed and /dev/mmcblk0p2 as target:

```console
olle@device:~$ bita clone --seed /dev/mmcblk0p1 http://host/file.ext4.cba /dev/mmcblk0p2
```


### Similar Tools
* [casync](https://github.com/systemd/casync)
* [zchunk](https://github.com/zchunk/zchunk)
* [zsync](http://zsync.moria.org.uk)
* [rsync](https://rsync.samba.org/)
