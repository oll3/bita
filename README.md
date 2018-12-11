## What

Tool for doing differential file updates of over http.

Aiming for low bandwidth, low or no extra disc usage and fast decompression.

The file to update could be any file where data is expected to only change partially between updates. Any local file that might contain data of the source file may be used as seed while unpacking.

In an update system with a A/B partition setup one could use bita to update the B partition while using the A partition as seed. This should result in a download only of the chunks that differ between partition A and the source file.


### Similar Tools
* [casync](https://github.com/systemd/casync)
* [zchunk](https://github.com/zchunk/zchunk)
* [zsync](http://zsync.moria.org.uk)
* [rsync](https://rsync.samba.org/)


## Building
`$ cargo build` or `$ cargo build --release`

## Example usage

#### Compress a stream
`olle@host:~$ gunzip -c file.gz | bita compress file.cba`

#### Compress a file
`olle@host:~$ bita compress file.ext4 file.ext4.cba`

#### Decompress using multiple seeds
`olle@device:~$ gunzip -c old.tar.gz | bita unpack --seed another_old.tar http://host/file.cba file`

#### Decompress using block device as seed and target
`olle@device:~$ bita unpack --seed /dev/disk/by-partlabel/rootfs-A http://host/file.ext4.cba /dev/disk/by-partlabel/rootfs-B`
