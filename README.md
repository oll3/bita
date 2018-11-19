
## Compress

### Compress stream
`$ gunzip -c file.tar.gz | bita compress file.cba`

### Compress file
`$ bita compress file.ext4 file.cba`

## Unpack
### Using multiple seeds, writing to a block device
`$ bita unpack --seed old.tar --seed old.cba http://some.url/new.cba /dev/mmcblk0p1`

### Using a streamed file and a block device as seed
`$ gunzip -c old.tar.gz | bita unpack --seed /dev/mmcblk0p2 http://file.cba /dev/mmcblk0p1`


## Download
`$ bita fetch --seed old.cba http://some.url/new.cba new.cba`
