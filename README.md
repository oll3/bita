
## Compress

### Compress a stream
`$ gunzip -c file.tar.gz | bita compress file.cba`

### Compress a file
`$ bita compress --input file.ext4 file.cba`

## Unpack
### Unpack using multiple seeds
`$ bita unpack --seed file.tar --seed /dev/mmcblk0p2 --seed file.cba http://file.cba > /dev/mmcblk0p1`
### Unpack using a streamed file and a block device as seed
`$ gunzip -c file.tar.gz | bita unpack --seed /dev/mmcblk0p2 http://file.cba > /dev/mmcblk0p1`

