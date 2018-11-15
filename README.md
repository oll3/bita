
Compress a stream:
`$ gunzip -c file.tar.gz | bita compress file.cba`

Compress a file:
`$ bita compress --input file.ext4 file.cba`

Unpack using multiple seeds:
`$ bita unpack --raw-seed file.tar --raw-seed /dev/mmcblk0p2 --cba-seed file.cba http://file.cba > /dev/mmcblk0p1`
Unpack using a streamed file and a block device as seed:
`$ gunzip -c file.tar.gz | bita unpack --raw-seed /dev/mmcblk0p2 http://file.cba > /dev/mmcblk0p1`

