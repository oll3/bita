### Should

 * Allow for fixed block size while chunking.

 * Allow to use archive files as seed, and understand that it is an archive.

### Probably

 * On unpack - Add optional flag to avoid writing destination if the unpacked chunk and the destination data is exactly the same. This as the write speed on some block devices is alot lower than the read speed. And to avoid unnecessary block wear when writing large devices/partitions.


### Maybe

 * Generate a chunk dictionary pointing into the destination file while unpacking.
   This to allow to lookup old chunks without scanning on next unpack. For speed.

 * Add command 'fetch' which would download and rebuild the source archive locally.

   If this command should allow to generate a new archive based on local seeds the compressed chunk size might differ (like if we're on different application version than the source using a different compression etc).

   Hence the rebuilt archive header/dictionary might differ in size from the source.

   So to rebuild without creating temporary files (one for headear, one for chunks and concatanate them) we might need to reserve extra space in the header. Other solution or acceptable?

 * Implement chunk per file (casync style)