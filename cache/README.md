# pgAs - cache using the UNLOGGED tables which optimizes high write queries by comprisomizing on WAL
 

 - UNLOGGED tables does not go through the WAL and flushes the data to disk periodically 
 this gives a great write op/sec 

 -- the issue is with PG uses MVCC and with every update on the key 
   a new version of same row is inserted and if the op is high it creates
   a lot ( a lot ) of dead tuple leading to the db bloat but for cache we can 
   trade off speed for this   