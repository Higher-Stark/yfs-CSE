# Cause of extent_server crash

## inode_manager::write_file

Be cautious when write block with buf as an argument. When buf content size is less than `BLOCK_SIZE`, `memcpy(blocks[id], buf, BLOCK_SIZE)` will trigger segment fault.

_Solution_

prepare a empty block before write block, copy `MIN(rest, BLOCK_SIZE)` bytes into empty block, then write block with the 'empty block'.