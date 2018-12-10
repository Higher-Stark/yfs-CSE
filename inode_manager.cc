#include "inode_manager.h"
#include <ctime>

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */

  int minDataBlkId = 1 + 8 + INODE_NUM / IPB;
  for (int i = minDataBlkId + 1; i < BLOCK_NUM; i++) {
    if (using_blocks.count(i) == 0 || using_blocks[i] == 0) {
      using_blocks[i] = 1;
      
      // mark corresponding bit in bitmap as 1
      char block_buf[BLOCK_SIZE];
      read_block(BBLOCK(i), block_buf);
      block_buf[i / 8] |= 1 << ( i % 8 );
      write_block(BBLOCK(i), block_buf);
      return i;
    }
  }
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  using_blocks[id] = 0;
  // mark bitmap
  char bit_buf[BLOCK_SIZE];
  read_block( BBLOCK(id), bit_buf );
  uint32_t byte_offset = id / 8;
  short bit_offset = id % 8;
  bit_buf[byte_offset] = bit_buf[byte_offset] & (0xff ^ ( 1 << bit_offset));
  d->write_block( BBLOCK(id), bit_buf);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
// |1 blk | 4096 bytes -> 8 blk | 1024 / IPB    | 
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

  int blks_inode = INODE_NUM / IPB;
  // blocks to store super block, block bitmap, inode table
  int minDataBlkId = 1 + 8 + blks_inode;
  for (int i = 1; i <= minDataBlkId; i++) {
    using_blocks.insert(std::map<uint32_t, int>::value_type(i, i <= minDataBlkId));
  }
  // mark corresponding bit in bit map 1
  for (int i = 0; i < (minDataBlkId + 4095) / 4096; i++) {
    char block_buf[BLOCK_SIZE];
    memset(block_buf, 0, sizeof(block_buf));
    for (int j = 0; j < minDataBlkId - 4096 * i; i++) {
      int cid = j / 8;
      int bit_offset = j % 8;
      block_buf[cid] |= 1 << bit_offset;
    }
    write_block(i + 1, block_buf);
  }
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  for (uint32_t i = 1; i != INODE_NUM; i++) {
    fflush(stdout);
    if (!get_inode(i)) {
      struct inode ino;
      memset(&ino, 0, sizeof(struct inode));
      ino.type = type;
      std::time_t t = std::time(NULL);
      ino.atime = t;
      ino.mtime = t;
      ino.ctime = t;
      put_inode(i, &ino);
      return i;
    }
  }
  return -1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  struct inode* ino = get_inode(inum);
  if (!ino) return;
  int i = 0;
  int nblks = ( ino->size + BLOCK_SIZE - 1) /  BLOCK_SIZE;
  int tmp = nblks < NDIRECT ? nblks : NDIRECT;
  for (; i != tmp; i++) {
    bm->free_block(ino->blocks[i]);
  } 
  if (nblks > NDIRECT) {
    blockid_t bid = ino->blocks[NDIRECT]; 
    char block_buf[BLOCK_SIZE];
    bm->read_block(bid, block_buf);
    bm->free_block(bid);
    uint *sec_block = (uint *)block_buf;
    for (; i < nblks; i++) {
      bm->free_block(sec_block[i - NDIRECT]);
    }
  }
  memset(ino, 0, sizeof(struct inode));
  put_inode(inum, ino);
  free(ino);
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  struct inode *ino = get_inode(inum);
  if (!ino) {
    *buf_out = NULL;
    *size = 0;
    return;
  }
  
  // update access time
  std::time_t t = std::time(0);
  ino->atime = t;

  *size = ino->size;
  
  blockid_t i = 0;
  int rest = ino->size;
  int offset = 0;
  unsigned int nblks = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  char *file_buf = (char *)malloc(sizeof(char) * BLOCK_SIZE * nblks);
  while (rest > 0 && i < MIN(NDIRECT, nblks)) {
    // char block_buf[BLOCK_SIZE];
    bm->read_block(ino->blocks[i], file_buf + offset);
    // memcpy(file_buf + offset, block_buf, MIN(rest, BLOCK_SIZE));
    offset += BLOCK_SIZE;
    rest -= BLOCK_SIZE;
    i++;
  }
  if (nblks > NDIRECT && rest > 0) {
    
    // printf("\tim: read indirect index block, nblks: %d, rest: %d\n", nblks, rest);

    blockid_t indir_id = ino->blocks[NDIRECT];
    char block_buf[BLOCK_SIZE];
    bm->read_block(indir_id, block_buf);

    // traverse indirect block index 
    uint *indir_blk = (uint *)block_buf;
    for (unsigned int j = 0; j != nblks - NDIRECT; j++) {
      if (rest > 0 && indir_blk[j] > 0) {
        char buffer[BLOCK_SIZE];
        bm->read_block(indir_blk[j], buffer);
        // copy block content into file buffer
        memcpy(file_buf + offset, buffer, MIN(rest, BLOCK_SIZE));
        offset += MIN(rest, BLOCK_SIZE);
        rest -= MIN(rest, BLOCK_SIZE);
      }
      else break;
    }
  }
  *buf_out = file_buf;
  put_inode(inum, ino);
  free(ino);
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  struct inode *ino = get_inode(inum);
  if (!ino) {
    printf("Error: File not exists\n");
    return;
  }

  // update modify time
  std::time_t t;

  blockid_t *blocks = ino->blocks;
  unsigned int blks_old = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  unsigned int blks_new = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  // buf needs more blocks
  if (blks_new > blks_old) {
    // fill old blocks with new data
    int offset = 0;
    unsigned int i = 0;
    unsigned int maxIdx = MIN(blks_old, NDIRECT);
    for (; i != maxIdx; i++) {
      bm->write_block(blocks[i], buf + offset);
      offset += BLOCK_SIZE;
    }
    if (blks_old > NDIRECT) {
      char blockid_buf[BLOCK_SIZE];
      bm->read_block(blocks[NDIRECT], blockid_buf);
      blockid_t *indir_blks = (blockid_t *) blockid_buf;
      for (unsigned int j = 0; j != blks_old - NDIRECT; j++) {
        bm->write_block(indir_blks[j], buf + offset);
        offset += BLOCK_SIZE;
        i++;
      }
    }
    int rest = size - offset;

    // if the file is too large, leave the exceeding part alone
    while (rest > 0 && i < MAXFILE) {
      // allocate new block to store file content
      uint32_t bid = bm->alloc_block();
      // alloc a new block in memory to store buf temporary
      char *blk_tmp = (char *)malloc(BLOCK_SIZE);
      int write_count = MIN(rest, BLOCK_SIZE); 
      memcpy(blk_tmp, buf + offset, write_count);
      bm->write_block(bid, blk_tmp);
      offset += write_count;
      rest -= write_count;
      free(blk_tmp);

      // put block id into inode
      if (i < NDIRECT) {
        blocks[i] = bid;
      }
      // allocate new block for storing indirect index block
      else if (i == NDIRECT) {
        uint32_t idx_blk_id = bm->alloc_block();
        blocks[NDIRECT] = idx_blk_id;
        char idx_blk[BLOCK_SIZE];
        blockid_t *blkid = (blockid_t *)idx_blk;
        blkid[0] = bid;
        bm->write_block(idx_blk_id, idx_blk);
      }
      else {
        char blockid_buf[BLOCK_SIZE];
        bm->read_block(blocks[NDIRECT], blockid_buf);
        blockid_t *blkid = (blockid_t *)blockid_buf;
        blkid[i - NDIRECT] = bid;
        bm->write_block(blocks[NDIRECT], blockid_buf);
      }
      i++;
    }
  }
  // new file needs no more blocks than old file
  else {
    int offset = 0;
    int rest = size;
    unsigned int maxIdx = MIN(blks_new, NDIRECT);
    unsigned int i = 0;
    char *blk_tmp = (char *)malloc(BLOCK_SIZE);
    for (; i != maxIdx && rest > 0; i++) {
      memset(blk_tmp, 0, BLOCK_SIZE);
      int write_count = MIN(rest, BLOCK_SIZE);
      memcpy(blk_tmp, buf + offset, write_count);
      bm->write_block(blocks[i], blk_tmp);
      offset += write_count;
      rest -= write_count;
    }
    // file is large, using indirect indexing block
    if (rest > 0 && blks_new > NDIRECT) {
      char blockid_buf[BLOCK_SIZE];
      bm->read_block(blocks[NDIRECT], blockid_buf);
      blockid_t *indir_blks = (blockid_t *) blockid_buf;
      for (; i != blks_new && rest > 0; i++) {
        memset(blk_tmp, 0, BLOCK_SIZE);
        int write_count = MIN(rest, BLOCK_SIZE);
        memcpy(blk_tmp, buf + offset, write_count);
        bm->write_block(indir_blks[i - NDIRECT], blk_tmp);
        offset += write_count;
        rest -= write_count;
      }
      for (; i < blks_old; i++) {
        bm->free_block(indir_blks[i - NDIRECT]);
      }
    }
    // file is small, no need for indirect indexing block
    else {
      for (; i < MIN(NDIRECT, blks_old); i++) {
        bm->free_block(blocks[i]);
        blocks[i] = 0;
      }
      // if original file is large, free indirect block
      if (blks_old > NDIRECT) {
        char blockid_buf[BLOCK_SIZE];
        bm->read_block(blocks[NDIRECT], blockid_buf);
        blockid_t *indir_blks = (blockid_t *) blockid_buf;
        
        for (; i < blks_old; i++) {
          bm->free_block(indir_blks[i - NDIRECT]);
        }
        bm->free_block(blocks[i]);
        blocks[i] = 0;
      }
    }
    free(blk_tmp);
  }
  ino->size = size;
  t = std::time(NULL);
  ino->ctime = t;
  t = std::time(NULL);
  ino->mtime = t;

  put_inode(inum, ino);
  free(ino);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  struct inode *ino = get_inode(inum);
  if (!ino) {
    memset(&a, 0, sizeof(a));
    return;
  }
  a.type = ino->type;
  a.size = ino->size;
  a.atime = ino->atime;
  a.ctime = ino->ctime;
  a.mtime = ino->mtime;
  free(ino);
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  free_inode(inum);
  return;
}

/*
 * function: append_block
 * description: allocate and append a block to the inode and return its id
 * @inum: inode number
 * @bid: latest allocated block
 */
void
inode_manager::append_block(uint32_t inum, blockid_t &bid)
{
  struct inode* ino = get_inode(inum);
  if (!ino) {
    bid = -1;
    fprintf(stderr, "[ Error ] 0001: inode %d not exist\n", inum);
    return;
  }

  bid = bm->alloc_block();
  blockid_t last_blk = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE ;
  // file size is not expected to excceed the max size
  assert(last_blk < MAXFILE);

  bool update_inode = false;
  if (last_blk < NDIRECT - 1) {
    ino->blocks[last_blk] = bid;
    update_inode = true;
  }
  else if (last_blk == NDIRECT - 1) {
    blockid_t indir_blk = bm->alloc_block();
    ino->blocks[NDIRECT - 1] = indir_blk;
    update_inode = true;
    char indir_blk_buf[BLOCK_SIZE];
    memset(indir_blk_buf, 0, BLOCK_SIZE);
    blockid_t *indir_blk_list = (blockid_t *)indir_blk_buf;
    indir_blk_list[0] = bid;
    // write indirect index block
    bm->write_block(indir_blk, indir_blk_buf);
  }
  else {
    char indir_blk_buf[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT - 1], indir_blk_buf);
    blockid_t *indir_blk_list = (blockid_t *)indir_blk_buf;
    indir_blk_list[last_blk - NDIRECT] = bid;
    // update indirect index block
    bm->write_block(ino->blocks[NDIRECT - 1], indir_blk_buf);
  }
  // update inode if necessary
  // leaving mtime update to function complete
  ino->size += BLOCK_SIZE;
  put_inode(inum, ino);
  free(ino);
  return;
}

/* 
 * function: get_block_ids
 * description: return id of all data blocks of the file
 * @inum: inode number
 * @block_ids: all data blocks' id read from inode inum
 */
void
inode_manager::get_block_ids(uint32_t inum, std::list<blockid_t> &block_ids)
{
  struct inode *ino = get_inode(inum);
  if (!ino) {
    fprintf(stderr, "[ Error ] 0002: inode %d not exist\n", inum);
    block_ids = std::list<blockid_t>();
    return;
  }

  blockid_t num_blks = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  if (num_blks == 0) {
    block_ids = std::list<blockid_t>();
    free(ino);
    return;
  }
  if (num_blks < NDIRECT - 1) {
    block_ids = std::list<blockid_t>(ino->blocks, ino->blocks + num_blks);
  }
  else {
    char indir_blk_buf[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT - 1], indir_blk_buf);
    blockid_t blk_list[MAXFILE];
    memcpy(blk_list, ino->blocks, (NDIRECT - 1) * sizeof(blockid_t));
    memcpy(blk_list + NDIRECT - 1, indir_blk_buf, (num_blks - NDIRECT + 1) * sizeof(blockid_t));
    block_ids = std::list<blockid_t>(blk_list, blk_list + num_blks);
  }
  free(ino);
  return;
}

/*
 * function: read_block
 * description: return the data of the disk block
 * @id: block id
 * @buf: block id's content to be placed
 */
void
inode_manager::read_block(blockid_t id, char buf[BLOCK_SIZE])
{
  bm->read_block(id, buf);
}

/* 
 * function: write_block
 * description: write the data to the disk block
 * @id: block id
 * @buf: content to write
 */
void
inode_manager::write_block(blockid_t id, const char buf[BLOCK_SIZE])
{
  bm->write_block(id, buf);
}

/* 
 * function: complete
 * decription: Given an inode number and size, this request indicates 
 * that the writes to the file is finished, so the metedata (file size, 
 * modification time) in inode could be updated safely.
 * @inum: inode number
 * @size: inode latest size
 */
void
inode_manager::complete(uint32_t inum, uint32_t size)
{
  struct inode* ino = get_inode(inum);
  if (!ino) {
    fprintf(stderr, "[ Error ] 0003: inode %d not exists\n", inum);
    return;
  }

  ino->size = size;
  ino->mtime = std::time(NULL);
  put_inode(inum, ino);
  free(ino);
  return;
}
