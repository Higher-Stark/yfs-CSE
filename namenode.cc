#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"

using namespace std;

#define _DEBUG_ 1

void NameNode::init(const string &extent_dst, const string &lock_dst) {
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  yfs = new yfs_client(extent_dst, lock_dst);

  /* Add your init logic here */
  if (ec->put(1, "") != extent_protocol::OK) {
    printf("error init root dir\n"); // XYB: init root dir
    fflush(stdout);
  }
  #if _DEBUG_
  fprintf(stdout, "[ Info ] init root OK\n");
  fflush(stdout);
  #endif
}

void NameNode::bids2lbs(const std::list<blockid_t> list, std::list<LocatedBlock>& lbs, const unsigned long long size)
{
  unsigned long long rest = size;
  size_t i = 0;
  for (std::list<blockid_t>::const_iterator it = list.cbegin(); it != list.cend(); it++, i++) {
    LocatedBlock l(*it, i*BLOCK_SIZE, MIN(BLOCK_SIZE, rest), master_datanode);
    rest -= MIN(BLOCK_SIZE, rest);
    lbs.push_back(l);
  }
  return;
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
  int r;
  std::list<blockid_t> blockids;
  ec->get_block_ids(ino, blockids);
  extent_protocol::attr a;
  r = ec->getattr(ino, a);
  if (r != extent_protocol::OK) {
    #if _DEBUG_
    fprintf(stdout, "[ Error ] 2003: get file attr error\n");
    fflush(stdout);
    #endif
    assert(0);
  }
  list<LocatedBlock> lbs = list<LocatedBlock>();
  bids2lbs(blockids, lbs, a.size);
  return lbs;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
  int r;
  r = ec->complete(ino, new_size);
  if (r == extent_protocol::OK) {
    lc->release(ino);
    return true;
  }
  return false;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
  int r;
  extent_protocol::attr a;
  r = ec->getattr(ino, a);
  // blockid_t blks = (a.size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  blockid_t bid;
  r = ec->append_block(ino, bid);
  if (r != extent_protocol::OK) {
    throw HdfsException("append block error");
  }
  uint64_t bSize = a.size % BLOCK_SIZE;
  if (bSize == 0) bSize = BLOCK_SIZE;
  return LocatedBlock(bid, a.size, bSize, master_datanode);
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
  // TODO: file already exists, recover src_dir
  #if _DEBUG_
  fprintf(stdout, "[ Info ] Rename: \n");
  fflush(stdout);
  #endif
  int r;
  std::string buf;
  std::list<yfs_client::dirent> dir_list;
  // remove directory entry from source directory
  ec->get(src_dir_ino, buf);
  yfs_client::deDir(buf, dir_list);
  yfs_client::inum mv_ino;
  r = yfs_client::rmdirentry(dir_list, src_name, mv_ino);
  if (r != yfs_client::OK) {
    #if _DEBUG_
    fprintf(stdout, "[ Error ] 2001: File %s not exist\n", src_name.c_str());
    fflush(stdout);
    #endif
    return false;
  }
  yfs_client::enDir(dir_list, buf);
  ec->put(src_dir_ino, buf);
  // add a directory entry under dest directory
  dir_list.clear();
  ec->get(dst_dir_ino, buf);
  yfs_client::deDir(buf, dir_list);
  r = yfs->adddirentry(dir_list, dst_name, mv_ino);
  if (r != yfs_client::OK) {
    #if _DEBUG_
    fprintf(stdout, "[ Error ] 2002: rename error\n");
    fflush(stdout);
    #endif
    return false;
  }
  yfs_client::enDir(dir_list, buf);
  ec->put(dst_dir_ino, buf);
  return true;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  int r = yfs->mkdir(parent, name.c_str(), mode, ino_out);
  if (r == yfs_client::OK) return true;
  return false;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  int r = yfs->create(parent, name.c_str(), mode, ino_out);
  if (r == yfs_client::OK){
    lc->acquire(ino_out);
    return true;
  } 
  return false;
}

bool NameNode::Isfile(yfs_client::inum ino) {
  extent_protocol::attr a;

  if (ec->getattr(ino, a) != extent_protocol::OK)
  {
    printf("error getting attr\n");
    return false;
  }

  if (a.type == extent_protocol::T_FILE)
  {
    printf("isfile: %lld is a file\n", ino);
    return true;
  }
  printf("isfile: %lld is not a file\n", ino);
  return false;
}

bool NameNode::Isdir(yfs_client::inum ino) {
    extent_protocol::attr a;

    if (ec->getattr(ino, a) != extent_protocol::OK) {
        printf("error get attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", ino);
        return true;
    }
    printf("isdir: %lld is not a dir\n", ino);
    return false;
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
  #if _DEBUG_
  printf("getfile %016llx\n", ino);
  #endif
  extent_protocol::attr a;
  if (ec->getattr(ino, a) != extent_protocol::OK)
  {
    return false;
  }

  info.atime = a.atime;
  info.mtime = a.mtime;
  info.ctime = a.ctime;
  info.size = a.size;
  #if _DEBUG_
  printf("getfile %016llx -> sz %llu\n", ino, info.size);
  fflush(stdout);
  #endif
  return true;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
  printf("getdir %016llx\n", ino);
  extent_protocol::attr a;
  if (ec->getattr(ino, a) != extent_protocol::OK)
  {
    return false;
  }
  info.atime = a.atime;
  info.mtime = a.mtime;
  info.ctime = a.ctime;
  return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
  int r = yfs->readdir_nolock(ino, dir);
  #if _DEBUG_
  for (std::list<yfs_client::dirent>::iterator it = dir.begin(); it != dir.end(); it++) {
    fprintf(stdout, "entry name: %s, inode: %lld\n", it->name.c_str(), it->inum);
  }
  fflush(stdout);
  #endif
  if (r == yfs_client::OK) return true;
  return false;
}

// TODO: what does ino mean
bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
  #if _DEBUG_
  printf("[ Info ] Unlink: name: %s; inum: %lld\n", name.c_str(), ino);
  fflush(stdout);
  #endif

  std::list<yfs_client::dirent> entries;
  yfs->readdir_nolock(parent, entries);

  bool found = false;
  std::list<yfs_client::dirent>::iterator target = entries.end();
  for (std::list<yfs_client::dirent>::iterator it = entries.begin(); it != entries.end(); it++)
  {
    if (it->name.compare(name) == 0)
    {
      found = true;
      target = it;
      break;
    }
  }

  if (!found && target == entries.end())
  {
    #if _DEBUG_
    fprintf(stderr, "[ Error ] 2003: file %s does not exist\n", name.c_str());
    fflush(stderr);
    #endif
    return false;
  }
  else
  {
    yfs_client::inum tinum = target->inum;
    entries.erase(target);
    ec->remove(tinum);
    std::string content;
    yfs_client::enDir(entries, content);
    ec->put(parent, content);
  }
  return true;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
  return list<DatanodeIDProto>();
}
