#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"
// #include "lock_client_cache.h"

using namespace std;

void NameNode::init(const string &extent_dst, const string &lock_dst) {
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  yfs = new yfs_client(extent_dst, lock_dst);

  /* Add your init logic here */
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
  lc->acquire(ino);
  std::list<blockid_t> blockids;
  ec->get_block_ids(ino, blockids);
  extent_protocol::attr a;
  r = ec->getattr(ino, a);
  if (r != extent_protocol::OK) {
    fprintf(stdout, "[ Error ] 2003: get file attr error\n");
    assert(0);
  }
  list<LocatedBlock> lbs = list<LocatedBlock>();
  bids2lbs(blockids, lbs, a.size);
  lc->release(ino);
  return lbs;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
  int r;
  lc->acquire(ino);
  r = ec->complete(ino, new_size);
  if (r == extent_protocol::OK) {
    lc->release(ino);
    return true;
  }
  lc->release(ino);
  return false;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
  int r;
  lc->acquire(ino);
  extent_protocol::attr a;
  r = ec->getattr(ino, a);
  blockid_t blks = (a.size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  blockid_t bid;
  r = ec->append_block(ino, bid);
  if (r != extent_protocol::OK) {
    lc->release(ino);
    throw HdfsException("append block error");
  }
  return LocatedBlock(bid, blks * BLOCK_SIZE, 0, master_datanode);
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
  // TODO: file already exists, recover src_dir
  int r;
  std::string buf;
  std::list<yfs_client::dirent> dir_list;
  // remove directory entry from source directory
  lc->acquire(src_dir_ino);
  ec->get(src_dir_ino, buf);
  yfs_client::deDir(buf, dir_list);
  yfs_client::inum mv_ino;
  r = yfs_client::rmdirentry(dir_list, src_name, mv_ino);
  if (r != yfs_client::OK) {
    fprintf(stdout, "[ Error ] 2001: File %s not exist\n", src_name.c_str());
    lc->release(src_dir_ino);
    return false;
  }
  yfs_client::enDir(dir_list, buf);
  ec->put(src_dir_ino, buf);
  lc->release(src_dir_ino);
  // add a directory entry under dest directory
  lc->acquire(dst_dir_ino);
  ec->get(dst_dir_ino, buf);
  yfs_client::deDir(buf, dir_list);
  r = yfs->adddirentry(dir_list, dst_name, mv_ino);
  if (r != yfs_client::OK) {
    fprintf(stdout, "[ Error ] 2002: rename error\n");
    return false;
  }
  yfs_client::enDir(dir_list, buf);
  ec->put(dst_dir_ino, buf);
  lc->release(dst_dir_ino);
  return true;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  int r = yfs->mkdir(parent, name.c_str(), mode, ino_out);
  if (r == yfs_client::OK) return true;
  return false;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  int r = yfs->create(parent, name.c_str(), mode, ino_out);
  if (r == yfs_client::OK) return true;
  return false;
}

bool NameNode::Isfile(yfs_client::inum ino) {
  return yfs->isfile(ino);
}

bool NameNode::Isdir(yfs_client::inum ino) {
  return yfs->isdir(ino);
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
  int r = yfs->getfile(ino, info);
  if (r == yfs_client::OK) return true;
  return false;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
  int r = yfs->getdir(ino, info);
  if (r == yfs_client::OK) return true;
  return false;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
  int r = yfs->readdir(ino, dir);
  if (r == yfs_client::OK) return true;
  return false;
}

// TODO: what does ino mean
bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
  int r = yfs->unlink(ino, name.c_str());
  if (r == yfs_client::OK) return true;
  return false;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
  return list<DatanodeIDProto>();
}
