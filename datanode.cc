#include "datanode.h"
#include <arpa/inet.h>
#include "extent_client.h"
#include <unistd.h>
#include <algorithm>
#include "threader.h"

using namespace std;

#ifndef _DEBUG_
#define _DEBUG_ 1
#endif

int DataNode::init(const string &extent_dst, const string &namenode, const struct sockaddr_in *bindaddr) {
  ec = new extent_client(extent_dst);

  // Generate ID based on listen address
  id.set_ipaddr(inet_ntoa(bindaddr->sin_addr));
  id.set_hostname(GetHostname());
  id.set_datanodeuuid(GenerateUUID());
  id.set_xferport(ntohs(bindaddr->sin_port));
  id.set_infoport(0);
  id.set_ipcport(0);

  // Save namenode address and connect
  make_sockaddr(namenode.c_str(), &namenode_addr);
  if (!ConnectToNN()) {
    delete ec;
    ec = NULL;
    return -1;
  }

  // Register on namenode
  if (!RegisterOnNamenode()) {
    delete ec;
    ec = NULL;
    close(namenode_conn);
    namenode_conn = -1;
    return -1;
  }

  /* Add your initialization here */

  if (ec->put(1, "") != extent_protocol::OK) {
    fprintf(stdout, "[ Error ]: 1000: init root failed\n");
    fflush(stdout);
    return -1;
  }
  #if _DEBUG_
  fprintf(stdout, "[ Info ] root initialized\n");
  #endif

  // send heartbeat
  NewThread(this, &DataNode::beat);
  return 0;
}

void DataNode::beat()
{
  int delay = 1;
  while (true) {
    if (SendHeartbeat()) {
      delay = 1;
      sleep(delay);
    }
    else {
      sleep(delay);
      delay *= 2;
    }
  }
}

/*
 * function: ReadBlock
 * description: read len bytes data start at offset from bid block's data and return
 * @bid: block id
 * @offset: start postion of block data
 * @len: data length
 * @buf: data read from the block
 */
bool DataNode::ReadBlock(blockid_t bid, uint64_t offset, uint64_t len, string &buf) {
  /* Your lab4 part 2 code */
  // TODO:
  #if _DEBUG_
  fprintf(stdout, "[ Info ] Read block %d [%ld:%ld]\n", bid, offset, len);
  fflush(stdout);
  #endif

  string tmpbuf;
  int ret = ec->read_block(bid, tmpbuf);
  if (ret != extent_protocol::OK) {
    #if _DEBUG_
    fprintf(stdout, "[ Error ] 1002: read block %d error\n", bid);
    fflush(stdout);
    #endif
    return false;
  }

  if (offset > tmpbuf.size()) {
    fprintf(stdout, "[ Warning ] 1001: out of string boundary\n");
    buf = "";
  }
  else buf = tmpbuf.substr(offset, len);
  return true;
}

/*
 * function: WriteBlock
 * description: write len bytes start at offset bytes in the block with first len bytes
 * of buf, rest remains the same
 */
bool DataNode::WriteBlock(blockid_t bid, uint64_t offset, uint64_t len, const string &buf) {
  /* Your lab4 part 2 code */
  // TODO:
  #if _DEBUG_
  fprintf(stdout, "[ Info ] Write Block %d, offset: %ld, len: %ld, buf size: %ld\n", bid, offset, len, buf.size());
  fflush(stdout);
  #endif

  string oldbuf;
  int ret = ec->read_block(bid, oldbuf);
  if (ret != extent_protocol::OK) {
    #if _DEBUG_
    fprintf(stdout, "[ Error ] 1003: read block %d error\n", bid);
    fflush(stdout);
    #endif
    return false;
  }
  /*#if _DEBUG_
  fprintf(stdout, "[ Info ] old content of block %d: %s\n", bid, oldbuf.c_str());
  fflush(stdout);
  #endif*/
  string newbuf = oldbuf.substr(0, offset) + buf + oldbuf.substr(offset + len);
  /*#if _DEBUG_
  fprintf(stdout, "[ Info ] new content of block %d: %s\n", bid, newbuf.c_str());
  fflush(stdout);
  #endif*/
  ret = ec->write_block(bid, newbuf);
  if (ret != extent_protocol::OK) {
    #if _DEBUG_
    fprintf(stdout, "[ Error ] 1004: write block %d error\n", bid);
    fflush(stdout);
    #endif
    return false;
  }
  return true;
}

