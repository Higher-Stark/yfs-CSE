#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <queue>


#include <map>
#include <set>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"


class lock_server_cache {
 private:
  int nacquire;
  
  pthread_mutex_t lock;
  pthread_cond_t cond;

  /*
   * revoke: have server sent revoke request to client?
   * free: whether the lock is held by some client
   * holder: client that hold the lock
   * waitings: clients who are waiting for the lock
   */
  typedef struct lockitem_ {
    bool revoke;
    bool free;
    std::string holder;
    std::set<std::string> waitings;
  } lockitem;
  std::map<lock_protocol::lockid_t, lockitem> ltable;

 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
