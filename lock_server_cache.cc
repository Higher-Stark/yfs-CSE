// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{
  pthread_mutex_init(&lock, NULL);
  pthread_cond_init(&cond, NULL);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&lock);
  tprintf("<server acquire> %lld, %s\n", lid, id.c_str());
  // no one has acquired lock lid
  if (ltable.find(lid) == ltable.end()) {
    lockitem li;
    li.revoke = false;
    li.free = true;
    li.waitings = std::set<std::string>();
    ltable.insert(std::pair<lock_protocol::lockid_t, lockitem>(lid, li));
  }
  if (ltable[lid].free) {
    ltable[lid].holder = id;
    ltable[lid].free = false;
    ltable[lid].revoke = false;
    if (ltable[lid].waitings.find(id) != ltable[lid].waitings.end()) {
      ltable[lid].waitings.erase(id);
    }
  }
  else {
    // put the client to wait
    ltable[lid].waitings.insert(id);
    /*
     * if holder hasn't been revoked, revoke
     */
    if (!ltable[lid].revoke) {
      handle owner(ltable[lid].holder);
      rpcc *cl = owner.safebind();
      int r;
      ltable[lid].revoke = true;
      std::string tmp = ltable[lid].holder;
      pthread_mutex_unlock(&lock);
      ret = cl->call(rlock_protocol::revoke, lid, r);
      pthread_mutex_lock(&lock);
      if (ret == rlock_protocol::OK)
      {
        tprintf("revoking lock %lld from %s successfully\n", lid, tmp.c_str());
      }
      else
      {
        tprintf("revoking lock %lld from %s failed\n", lid, tmp.c_str());
        ltable[lid].revoke = false;
      }
    }
    ret = lock_protocol::RETRY;
  }
  pthread_mutex_unlock(&lock);
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  tprintf("<server release> %lld, %s\n", lid, id.c_str());
  pthread_mutex_lock(&lock);
  if (ltable.find(lid) == ltable.end()) {
    tprintf("Error: an registered lock %lld returned\n", lid);
    pthread_mutex_unlock(&lock);
    return ret;
  }
  ltable[lid].free = true;
  ltable[lid].revoke = false;
  if (!ltable[lid].waitings.empty()) {
    std::string succ = *ltable[lid].waitings.begin();
    tprintf("asking %s to retry acquiring lock %lld\n", succ.c_str(), lid);
    handle successor(succ);
    rpcc *cl = successor.safebind();
    int r;
    pthread_mutex_unlock(&lock);
    ret = cl->call(rlock_protocol::retry, lid, r);
    pthread_mutex_lock(&lock);
    if (ret == lock_protocol::OK) {
      tprintf("retry %s successfully\n", succ.c_str());
    }
    else {
      tprintf("retry %s failed\n", succ.c_str());
    }
  }
  pthread_mutex_unlock(&lock);
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

