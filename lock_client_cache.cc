// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  pthread_mutex_init(&pooll, NULL);
  pthread_cond_init(&poolc, NULL);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  pthread_mutex_lock(&pooll);
  tprintf("%s<<%ld>> <client acquire> %lld\n",id.c_str(), pthread_self(), lid);
  // init alock in lock table if not exists
  if (ltable.find(lid) == ltable.end()) {
    alock al;
    al.l_state = NONE;
    al.revoke = false;
    al.waitings = std::set<pthread_t>();
    pthread_cond_init(&al.l_cond, NULL);
    ltable.insert(std::pair<lock_protocol::lockid_t, alock>(lid, al));
  }
  while (true) {
    switch(ltable[lid].l_state) {
      case NONE: {
        // change state to acquiring
        tprintf("%s<<%ld>> attempty to acquire lock %lld\n", id.c_str(), pthread_self(), lid);
        ltable[lid].l_state = ACQUIRING;
        pthread_mutex_unlock(&pooll);
        int r;
        ret = cl->call(lock_protocol::acquire, lid, id, r);     // revoke may arrive earlier than OK
        pthread_mutex_lock(&pooll);
        // OK -> lock acquired
        if (ret == lock_protocol::OK) {
          // tprintf("OK!\t");
          if (!ltable[lid].revoke) {
            ltable[lid].l_state = OWN;
          }
          // if received revoke already, pend revoke
          else {
            ltable[lid].l_state = RELEASING;
          }
          // printf("[1]%d\n", ltable[lid].l_state);
          goto finish;
        }
        else if (ret == lock_protocol::RETRY) {
          tprintf("%s<<%ld>>: lock %lld acquiring failed\n", id.c_str(), pthread_self(), lid);
        }
        else {
          tprintf("%s: unexpected return value %d", id.c_str(), ret);
        }
        break;
      }
      case OWN: case ACQUIRING: case RELEASING: {
        ltable[lid].waitings.insert(pthread_self());
        pthread_cond_wait(&ltable[lid].l_cond, &pooll);
        break;
      }
      case FREE: {
        ltable[lid].l_state = OWN;
        goto finish;
      }
      default: ;
    }
  }
finish:
  pthread_t myself = pthread_self();
  if (ltable[lid].waitings.find(myself) != ltable[lid].waitings.end())
  {
    ltable[lid].waitings.erase(myself);
  }
  tprintf("%s<<%ld>>: lock %lld grabbed\n", id.c_str(), myself, lid);
  ret = lock_protocol::OK;
  pthread_mutex_unlock(&pooll);
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  pthread_mutex_lock(&pooll);
  tprintf("%s<<%ld>> <client release> %lld\n",id.c_str(), pthread_self(), lid);
  if (ltable.find(lid) == ltable.end()) {
    tprintf("Warning [1]: an unexisting lock %lld released\n", lid);
    pthread_mutex_unlock(&pooll);
    return ret;
  }
  if (ltable[lid].l_state == OWN) {
    if (ltable[lid].waitings.empty()) {
      ltable[lid].l_state = RELEASING;
    }
    else {
      ltable[lid].l_state = FREE;
      tprintf("%s<<%ld>> freed lock %lld\n",id.c_str(), pthread_self(), lid);
      pthread_cond_signal(&ltable[lid].l_cond);
      goto finish;
    }
  }
  if (ltable[lid].l_state == RELEASING) {
    int r;
    pthread_mutex_unlock(&pooll);
    ret = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&pooll);
    if (ret == lock_protocol::OK) {
      tprintf("%s lock %lld has been returned to server successfully\n", id.c_str(), lid);
      ltable[lid].l_state = NONE;
      pthread_cond_signal(&ltable[lid].l_cond);   // here we awake someone
    }
    else {
      tprintf("%s returning lock %lld to server failed\n", id.c_str(), lid);
    }
  }
  else {
    tprintf("Warning [2]:%s lock %lld is released in abnormal state %d\n",id.c_str(), lid, ltable[lid].l_state);
    ret = lock_protocol::IOERR;
  }
finish:
  pthread_mutex_unlock(&pooll);
  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&pooll);
  tprintf("%s<<%ld>> <client revoke> %lld\n",id.c_str(), pthread_self(), lid);
  if (ltable[lid].l_state == FREE) {
    int r;
    ltable[lid].l_state = RELEASING;
    pthread_mutex_unlock(&pooll);
    ret = cl->call(lock_protocol::release, lid, id, r);   // someone might acquire the lock
    pthread_mutex_lock(&pooll);
    if (ret == lock_protocol::OK) {
      tprintf("%s lock %lld has been returned to server successfully\n", id.c_str(), lid);
      ltable[lid].l_state = NONE;
      pthread_cond_signal(&ltable[lid].l_cond);   // here we awake someone
    }
    else {
      tprintf("%s returning lock %lld to server failed\n", id.c_str(), lid);
    }
  }
  else if (ltable[lid].l_state == OWN) {
    ltable[lid].l_state = RELEASING;
    tprintf("revoke lock %lld pending\n", lid);
  }
  else if (ltable[lid].l_state == ACQUIRING) {
    ltable[lid].revoke = true;
    tprintf("revoke lock %lld pending\n", lid);
  }
  else if (ltable[lid].l_state == RELEASING) {}
  else {
    tprintf ("Warning [3]:%s revoke received, but lock %lld in abnormal state %d\n",id.c_str(), lid, ltable[lid].l_state);
    ret = lock_protocol::IOERR;
  }
  pthread_mutex_unlock(&pooll);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&pooll);
  tprintf("%s<<%ld>> <client retry> %lld\n",id.c_str(), pthread_self(), lid);
  if (ltable.find(lid) == ltable.end()) {
    tprintf("Error: client %s hasn't acquired for lock %lld\n", id.c_str(), lid);
    pthread_mutex_unlock(&pooll);
    return ret;
  }
  if (ltable[lid].l_state == ACQUIRING) {
    ltable[lid].l_state = NONE;
    pthread_cond_signal(&ltable[lid].l_cond);
  } 
  else {
    tprintf("Warning [4]:%s<<%ld>> lock %lld in state %d\n",id.c_str(), pthread_self(), lid, ltable[lid].l_state);
  }
  pthread_mutex_unlock(&pooll);
  return ret;
}



