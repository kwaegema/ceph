#ifndef CEPH_OBJECT_REGISTRY_H
#define CEPH_OBJECT_REGISTRY_H

#include <boost/optional.hpp>

#include "osd_types.h"
#include "os/ObjectStore.h"

class ObjectContext;
typedef std::tr1::shared_ptr<ObjectContext> ObjectContextRef;

class ObjectState {
protected:
  object_info_t oi;
  bool exists;

  friend ostream& operator<<(ostream& out, ObjectState& obs);

public:
  ObjectState(const ObjectState &other) {
    *this = other;
  }
  ObjectState(const object_info_t &_oi, bool _exists)
    : oi(_oi), exists(_exists) {}
  ObjectState()
    : exists(false) {}

  bool get_exists() const {
    return exists;
  }

  void set_exists(bool _exists) {
    exists = _exists;
  }

  const object_info_t &get_oi() const {
    return oi;
  }

  object_info_t &get_oi() {
    return oi;
  }

  void set_user_version(eversion_t version) {
    oi.user_version = version;
  }

  void set_version(eversion_t version) {
    oi.version = version;
  }

  friend class ObjectContext;
  friend class PGRegistry;
};


struct SnapSetContext;
typedef std::tr1::shared_ptr<SnapSetContext> SnapSetContextRef;

class SnapSetContext {
protected:
  object_t oid;
  SnapSet snapset;

public:
  SnapSetContext(const SnapSetContext &other) {
    *this = other;
  }
  SnapSetContext() { }
  SnapSetContext(object_t _oid) : oid(_oid) { }

  const SnapSet &get_snapset() const {
    return snapset;
  }

  SnapSet &get_snapset() {
    return snapset;
  }

  friend class ObjectContext;
  friend class PGRegistry;
};

inline ostream& operator<<(ostream& out, ObjectState& obs)
{
  out << obs.oi.soid;
  if (!obs.exists)
    out << "(dne)";
  return out;
}

/*
  * keep tabs on object modifications that are in flight.
  * we need to know the projected existence, size, snapset,
  * etc., because we don't send writes down to disk until after
  * replicas ack.
  */
class ObjectContext {
protected:
  ObjectState obs;

  SnapSetContextRef ssc;  // may be null

  Mutex lock;

  Cond cond;
  int unstable_writes, readers, writers_waiting, readers_waiting;

  // set if writes for this object are blocked on another objects recovery
  ObjectContextRef blocked_by;      // object blocking our writes
  set<ObjectContextRef> blocking;   // objects whose writes we block

  // any entity in obs.oi.watchers MUST be in either watchers or unconnected_watchers.
  map<pair<uint64_t, entity_name_t>, WatchRef> watchers;

  friend ostream& operator<<(ostream& out, ObjectContext& obc);

public:

  ObjectContext()
    : lock("ReplicatedPG::ObjectContext::lock"),
      unstable_writes(0), readers(0), writers_waiting(0), readers_waiting(0)
      {}

  //
  // setters
  //

  void set_exists(bool exists) {
    obs.exists = exists;
  }
  
  void set_version(eversion_t version);

  void set_snaps(set<snapid_t> &new_snaps);

  //
  // getters
  //

  bool get_exists() const {
    return obs.exists;
  }

  const ObjectState &get_obs() const {
    return obs;
  }

  uint64_t get_size() const {
    return obs.oi.size;
  }
  
  eversion_t get_version() const {
    return obs.oi.version;
  }

  eversion_t get_user_version() const {
    return obs.oi.user_version;
  }

  eversion_t get_prior_version() const {
    return obs.oi.prior_version;
  }

  bool get_lost() const {
    return obs.oi.lost;
  }

  const string &get_category() const {
    return obs.oi.category;
  }

  const hobject_t &get_soid() const {
    return obs.oi.soid;
  }

  snapid_t get_snap() const {
    return obs.oi.soid.snap;
  }
  
  set<snapid_t> get_snaps_as_set() const {
    const vector<snapid_t> &snaps = obs.oi.snaps;
    return set<snapid_t>(snaps.begin(), snaps.end());
  }

  const vector<snapid_t> &get_snaps() const {
    return obs.oi.snaps;
  }

  const object_info_t &get_oi() const {
    return obs.oi;
  }

  const vector<snapid_t> &get_clones() const {
    return ssc->snapset.clones;
  }

  const SnapSet &get_snapset() const {
    assert(ssc);
    return ssc->get_snapset();
  }

  const SnapSetContextRef &get_ssc() const {
    return ssc;
  }

  unsigned int get_watchers_size() const {
    return watchers.size();
  }

  void get_watchers(list<obj_watch_item_t> &pg_watchers) const;

  const hobject_t &get_blocked_by_soid() const {
    return blocked_by->obs.oi.soid;
  }

  //
  // predicates
  //

  bool is_old_snap(snapid_t seq) const {
    return seq < ssc->snapset.seq;
  }

  int assert_src_version(uint64_t ver) const {
    if (!ver)
      return -EINVAL;
    else if (ver < obs.oi.user_version.version)
      return -ERANGE;
    else if (ver > obs.oi.user_version.version)
      return -EOVERFLOW;
    return -EINVAL;
  }
  
  bool different_keys_maybe_same_names(const ObjectContextRef other) {
    //
    // return true for
    //
    //       this   other
    // key    A      B
    // name   C      C
    //
    //       this   other
    // key    A      B
    // name   C      D
    //
    return obs.oi.soid.get_key() != other->obs.oi.soid.get_key() &&
      obs.oi.soid.get_key() != other->obs.oi.soid.oid.name &&
      obs.oi.soid.oid.name != other->obs.oi.soid.get_key();
  }

  bool busy() const {
    return obs.oi.watchers.size() > 0;
  }

  bool is_blocked() const {
    return blocked_by;
  }

  //
  // side effects
  //

  void apply(ObjectState &_obs, SnapSet &_snapset) {
    obs = _obs;
    assert(ssc);
    ssc->snapset = _snapset;
  }

  object_stat_sum_t remove_clone_from_snapset(snapid_t last);

  void set_blocked_by(ObjectContextRef _blocked_by) {
    blocked_by = _blocked_by;
    ObjectContextRef self(this);
    blocked_by->blocking.insert(self);
  }

  void notify_ack_watchers(uint64_t notify_id, boost::optional<uint64_t> &watch_cookie, entity_name_t &entity);
  void watchers_erase(WatchRef watch);
  void notify_watchers(NotifyRef notif);
  void remove_watcher(watch_info_t &watch_info, entity_name_t &entity);
  WatchRef get_or_create_watcher(watch_info_t &watch_info, entity_name_t &entity, const entity_addr_t &peer_addr, ReplicatedPG *pg, OSDService *osd);

  void no_longer_blocking();

  //
  // writers
  //

  void save_object_info(coll_t coll, ObjectStore::Transaction *t);

  void save_snapset(coll_t coll, ObjectStore::Transaction *t);

  // do simple synchronous mutual exclusion, for now.  now waitqueues or anything fancy.
  void ondisk_write_lock() {
    lock.Lock();
    writers_waiting++;
    while (readers_waiting || readers)
      cond.Wait(lock);
    writers_waiting--;
    unstable_writes++;
    lock.Unlock();
  }
  void ondisk_write_unlock() {
    lock.Lock();
    assert(unstable_writes > 0);
    unstable_writes--;
    if (!unstable_writes && readers_waiting)
      cond.Signal();
    lock.Unlock();
  }
  void ondisk_read_lock() {
    lock.Lock();
    readers_waiting++;
    while (unstable_writes)
      cond.Wait(lock);
    readers_waiting--;
    readers++;
    lock.Unlock();
  }
  void ondisk_read_unlock() {
    lock.Lock();
    assert(readers > 0);
    readers--;
    if (!readers && writers_waiting)
      cond.Signal();
    lock.Unlock();
  }

  friend class PGRegistry;
};

inline ostream& operator<<(ostream& out, ObjectContext& obc)
{
  return out << "obc(" << obc.obs << ")";
}

struct obj_watch_item_t;
struct ReplicatedPG_OpContext; 

class PGRegistry {
protected:
  SharedPtrRegistry<hobject_t, ObjectContext> objects;
  SharedPtrRegistry<object_t, SnapSetContext> snapsets;
  ReplicatedPG *pg;
  OSDService *osd;

  void populate_obc_watchers(ObjectContextRef obc);
  void check_blacklisted_obc_watchers(ObjectContextRef obc);

public:

 PGRegistry(ReplicatedPG *_pg, OSDService *_osd) :
  pg(_pg), osd(_osd)
  {}

  bool empty() const {
    return objects.empty();
  }

  void check_blacklisted_watchers();
  void get_watchers(list<obj_watch_item_t> &pg_watchers) const;
  void discard_watchers();

  ObjectContextRef lookup_object_context(const hobject_t& soid) {
    return objects.lookup(soid);
  }
  // new ObjectContext but do not add it to the registry
  //  populates watchers
  //  do not read from disk
  //  set ssc 
  //  ignores oi.snap
  ObjectContextRef new_object_context(const object_info_t& oi, SnapSetContextRef ssc);
  // new ObjectContext if not already in the registry
  //  do not populates watchers
  //  do not read from disk
  //  do not set ssc
  //  ignores oi.snap
  ObjectContextRef lookup_or_create_object_context(const object_info_t& oi);
  // new ObjectContext if not already in the registry
  //  populates watchers
  //  reads from disk
  //  set ssc with new snapsetcontext if not already in the registry
  //  ignores oi.snap
  ObjectContextRef get_object_context(const hobject_t& soid, bool can_create);
  // new ObjectContext if not already in the registry
  //  populates watchers
  //  reads from disk
  //  if with_ssc is true
  //    set ssc with new snapsetcontext if not already in the registry
  //  the returned ObjectContext depends on oid.snap
  int find_object_context(const hobject_t& oid,
			  ObjectContextRef &pobc,
                          snapid_t *psnapid,
			  bool with_ssc);
  // new ObjectContext if not already in the registry
  //  populates watchers
  //  reads from disk
  //  do not set ssc
  //  the returned ObjectContext depends on oid.snap
  int find_or_create_object_context(const hobject_t& oid,
			  ObjectContextRef &pobc,
			  bool can_create, snapid_t *psnapid=NULL);

  // new SnapSetContext if not already in the registry
  //  do not read from disk
  //  empty SnapSet
  SnapSetContextRef create_snapset_context(const object_t &oid);
  // new SnapSetContext if not already in the registry
  //  do not read from disk
  //  set SnapSet
  SnapSetContextRef create_snapset_context(const object_t &_oid, SnapSet &_snapset);
  // new SnapSetContext if not already in the registry
  //  reads from disk
  //  set SnapSet
  SnapSetContextRef get_or_create_snapset_context(const object_t& oid, const string &key,
                                        ps_t seed, bool can_create, const string &nspace);
  int list_snaps(const hobject_t& clone_oid, obj_list_snap_response_t& resp);

  //
  // stats helper
  //
  void add_object_context_to_pg_stat(ObjectContextRef obc, pg_stat_t *stat);

  //
  // writers
  //
  ObjectContextRef mark_object_lost(ObjectStore::Transaction *t, const hobject_t &oid, eversion_t version);
  void save_snapset(ReplicatedPG_OpContext *ctx, const hobject_t &soid);
  void save_head(ReplicatedPG_OpContext *ctx, const hobject_t &soid, eversion_t old_version);
};

#endif // CEPH_OBJECT_REGISTRY_H
