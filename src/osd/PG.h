// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_PG_H
#define CEPH_PG_H

#include <boost/statechart/custom_reaction.hpp>
#include <boost/statechart/event.hpp>
#include <boost/statechart/simple_state.hpp>
#include <boost/statechart/state.hpp>
#include <boost/statechart/state_machine.hpp>
#include <boost/statechart/transition.hpp>
#include <boost/statechart/event_base.hpp>
#include <boost/scoped_ptr.hpp>
#include <tr1/memory>

// re-include our assert to clobber boost's
#include "include/assert.h" 

#include "include/types.h"
#include "include/stringify.h"
#include "osd_types.h"
#include "include/buffer.h"
#include "include/xlist.h"
#include "include/atomic.h"
#include "SnapMapper.h"

#include "PGRecoveryState.h"
#include "PGPeering.h"
#include "PGLog.h"
#include "OpRequest.h"
#include "OSDMap.h"
#include "os/ObjectStore.h"
#include "msg/Messenger.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDPGLog.h"
#include "common/tracked_int_ptr.hpp"
#include "common/WorkQueue.h"

#include <list>
#include <memory>
#include <string>
using namespace std;

#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;

using namespace PGRecoveryState;

//#define DEBUG_RECOVERY_OIDS   // track set of recovering oids explicitly, to find counting bugs

class OSD;
class OSDService;
class MOSDOp;
class MOSDSubOp;
class MOSDSubOpReply;
class MOSDPGScan;
class MOSDPGBackfill;
class MOSDPGInfo;

class PG;

void intrusive_ptr_add_ref(PG *pg);
void intrusive_ptr_release(PG *pg);

#ifdef PG_DEBUG_REFS
  uint64_t get_with_id(PG *pg);
  void put_with_id(PG *pg, uint64_t id);
  typedef TrackedIntPtr<PG> PGRef;
#else
  typedef boost::intrusive_ptr<PG> PGRef;
#endif

struct PGPool {
  int64_t id;
  string name;
  uint64_t auid;

  pg_pool_t info;      
  SnapContext snapc;   // the default pool snapc, ready to go.

  interval_set<snapid_t> cached_removed_snaps;      // current removed_snaps set
  interval_set<snapid_t> newly_removed_snaps;  // newly removed in the last epoch

  PGPool(int64_t i, const char *_name, uint64_t au) :
    id(i), auid(au) {
    if (_name)
      name = _name;
  }

  void update(OSDMapRef map);
};

/** PG - Replica Placement Group
 *
 */

// -- scrub --
struct PGScrubber {
  PGScrubber() :
    reserved(false), reserve_failed(false),
    epoch_start(0),
    block_writes(false), active(false), queue_snap_trim(false),
    waiting_on(0), shallow_errors(0), deep_errors(0), fixed(0),
    active_rep_scrub(0),
    must_scrub(false), must_deep_scrub(false), must_repair(false),
    classic(false),
    finalizing(false), is_chunky(false), state(INACTIVE),
    deep(false)
  {
  }

  // metadata
  set<int> reserved_peers;
  bool reserved, reserve_failed;
  epoch_t epoch_start;

  // common to both scrubs
  bool block_writes;
  bool active;
  bool queue_snap_trim;
  int waiting_on;
  set<int> waiting_on_whom;
  int shallow_errors;
  int deep_errors;
  int fixed;
  ScrubMap primary_scrubmap;
  map<int,ScrubMap> received_maps;
  MOSDRepScrub *active_rep_scrub;
  utime_t scrub_reg_stamp;  // stamp we registered for

  // flags to indicate explicitly requested scrubs (by admin)
  bool must_scrub, must_deep_scrub, must_repair;

  // Maps from objects with errors to missing/inconsistent peers
  map<hobject_t, set<int> > missing;
  map<hobject_t, set<int> > inconsistent;
  map<hobject_t, set<int> > inconsistent_snapcolls;

  // Map from object with errors to good peer
  map<hobject_t, pair<ScrubMap::object, int> > authoritative;

  // classic scrub
  bool classic;
  bool finalizing;

  // chunky scrub
  bool is_chunky;
  hobject_t start, end;
  eversion_t subset_last_update;

  // chunky scrub state
  enum State {
    INACTIVE,
    NEW_CHUNK,
    WAIT_PUSHES,
    WAIT_LAST_UPDATE,
    BUILD_MAP,
    WAIT_REPLICAS,
    COMPARE_MAPS,
    FINISH,
  } state;

  // deep scrub
  bool deep;

  list<Context*> callbacks;
  void add_callback(Context *context) {
    callbacks.push_back(context);
  }
  void run_callbacks() {
    list<Context*> to_run;
    to_run.swap(callbacks);
    for (list<Context*>::iterator i = to_run.begin();
	 i != to_run.end();
	 ++i) {
      (*i)->complete(0);
    }
  }

  static const char *state_string(const PGScrubber::State& state) {
    const char *ret = NULL;
    switch( state )
      {
      case INACTIVE: ret = "INACTIVE"; break;
      case NEW_CHUNK: ret = "NEW_CHUNK"; break;
      case WAIT_PUSHES: ret = "WAIT_PUSHES"; break;
      case WAIT_LAST_UPDATE: ret = "WAIT_LAST_UPDATE"; break;
      case BUILD_MAP: ret = "BUILD_MAP"; break;
      case WAIT_REPLICAS: ret = "WAIT_REPLICAS"; break;
      case COMPARE_MAPS: ret = "COMPARE_MAPS"; break;
      case FINISH: ret = "FINISH"; break;
      }
    return ret;
  }

  bool is_chunky_scrub_active() const { return state != INACTIVE; }

  // classic (non chunk) scrubs block all writes
  // chunky scrubs only block writes to a range
  bool write_blocked_by_scrub(const hobject_t &soid) {
    if (!block_writes)
      return false;

    if (!is_chunky)
      return true;

    if (soid >= start && soid < end)
      return true;

    return false;
  }

  // clear all state
  void reset() {
    classic = false;
    finalizing = false;
    block_writes = false;
    active = false;
    queue_snap_trim = false;
    waiting_on = 0;
    waiting_on_whom.clear();
    if (active_rep_scrub) {
      active_rep_scrub->put();
      active_rep_scrub = NULL;
    }
    received_maps.clear();

    must_scrub = false;
    must_deep_scrub = false;
    must_repair = false;

    state = PGScrubber::INACTIVE;
    start = hobject_t();
    end = hobject_t();
    subset_last_update = eversion_t();
    shallow_errors = 0;
    deep_errors = 0;
    fixed = 0;
    deep = false;
    run_callbacks();
    inconsistent.clear();
    missing.clear();
    authoritative.clear();
  }

};

class PG : public PGRecoveryStateInterface {
public:
  virtual std::string gen_prefix() const;

  /*** PG ****/
protected:
  OSDService *osd;
  virtual OSDService *get_osd() { return osd; }
  OSDriver osdriver;
  SnapMapper snap_mapper;
public:
  void update_snap_mapper_bits(uint32_t bits) {
    snap_mapper.update_bits(bits);
  }
protected:
  // Ops waiting for map, should be queued at back
  Mutex map_lock;
  list<OpRequestRef> waiting_for_map;
  OSDMapRef osdmap_ref;
  OSDMapRef last_persisted_osdmap_ref;
  PGPool pool;
  virtual const PGPool &get_pool() const { return pool; }

  void queue_op(OpRequestRef op);
  void take_op_map_waiters();

  void update_osdmap_ref(OSDMapRef newmap) {
    assert(_lock.is_locked_by_me());
    Mutex::Locker l(map_lock);
    osdmap_ref = newmap;
  }

  OSDMapRef get_osdmap_with_maplock() const {
    assert(map_lock.is_locked());
    assert(osdmap_ref);
    return osdmap_ref;
  }

  virtual OSDMapRef get_osdmap() const {
    assert(is_locked());
    assert(osdmap_ref);
    return osdmap_ref;
  }

  /** locking and reference counting.
   * I destroy myself when the reference count hits zero.
   * lock() should be called before doing anything.
   * get() should be called on pointer copy (to another thread, etc.).
   * put() should be called on destruction of some previously copied pointer.
   * put_unlock() when done with the current pointer (_most common_).
   */  
  Mutex _lock;
  Cond _cond;
  atomic_t ref;

#ifdef PG_DEBUG_REFS
  Mutex _ref_id_lock;
  map<uint64_t, string> _live_ids;
  map<string, uint64_t> _tag_counts;
  uint64_t _ref_id;
#endif

public:
  bool deleting;  // true while in removing or OSD is shutting down

  virtual void lock(bool no_lockdep = false);
  virtual void unlock() {
    //generic_dout(0) << this << " " << info.pgid << " unlock" << dendl;
    assert(!dirty_info);
    assert(!dirty_big_info);
    _lock.Unlock();
  }

  void assert_locked() {
    assert(_lock.is_locked());
  }
  bool is_locked() const {
    return _lock.is_locked();
  }
  void wait() {
    assert(_lock.is_locked());
    _cond.Wait(_lock);
  }
  void kick() {
    assert(_lock.is_locked());
    _cond.Signal();
  }

#ifdef PG_DEBUG_REFS
  uint64_t get_with_id();
  void put_with_id(uint64_t);
  void dump_live_ids();
#endif
  virtual void get(const string &tag);
  virtual void put(const string &tag);

  bool dirty_info, dirty_big_info;
  virtual void set_dirty_info(bool _dirty_info) {
    dirty_info = _dirty_info;
  }
  virtual void set_dirty_big_info(bool _dirty_big_info) {
    dirty_big_info = _dirty_big_info;
  }

  pg_info_t        info;
  virtual const pg_info_t &get_info() const { return info; }
  virtual void set_info(const pg_info_t &_info) { info = _info; }
  virtual void set_info_stats(const pg_stat_t &stats) {
    info.stats = stats;
  }
  __u8 info_struct_v;
  static const __u8 cur_struct_v = 7;
  bool must_upgrade() {
    return info_struct_v < 7;
  }
  void upgrade(
    ObjectStore *store,
    const interval_set<snapid_t> &snapcolls);

  const coll_t coll;
  PGLog  pg_log;
  virtual const PGLog &get_pg_log() const {
    return pg_log;
  }
  virtual void claim_log(const pg_log_t& o) {
    pg_log.claim_log(o);
  }
  virtual void reset_backfill() {
    pg_log.reset_backfill();
  }
  static string get_info_key(pg_t pgid) {
    return stringify(pgid) + "_info";
  }
  static string get_biginfo_key(pg_t pgid) {
    return stringify(pgid) + "_biginfo";
  }
  static string get_epoch_key(pg_t pgid) {
    return stringify(pgid) + "_epoch";
  }
  hobject_t    log_oid;
  hobject_t    biginfo_oid;
  map<hobject_t, set<int> > missing_loc;
  virtual const map<hobject_t, set<int> > &get_missing_loc() const {
    return missing_loc;
  }
  set<int> missing_loc_sources;           // superset of missing_loc locations
  
  interval_set<snapid_t> snap_collections; // obsolete
  map<epoch_t,pg_interval_t> past_intervals;
  virtual const map<epoch_t,pg_interval_t> get_past_intervals() const { return past_intervals; }

  interval_set<snapid_t> snap_trimq;
  virtual interval_set<snapid_t> get_snap_trimq() {
    return snap_trimq;
  }

  /* You should not use these items without taking their respective queue locks
   * (if they have one) */
  xlist<PG*>::item recovery_item, scrub_item, scrub_finalize_item, snap_trim_item, stat_queue_item;
  int recovery_ops_active;
  bool waiting_on_backfill;
#ifdef DEBUG_RECOVERY_OIDS
  set<hobject_t> recovering_oids;
#endif

  utime_t replay_until;

protected:
  int         role;    // 0 = primary, 1 = replica, -1=none.
  unsigned    state;   // PG_STATE_*

  bool send_notify;    ///< true if we are non-primary and should notify the primary

public:
  eversion_t  last_update_ondisk;    // last_update that has committed; ONLY DEFINED WHEN is_active()
  eversion_t  last_complete_ondisk;  // last_complete that has committed.
  eversion_t  last_update_applied;

  // primary state
 public:
  vector<int> up, acting, want_acting;
  virtual const vector<int> &get_up() const { return up; }
  virtual const vector<int> &get_acting() const { return acting; }
  virtual const vector<int> &get_want_acting() const { return want_acting; }
  virtual void clear_want_acting() { return want_acting.clear(); }
  map<int,eversion_t> peer_last_complete_ondisk;
  eversion_t  min_last_complete_ondisk;  // up: min over last_complete_ondisk, peer_last_complete_ondisk
  eversion_t  pg_trim_to;

  // [primary only] content recovery state
protected:
  bool may_need_replay(const OSDMapRef osdmap) const;

  /*
   * peer_info    -- projected (updates _before_ replicas ack)
   * peer_missing -- committed (updates _after_ replicas ack)
   */
  
  bool        need_up_thru;
  virtual bool get_need_up_thru() const {
    return need_up_thru;
  }
  set<int>    stray_set;   // non-acting osds that have PG data.
  eversion_t  oldest_update; // acting: lowest (valid) last_update in active set
  map<int,pg_info_t>    peer_info;   // info from peers (stray or prior)
  virtual const map<int,pg_info_t>  &get_peer_info() { return peer_info; }  // info from peers (stray or prior)
  set<int> peer_purged; // peers purged
  virtual const set<int> &get_peer_purged() const {
    return peer_purged;
  }
  map<int,pg_missing_t> peer_missing;
  virtual const map<int,pg_missing_t> &get_peer_missing() const {
    return peer_missing;
  }
  virtual pg_missing_t &get_peer_missing(int peer) {
    return peer_missing[peer];
  }
  virtual void set_peer_missing(int peer) {
    peer_missing[peer];
  }
  set<int>             peer_log_requested;  // logs i've requested (and start stamps)
  virtual const set<int> &get_peer_log_requested() const {
    return peer_log_requested;
  }
  set<int>             peer_missing_requested;
  virtual const set<int> &get_peer_missing_requested() const {
    return peer_missing_requested;
  }
  set<int>             stray_purged;  // i deleted these strays; ignore racing PGInfo from them
  set<int>             peer_activated;
  virtual set<int> &get_peer_activated() {
    return peer_activated;
  }

  // primary-only, recovery-only state
  set<int>             might_have_unfound;  // These osds might have objects on them
					    // which are unfound on the primary
  virtual const set<int> &get_might_have_unfound() const {
    return might_have_unfound;
  }
  bool need_flush;     // need to flush before any new activity

  epoch_t last_peering_reset;


  /* heartbeat peers */
  void set_probe_targets(const set<int> &probe_set);
  virtual void clear_probe_targets();
public:
  Mutex heartbeat_peer_lock;
  set<int> heartbeat_peers;
  set<int> probe_targets;

protected:
  /**
   * BackfillInterval
   *
   * Represents the objects in a range [begin, end)
   *
   * Possible states:
   * 1) begin == end == hobject_t() indicates the the interval is unpopulated
   * 2) Else, objects contains all objects in [begin, end)
   */
  struct BackfillInterval {
    // info about a backfill interval on a peer
    map<hobject_t,eversion_t> objects;
    hobject_t begin;
    hobject_t end;
    
    /// clear content
    void clear() {
      objects.clear();
      begin = end = hobject_t();
    }

    void reset(hobject_t start) {
      clear();
      begin = end = start;
    }

    /// true if there are no objects in this interval
    bool empty() {
      return objects.empty();
    }

    /// true if interval extends to the end of the range
    bool extends_to_end() {
      return end == hobject_t::get_max();
    }

    /// Adjusts begin to the first object
    void trim() {
      if (!objects.empty())
	begin = objects.begin()->first;
      else
	begin = end;
    }

    /// drop first entry, and adjust @begin accordingly
    void pop_front() {
      assert(!objects.empty());
      objects.erase(objects.begin());
      if (objects.empty())
	begin = end;
      else
	begin = objects.begin()->first;
    }

    /// dump
    void dump(Formatter *f) const {
      f->dump_stream("begin") << begin;
      f->dump_stream("end") << end;
      f->open_array_section("objects");
      for (map<hobject_t, eversion_t>::const_iterator i = objects.begin();
	   i != objects.end();
	   ++i) {
	f->open_object_section("object");
	f->dump_stream("object") << i->first;
	f->dump_stream("version") << i->second;
	f->close_section();
      }
      f->close_section();
    }
  };
  
  BackfillInterval backfill_info;
  BackfillInterval peer_backfill_info;
  int backfill_target;
  bool backfill_reserved;
  virtual bool get_backfill_reserved() const {
    return backfill_reserved;
  }
  virtual void set_backfill_reserved(bool _backfill_reserved) {
    backfill_reserved = _backfill_reserved;
  }
  bool backfill_reserving;
  virtual bool get_backfill_reserving() const {
    return backfill_reserving;
  }
  virtual void set_backfill_reserving(bool _backfill_reserving) {
    backfill_reserving = _backfill_reserving;
  }

  friend class OSD;

public:
  virtual int get_backfill_target() const {
    return backfill_target;
  }

protected:


  // pg waiters
  bool flushed;

  virtual bool get_flushed() const { return flushed; }
  virtual void set_flushed(bool _flushed) { flushed = _flushed; }

  // Ops waiting on backfill_pos to change
  list<OpRequestRef> waiting_for_backfill_pos;
  list<OpRequestRef>            waiting_for_active;
  virtual void requeue_waiting_for_active() { requeue_ops(waiting_for_active); }
  list<OpRequestRef>            waiting_for_all_missing;
  map<hobject_t, list<OpRequestRef> > waiting_for_missing_object,
                                        waiting_for_degraded_object;
  // Callbacks should assume pg (and nothing else) is locked
  map<hobject_t, list<Context*> > callbacks_for_degraded_object;
  map<eversion_t,list<OpRequestRef> > waiting_for_ack, waiting_for_ondisk;
  map<eversion_t,OpRequestRef>   replay_queue;
  void split_ops(PG *child, unsigned split_bits);

  void requeue_object_waiters(map<hobject_t, list<OpRequestRef> >& m);
  void requeue_ops(list<OpRequestRef> &l);

  // stats that persist lazily
  object_stat_collection_t unstable_stats;

  // publish stats
  Mutex pg_stats_publish_lock;
  bool pg_stats_publish_valid;
  pg_stat_t pg_stats_publish;

  // for ordering writes
  std::tr1::shared_ptr<ObjectStore::Sequencer> osr;

  virtual void publish_stats_to_osd();
  void clear_publish_stats();

public:
  void clear_primary_state();

 public:
  virtual bool is_acting(int osd) const { 
    for (unsigned i=0; i<acting.size(); i++)
      if (acting[i] == osd) return true;
    return false;
  }
  bool is_up(int osd) const { 
    for (unsigned i=0; i<up.size(); i++)
      if (up[i] == osd) return true;
    return false;
  }
  
  virtual bool needs_recovery() const;
  bool needs_backfill() const;

  virtual void mark_clean();  ///< mark an active pg clean

  bool _calc_past_interval_range(epoch_t *start, epoch_t *end);
  virtual void generate_past_intervals();
  void trim_past_intervals();
  virtual void build_prior(std::auto_ptr<PriorSet> &prior_set);

  virtual void remove_down_peer_info(const OSDMapRef osdmap);

  virtual bool adjust_need_up_thru(const OSDMapRef osdmap);

  virtual bool all_unfound_are_queried_or_lost(const OSDMapRef osdmap) const;
  virtual void mark_all_unfound_lost(int how) = 0;

  bool calc_min_last_complete_ondisk() {
    eversion_t min = last_complete_ondisk;
    for (unsigned i=1; i<acting.size(); i++) {
      if (peer_last_complete_ondisk.count(acting[i]) == 0)
	return false;   // we don't have complete info
      eversion_t a = peer_last_complete_ondisk[acting[i]];
      if (a < min)
	min = a;
    }
    if (min == min_last_complete_ondisk)
      return false;
    min_last_complete_ondisk = min;
    return true;
  }

  virtual void calc_trim_to() = 0;

  virtual void proc_replica_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog,
			pg_missing_t& omissing, int from);
  virtual void proc_master_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog,
		       pg_missing_t& omissing, int from);
  virtual bool proc_replica_info(int from, const pg_info_t &info);
  void remove_snap_mapped_object(
    ObjectStore::Transaction& t, const hobject_t& soid);
  virtual void merge_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog, int from);
  virtual void rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead);
  virtual bool search_for_missing(const pg_info_t &oinfo, const pg_missing_t *omissing,
			  int fromosd);

  void check_for_lost_objects();
  void forget_lost_objects();

  virtual void discover_all_missing(std::map< int, map<pg_t,pg_query_t> > &query_map);
  
  void trim_write_ahead();

  map<int, pg_info_t>::const_iterator find_best_info(const map<int, pg_info_t> &infos) const;
  bool calc_acting(int& newest_update_osd, vector<int>& want) const;
  virtual bool choose_acting(int& newest_update_osd);
  void build_might_have_unfound();
  void replay_queued_ops();
  virtual void activate(ObjectStore::Transaction& t,
		epoch_t query_epoch,
		list<Context*>& tfin,
		map< int, map<pg_t,pg_query_t> >& query_map,
		map<int, vector<pair<pg_notify_t, pg_interval_map_t> > > *activator_map=0);
  void _activate_committed(epoch_t e);
  virtual void all_activated_and_committed();

  virtual void proc_primary_info(ObjectStore::Transaction &t, const pg_info_t &info);

  virtual bool have_unfound() const { 
    return pg_log.get_missing().num_missing() > missing_loc.size();
  }
  int get_num_unfound() const {
    return pg_log.get_missing().num_missing() - missing_loc.size();
  }

  virtual void clean_up_local(ObjectStore::Transaction& t) = 0;

  virtual int start_recovery_ops(int max, RecoveryCtx *prctx) = 0;

  void purge_strays();

  virtual void update_heartbeat_peers();

  Context *finish_sync_event;

  virtual void finish_recovery(list<Context*>& tfin);
  void _finish_recovery(Context *c);
  void cancel_recovery();
  void clear_recovery_state();
  virtual void _clear_recovery_state() = 0;
  virtual void check_recovery_sources(const OSDMapRef newmap) = 0;
  void start_recovery_op(const hobject_t& soid);
  void finish_recovery_op(const hobject_t& soid, bool dequeue=false);

  void split_into(pg_t child_pgid, PG *child, unsigned split_bits);
  virtual void _split_into(pg_t child_pgid, PG *child, unsigned split_bits) = 0;

  loff_t get_log_write_pos() {
    return 0;
  }

  friend class C_OSD_RepModify_Commit;


  PGScrubber scrubber;

  virtual const PGScrubber &get_scrubber() const {
    return scrubber;
  }

  bool scrub_after_recovery;

  int active_pushes;

  void repair_object(const hobject_t& soid, ScrubMap::object *po, int bad_peer, int ok_peer);
  map<int, ScrubMap *>::const_iterator _select_auth_object(
    const hobject_t &obj,
    const map<int,ScrubMap*> &maps);

  enum error_type {
    CLEAN,
    DEEP_ERROR,
    SHALLOW_ERROR
  };
  enum error_type _compare_scrub_objects(ScrubMap::object &auth,
			      ScrubMap::object &candidate,
			      ostream &errorstream);
  void _compare_scrubmaps(const map<int,ScrubMap*> &maps,  
			  map<hobject_t, set<int> > &missing,
			  map<hobject_t, set<int> > &inconsistent,
			  map<hobject_t, int> &authoritative,
			  map<hobject_t, set<int> > &inconsistent_snapcolls,
			  ostream &errorstream);
  void scrub(ThreadPool::TPHandle &handle);
  void classic_scrub(ThreadPool::TPHandle &handle);
  void chunky_scrub(ThreadPool::TPHandle &handle);
  void scrub_compare_maps();
  void scrub_process_inconsistent();
  void scrub_finalize();
  void scrub_finish();
  void scrub_clear_state();
  bool scrub_gather_replica_maps();
  void _scan_list(
    ScrubMap &map, vector<hobject_t> &ls, bool deep,
    ThreadPool::TPHandle &handle);
  void _scan_snaps(ScrubMap &map);
  void _request_scrub_map_classic(int replica, eversion_t version);
  void _request_scrub_map(int replica, eversion_t version,
                          hobject_t start, hobject_t end, bool deep);
  int build_scrub_map_chunk(
    ScrubMap &map,
    hobject_t start, hobject_t end, bool deep,
    ThreadPool::TPHandle &handle);
  void build_scrub_map(ScrubMap &map, ThreadPool::TPHandle &handle);
  void build_inc_scrub_map(
    ScrubMap &map, eversion_t v, ThreadPool::TPHandle &handle);
  virtual void _scrub(ScrubMap &map) { }
  virtual void _scrub_clear_state() { }
  virtual void _scrub_finish() { }
  virtual coll_t get_temp_coll() = 0;
  virtual bool have_temp_coll() = 0;
  virtual bool _report_snap_collection_errors(
    const hobject_t &hoid,
    const map<string, bufferptr> &attrs,
    int osd,
    ostream &out) { return false; };
  void clear_scrub_reserved();
  void scrub_reserve_replicas();
  void scrub_unreserve_replicas();
  bool scrub_all_replicas_reserved() const;
  bool sched_scrub();
  virtual void reg_next_scrub();
  virtual void unreg_next_scrub();

  void replica_scrub(
    class MOSDRepScrub *op,
    ThreadPool::TPHandle &handle);
  void sub_op_scrub_map(OpRequestRef op);
  void sub_op_scrub_reserve(OpRequestRef op);
  void sub_op_scrub_reserve_reply(OpRequestRef op);
  void sub_op_scrub_unreserve(OpRequestRef op);
  void sub_op_scrub_stop(OpRequestRef op);

  virtual void reject_reservation();
  virtual void schedule_backfill_full_retry();

  list<CephPeeringEvtRef> peering_queue;  // op queue
  list<CephPeeringEvtRef> peering_waiters;

  RecoveryState recovery_state;


 public:
  PG(OSDService *o, OSDMapRef curmap,
     const PGPool &pool, pg_t p, const hobject_t& loid, const hobject_t& ioid);
  virtual ~PG();

 private:
  // Prevent copying
  PG(const PG& rhs);
  PG& operator=(const PG& rhs);

 public:
  pg_t       get_pgid() const { return info.pgid; }
  int        get_nrep() const { return acting.size(); }

  virtual int        get_primary() { return acting.empty() ? -1:acting[0]; }
  
  int        get_role() const { return role; }
  void       set_role(int r) { role = r; }

  virtual bool       is_primary() const { return role == 0; }
  bool       is_replica() const { return role > 0; }

  epoch_t get_last_peering_reset() const { return last_peering_reset; }
  
  //int  get_state() const { return state; }
  bool state_test(int m) const { return (state & m) != 0; }
  virtual void state_set(int m) { state |= m; }
  virtual void state_clear(int m) { state &= ~m; }

  bool is_complete() const { return info.last_complete == info.last_update; }
  virtual bool should_send_notify() const { return send_notify; }
  virtual void set_send_notify() { send_notify = is_replica(); }

  int get_state() const { return state; }
  virtual bool       is_active() const { return state_test(PG_STATE_ACTIVE); }
  virtual bool       is_peering() const { return state_test(PG_STATE_PEERING); }
  bool       is_down() const { return state_test(PG_STATE_DOWN); }
  bool       is_replay() const { return state_test(PG_STATE_REPLAY); }
  virtual bool       is_clean() const { return state_test(PG_STATE_CLEAN); }
  virtual bool       is_degraded() const { return state_test(PG_STATE_DEGRADED); }

  bool       is_scrubbing() const { return state_test(PG_STATE_SCRUBBING); }

  bool  is_empty() const { return info.last_update == eversion_t(0,0); }

  void init(
    int role,
    vector<int>& up,
    vector<int>& acting,
    pg_history_t& history,
    pg_interval_map_t& pim,
    bool backfill,
    ObjectStore::Transaction *t);

  // pg on-disk state
  void do_pending_flush();

private:
  void write_info(ObjectStore::Transaction& t);

public:
  static int _write_info(ObjectStore::Transaction& t, epoch_t epoch,
    pg_info_t &info, coll_t coll,
    map<epoch_t,pg_interval_t> &past_intervals,
    interval_set<snapid_t> &snap_collections,
    hobject_t &infos_oid,
    __u8 info_struct_v, bool dirty_big_info, bool force_ver = false);
  void write_if_dirty(ObjectStore::Transaction& t);

  void add_log_entry(pg_log_entry_t& e, bufferlist& log_bl);
  void append_log(
    vector<pg_log_entry_t>& logv, eversion_t trim_to, ObjectStore::Transaction &t);
  virtual bool check_log_for_corruption(ObjectStore *store);
  void trim_peers();

  std::string get_corrupt_pg_log_name() const;
  static int read_info(
    ObjectStore *store, const coll_t coll,
    bufferlist &bl, pg_info_t &info, map<epoch_t,pg_interval_t> &past_intervals,
    hobject_t &biginfo_oid, hobject_t &infos_oid,
    interval_set<snapid_t>  &snap_collections, __u8 &);
  void read_state(ObjectStore *store, bufferlist &bl);
  static epoch_t peek_map_epoch(ObjectStore *store, coll_t coll,
                               hobject_t &infos_oid, bufferlist *bl);
  void update_snap_map(
    vector<pg_log_entry_t> &log_entries,
    ObjectStore::Transaction& t);

  void filter_snapc(SnapContext& snapc);

  void log_weirdness();

  virtual void queue_snap_trim();
  bool queue_scrub();

  /// share pg info after a pg is active
  virtual void share_pg_info();
  /// share new pg log entries after a pg is active
  void share_pg_log();

  virtual void start_peering_interval(const OSDMapRef lastmap,
			      const vector<int>& newup,
			      const vector<int>& newacting);
  virtual void start_flush(ObjectStore::Transaction *t,
		   list<Context *> *on_applied,
		   list<Context *> *on_safe);
  virtual void set_last_peering_reset();
  bool pg_has_reset_since(epoch_t e) {
    assert(is_locked());
    return deleting || e < get_last_peering_reset();
  }

  virtual void update_history_from_master(pg_history_t new_history);
  virtual void fulfill_info(int from, const pg_query_t &query, 
		    pair<int, pg_info_t> &notify_info);
  virtual void fulfill_log(int from, const pg_query_t &query, epoch_t query_epoch);
  virtual bool is_split(OSDMapRef lastmap, OSDMapRef nextmap);
  virtual bool acting_up_affected(const vector<int>& newup, const vector<int>& newacting);

  // OpRequest queueing
  bool can_discard_op(OpRequestRef op);
  bool can_discard_scan(OpRequestRef op);
  bool can_discard_subop(OpRequestRef op);
  bool can_discard_backfill(OpRequestRef op);
  bool can_discard_request(OpRequestRef op);

  static bool op_must_wait_for_map(OSDMapRef curmap, OpRequestRef op);

  static bool split_request(OpRequestRef op, unsigned match, unsigned bits);

  bool old_peering_msg(epoch_t reply_epoch, epoch_t query_epoch);
  bool old_peering_evt(CephPeeringEvtRef evt) {
    return old_peering_msg(evt->get_epoch_sent(), evt->get_epoch_requested());
  }
  static bool have_same_or_newer_map(OSDMapRef osdmap, epoch_t e) {
    return e <= osdmap->get_epoch();
  }
  bool have_same_or_newer_map(epoch_t e) {
    return e <= get_osdmap()->get_epoch();
  }

  bool op_has_sufficient_caps(OpRequestRef op);


  // recovery bits
  virtual void take_waiters();
  virtual void queue_peering_event(CephPeeringEvtRef evt);
  void handle_peering_event(CephPeeringEvtRef evt, RecoveryCtx *rctx);
  void queue_notify(epoch_t msg_epoch, epoch_t query_epoch,
		    int from, pg_notify_t& i);
  void queue_info(epoch_t msg_epoch, epoch_t query_epoch,
		  int from, pg_info_t& i);
  void queue_log(epoch_t msg_epoch, epoch_t query_epoch, int from,
		 MOSDPGLog *msg);
  void queue_query(epoch_t msg_epoch, epoch_t query_epoch,
		   int from, const pg_query_t& q);
  void queue_null(epoch_t msg_epoch, epoch_t query_epoch);
  void queue_flushed(epoch_t started_at);
  void handle_advance_map(OSDMapRef osdmap, OSDMapRef lastmap,
			  vector<int>& newup, vector<int>& newacting,
			  RecoveryCtx *rctx);
  void handle_activate_map(RecoveryCtx *rctx);
  void handle_create(RecoveryCtx *rctx);
  void handle_loaded(RecoveryCtx *rctx);
  void handle_query_state(Formatter *f);

  virtual void on_removal(ObjectStore::Transaction *t) = 0;


  // abstract bits
  void do_request(OpRequestRef op);

  virtual void do_op(OpRequestRef op) = 0;
  virtual void do_sub_op(OpRequestRef op) = 0;
  virtual void do_sub_op_reply(OpRequestRef op) = 0;
  virtual void do_scan(OpRequestRef op) = 0;
  virtual void do_backfill(OpRequestRef op) = 0;
  virtual void snap_trimmer() = 0;

  virtual int do_command(vector<string>& cmd, ostream& ss,
			 bufferlist& idata, bufferlist& odata) = 0;

  virtual bool same_for_read_since(epoch_t e) = 0;
  virtual bool same_for_modify_since(epoch_t e) = 0;
  virtual bool same_for_rep_modify_since(epoch_t e) = 0;

  virtual void on_role_change() = 0;
  virtual void on_change() = 0;
  virtual void on_activate() = 0;
  virtual void on_shutdown() = 0;
  virtual void check_blacklisted_watchers() = 0;
  virtual void get_watchers(std::list<obj_watch_item_t>&) = 0;

  virtual ostream& operator<<(ostream& out) const;
};

ostream& operator<<(ostream& out, const PG& pg);

#endif
