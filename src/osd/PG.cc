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

#include "PG.h"
#include "common/errno.h"
#include "common/config.h"
#include "OSD.h"
#include "OpRequest.h"

#include "common/Timer.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGBackfill.h"
#include "messages/MBackfillReserve.h"

#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"
#include "common/BackTrace.h"

#include <sstream>

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, const PG *pg) 
{
  return *_dout << pg->gen_prefix();
}

void PG::get(const string &tag) 
{
  ref.inc();
#ifdef PG_DEBUG_REFS
  Mutex::Locker l(_ref_id_lock);
  if (!_tag_counts.count(tag)) {
    _tag_counts[tag] = 0;
  }
  _tag_counts[tag]++;
#endif
}

void PG::put(const string &tag)
{
#ifdef PG_DEBUG_REFS
  {
    Mutex::Locker l(_ref_id_lock);
    assert(_tag_counts.count(tag));
    _tag_counts[tag]--;
    if (_tag_counts[tag] == 0) {
      _tag_counts.erase(tag);
    }
  }
#endif
  if (ref.dec() == 0)
    delete this;
}

#ifdef PG_DEBUG_REFS
uint64_t PG::get_with_id()
{
  ref.inc();
  Mutex::Locker l(_ref_id_lock);
  uint64_t id = ++_ref_id;
  BackTrace bt(0);
  stringstream ss;
  bt.print(ss);
  dout(20) << __func__ << ": " << info.pgid << " got id " << id << dendl;
  assert(!_live_ids.count(id));
  _live_ids.insert(make_pair(id, ss.str()));
  return id;
}

void PG::put_with_id(uint64_t id)
{
  dout(20) << __func__ << ": " << info.pgid << " put id " << id << dendl;
  {
    Mutex::Locker l(_ref_id_lock);
    assert(_live_ids.count(id));
    _live_ids.erase(id);
  }
  if (ref.dec() == 0)
    delete this;
}

void PG::dump_live_ids()
{
  Mutex::Locker l(_ref_id_lock);
  dout(0) << "\t" << __func__ << ": " << info.pgid << " live ids:" << dendl;
  for (map<uint64_t, string>::iterator i = _live_ids.begin();
       i != _live_ids.end();
       ++i) {
    dout(0) << "\t\tid: " << *i << dendl;
  }
  dout(0) << "\t" << __func__ << ": " << info.pgid << " live tags:" << dendl;
  for (map<string, uint64_t>::iterator i = _tag_counts.begin();
       i != _tag_counts.end();
       ++i) {
    dout(0) << "\t\tid: " << *i << dendl;
  }
}
#endif

void PGPool::update(OSDMapRef map)
{
  const pg_pool_t *pi = map->get_pg_pool(id);
  assert(pi);
  info = *pi;
  auid = pi->auid;
  name = map->get_pool_name(id);
  if (pi->get_snap_epoch() == map->get_epoch()) {
    pi->build_removed_snaps(newly_removed_snaps);
    newly_removed_snaps.subtract(cached_removed_snaps);
    cached_removed_snaps.union_of(newly_removed_snaps);
    snapc = pi->get_snap_context();
  } else {
    newly_removed_snaps.clear();
  }
}

PG::PG(OSDService *o, OSDMapRef curmap,
       const PGPool &_pool, pg_t p, const hobject_t& loid,
       const hobject_t& ioid) :
  osd(o),
  osdriver(osd->store, coll_t(), OSD::make_snapmapper_oid()),
  snap_mapper(
    &osdriver,
    p.m_seed,
    p.get_split_bits(curmap->get_pg_num(_pool.id)),
    _pool.id),
  map_lock("PG::map_lock"),
  osdmap_ref(curmap), last_persisted_osdmap_ref(curmap), pool(_pool),
  _lock("PG::_lock"),
  ref(0),
  #ifdef PG_DEBUG_REFS
  _ref_id_lock("PG::_ref_id_lock"), _ref_id(0),
  #endif
  deleting(false), dirty_info(false), dirty_big_info(false),
  info(p),
  info_struct_v(0),
  coll(p), log_oid(loid), biginfo_oid(ioid),
  recovery_item(this), scrub_item(this), scrub_finalize_item(this), snap_trim_item(this), stat_queue_item(this),
  recovery_ops_active(0),
  waiting_on_backfill(0),
  role(0),
  state(0),
  send_notify(false),
  need_up_thru(false),
  need_flush(false),
  last_peering_reset(0),
  heartbeat_peer_lock("PG::heartbeat_peer_lock"),
  backfill_target(-1),
  backfill_reserved(0),
  backfill_reserving(0),
  flushed(false),
  pg_stats_publish_lock("PG::pg_stats_publish_lock"),
  pg_stats_publish_valid(false),
  osr(osd->osr_registry.lookup_or_create(p, (stringify(p)))),
  finish_sync_event(NULL),
  scrub_after_recovery(false),
  active_pushes(0),
  recovery_state(this)
{
#ifdef PG_DEBUG_REFS
  osd->add_pgid(p, this);
#endif
}

PG::~PG()
{
#ifdef PG_DEBUG_REFS
  osd->remove_pgid(info.pgid, this);
#endif
}

void PG::lock(bool no_lockdep)
{
  _lock.Lock(no_lockdep);
  // if we have unrecorded dirty state with the lock dropped, there is a bug
  assert(!dirty_info);
  assert(!dirty_big_info);

  dout(30) << "lock" << dendl;
}

std::string PG::gen_prefix() const
{
  stringstream out;
  OSDMapRef mapref = osdmap_ref;
  if (_lock.is_locked_by_me()) {
    out << "osd." << osd->whoami
	<< " pg_epoch: " << (mapref ? mapref->get_epoch():0)
	<< " " << *this << " ";
  } else {
    out << "osd." << osd->whoami
	<< " pg_epoch: " << (mapref ? mapref->get_epoch():0)
	<< " pg[" << info.pgid << "(unlocked)] ";
  }
  return out.str();
}
  
/********* PG **********/

void PG::proc_master_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog, pg_missing_t& omissing, int from)
{
  dout(10) << "proc_master_log for osd." << from << ": " << olog << " " << omissing << dendl;
  assert(!is_active() && is_primary());

  // merge log into our own log to build master log.  no need to
  // make any adjustments to their missing map; we are taking their
  // log to be authoritative (i.e., their entries are by definitely
  // non-divergent).
  merge_log(t, oinfo, olog, from);
  peer_info[from] = oinfo;
  dout(10) << " peer osd." << from << " now " << oinfo << " " << omissing << dendl;
  might_have_unfound.insert(from);

  search_for_missing(oinfo, &omissing, from);
  peer_missing[from].swap(omissing);
}
    
void PG::proc_replica_log(ObjectStore::Transaction& t,
			  pg_info_t &oinfo, pg_log_t &olog, pg_missing_t& omissing, int from)
{
  dout(10) << "proc_replica_log for osd." << from << ": "
	   << oinfo << " " << olog << " " << omissing << dendl;

  pg_log.proc_replica_log(t, oinfo, olog, omissing, from);

  peer_info[from] = oinfo;
  dout(10) << " peer osd." << from << " now " << oinfo << " " << omissing << dendl;
  might_have_unfound.insert(from);

  search_for_missing(oinfo, &omissing, from);
  for (map<hobject_t, pg_missing_t::item>::iterator i = omissing.missing.begin();
       i != omissing.missing.end();
       ++i) {
    dout(20) << " after missing " << i->first << " need " << i->second.need
	     << " have " << i->second.have << dendl;
  }
  peer_missing[from].swap(omissing);
}

bool PG::proc_replica_info(int from, const pg_info_t &oinfo)
{
  map<int,pg_info_t>::iterator p = peer_info.find(from);
  if (p != peer_info.end() && p->second.last_update == oinfo.last_update) {
    dout(10) << " got dup osd." << from << " info " << oinfo << ", identical to ours" << dendl;
    return false;
  }

  dout(10) << " got osd." << from << " " << oinfo << dendl;
  assert(is_primary());
  peer_info[from] = oinfo;
  might_have_unfound.insert(from);
  
  unreg_next_scrub();
  if (info.history.merge(oinfo.history))
    dirty_info = true;
  reg_next_scrub();
  
  // stray?
  if (!is_acting(from)) {
    dout(10) << " osd." << from << " has stray content: " << oinfo << dendl;
    stray_set.insert(from);
    if (is_clean()) {
      purge_strays();
    }
  }

  // was this a new info?  if so, update peers!
  if (p == peer_info.end())
    update_heartbeat_peers();

  return true;
}

void PG::remove_snap_mapped_object(
  ObjectStore::Transaction& t, const hobject_t& soid)
{
  t.remove(coll, soid);
  OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
  if (soid.snap < CEPH_MAXSNAP) {
    int r = snap_mapper.remove_oid(
      soid,
      &_t);
    if (!(r == 0 || r == -ENOENT)) {
      derr << __func__ << ": remove_oid returned " << cpp_strerror(r) << dendl;
      assert(0);
    }
  }
}

void PG::merge_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog, int from)
{
  list<hobject_t> to_remove;
  pg_log.merge_log(t, oinfo, olog, from, info, to_remove, dirty_info, dirty_big_info);
  for(list<hobject_t>::iterator i = to_remove.begin();
      i != to_remove.end();
      ++i)
    remove_snap_mapped_object(t, *i);
}

void PG::rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead)
{
  list<hobject_t> to_remove;
  pg_log.rewind_divergent_log(t, newhead, info, to_remove, dirty_info, dirty_big_info);
  for(list<hobject_t>::iterator i = to_remove.begin();
      i != to_remove.end();
      ++i)
    remove_snap_mapped_object(t, *i);
}

/*
 * Process information from a replica to determine if it could have any
 * objects that i need.
 *
 * TODO: if the missing set becomes very large, this could get expensive.
 * Instead, we probably want to just iterate over our unfound set.
 */
bool PG::search_for_missing(const pg_info_t &oinfo, const pg_missing_t *omissing,
			    int fromosd)
{
  bool stats_updated = false;
  bool found_missing = false;

  // take note that we've probed this peer, for
  // all_unfound_are_queried_or_lost()'s benefit.
  peer_missing[fromosd];

  // found items?
  for (map<hobject_t,pg_missing_t::item>::const_iterator p = pg_log.get_missing().missing.begin();
       p != pg_log.get_missing().missing.end();
       ++p) {
    const hobject_t &soid(p->first);
    eversion_t need = p->second.need;
    if (oinfo.last_update < need) {
      dout(10) << "search_for_missing " << soid << " " << need
	       << " also missing on osd." << fromosd
	       << " (last_update " << oinfo.last_update << " < needed " << need << ")"
	       << dendl;
      continue;
    }
    if (p->first >= oinfo.last_backfill) {
      // FIXME: this is _probably_ true, although it could conceivably
      // be in the undefined region!  Hmm!
      dout(10) << "search_for_missing " << soid << " " << need
	       << " also missing on osd." << fromosd
	       << " (past last_backfill " << oinfo.last_backfill << ")"
	       << dendl;
      continue;
    }
    if (oinfo.last_complete < need) {
      if (!omissing) {
	// We know that the peer lacks some objects at the revision we need.
	// Without the peer's missing set, we don't know whether it has this
	// particular object or not.
	dout(10) << __func__ << " " << soid << " " << need
		 << " might also be missing on osd." << fromosd << dendl;
	continue;
      }

      if (omissing->is_missing(soid)) {
	dout(10) << "search_for_missing " << soid << " " << need
		 << " also missing on osd." << fromosd << dendl;
	continue;
      }
    }
    dout(10) << "search_for_missing " << soid << " " << need
	     << " is on osd." << fromosd << dendl;

    map<hobject_t, set<int> >::iterator ml = missing_loc.find(soid);
    if (ml == missing_loc.end()) {
      map<hobject_t, list<OpRequestRef> >::iterator wmo =
	waiting_for_missing_object.find(soid);
      if (wmo != waiting_for_missing_object.end()) {
	requeue_ops(wmo->second);
      }
      stats_updated = true;
      missing_loc[soid].insert(fromosd);
      missing_loc_sources.insert(fromosd);
    }
    else {
      ml->second.insert(fromosd);
      missing_loc_sources.insert(fromosd);
    }
    found_missing = true;
  }
  if (stats_updated) {
    publish_stats_to_osd();
  }

  dout(20) << "search_for_missing missing " << pg_log.get_missing().missing << dendl;
  return found_missing;
}

void PG::discover_all_missing(map< int, map<pg_t,pg_query_t> > &query_map)
{
  const pg_missing_t &missing = pg_log.get_missing();
  assert(missing.have_missing());

  dout(10) << __func__ << " "
	   << missing.num_missing() << " missing, "
	   << get_num_unfound() << " unfound"
	   << dendl;

  std::set<int>::const_iterator m = might_have_unfound.begin();
  std::set<int>::const_iterator mend = might_have_unfound.end();
  for (; m != mend; ++m) {
    int peer(*m);
    
    if (!get_osdmap()->is_up(peer)) {
      dout(20) << __func__ << " skipping down osd." << peer << dendl;
      continue;
    }

    // If we've requested any of this stuff, the pg_missing_t information
    // should be on its way.
    // TODO: coalsce requested_* into a single data structure
    if (peer_missing.find(peer) != peer_missing.end()) {
      dout(20) << __func__ << ": osd." << peer
	       << ": we already have pg_missing_t" << dendl;
      continue;
    }
    if (peer_log_requested.find(peer) != peer_log_requested.end()) {
      dout(20) << __func__ << ": osd." << peer
	       << ": in peer_log_requested" << dendl;
      continue;
    }
    if (peer_missing_requested.find(peer) != peer_missing_requested.end()) {
      dout(20) << __func__ << ": osd." << peer
	       << ": in peer_missing_requested" << dendl;
      continue;
    }

    // Request missing
    dout(10) << __func__ << ": osd." << peer << ": requesting pg_missing_t"
	     << dendl;
    peer_missing_requested.insert(peer);
    query_map[peer][info.pgid] =
      pg_query_t(pg_query_t::MISSING, info.history, get_osdmap()->get_epoch());
  }
}

/******* PG ***********/
bool PG::needs_recovery() const
{
  assert(is_primary());

  bool ret = false;

  const pg_missing_t &missing = pg_log.get_missing();

  if (missing.num_missing()) {
    dout(10) << __func__ << " primary has " << missing.num_missing() << dendl;
    ret = true;
  }

  vector<int>::const_iterator end = acting.end();
  vector<int>::const_iterator a = acting.begin();
  assert(a != end);
  ++a;
  for (; a != end; ++a) {
    int peer = *a;
    map<int, pg_missing_t>::const_iterator pm = peer_missing.find(peer);
    if (pm == peer_missing.end()) {
      dout(10) << __func__ << " osd." << peer << " don't have missing set" << dendl;
      ret = true;
      continue;
    }
    if (pm->second.num_missing()) {
      dout(10) << __func__ << " osd." << peer << " has " << pm->second.num_missing() << " missing" << dendl;
      ret = true;
    }
  }

  if (!ret)
    dout(10) << __func__ << " is recovered" << dendl;
  return ret;
}

bool PG::needs_backfill() const
{
  assert(is_primary());

  bool ret = false;

  vector<int>::const_iterator end = acting.end();
  vector<int>::const_iterator a = acting.begin();
  assert(a != end);
  ++a;
  for (; a != end; ++a) {
    int peer = *a;
    map<int,pg_info_t>::const_iterator pi = peer_info.find(peer);
    if (pi->second.last_backfill != hobject_t::get_max()) {
      dout(10) << __func__ << " osd." << peer << " has last_backfill " << pi->second.last_backfill << dendl;
      ret = true;
    }
  }

  if (!ret)
    dout(10) << __func__ << " does not need backfill" << dendl;
  return ret;
}

bool PG::_calc_past_interval_range(epoch_t *start, epoch_t *end)
{
  *end = info.history.same_interval_since;

  // Do we already have the intervals we want?
  map<epoch_t,pg_interval_t>::const_iterator pif = past_intervals.begin();
  if (pif != past_intervals.end()) {
    if (pif->first <= info.history.last_epoch_clean) {
      dout(10) << __func__ << ": already have past intervals back to "
	       << info.history.last_epoch_clean << dendl;
      return false;
    }
    *end = past_intervals.begin()->first;
  }

  *start = MAX(MAX(info.history.epoch_created,
		   info.history.last_epoch_clean),
	       osd->get_superblock().oldest_map);
  if (*start >= *end) {
    dout(10) << __func__ << " start epoch " << *start << " >= end epoch " << *end
	     << ", nothing to do" << dendl;
    return false;
  }

  return true;
}


void PG::generate_past_intervals()
{
  epoch_t cur_epoch, end_epoch;
  if (!_calc_past_interval_range(&cur_epoch, &end_epoch)) {
    return;
  }

  OSDMapRef last_map, cur_map;
  vector<int> acting, up, old_acting, old_up;

  cur_map = osd->get_map(cur_epoch);
  cur_map->pg_to_up_acting_osds(get_pgid(), up, acting);
  epoch_t same_interval_since = cur_epoch;
  dout(10) << __func__ << " over epochs " << cur_epoch << "-"
	   << end_epoch << dendl;
  ++cur_epoch;
  for (; cur_epoch <= end_epoch; ++cur_epoch) {
    last_map.swap(cur_map);
    old_up.swap(up);
    old_acting.swap(acting);

    cur_map = osd->get_map(cur_epoch);
    cur_map->pg_to_up_acting_osds(get_pgid(), up, acting);

    std::stringstream debug;
    bool new_interval = pg_interval_t::check_new_interval(
      old_acting,
      acting,
      old_up,
      up,
      same_interval_since,
      info.history.last_epoch_clean,
      cur_map,
      last_map,
      info.pgid.pool(),
      info.pgid,
      &past_intervals,
      &debug);
    if (new_interval) {
      dout(10) << debug.str() << dendl;
      same_interval_since = cur_epoch;
    }
  }

  // record our work.
  dirty_info = true;
  dirty_big_info = true;
}

/*
 * Trim past_intervals.
 *
 * This gets rid of all the past_intervals that happened before last_epoch_clean.
 */
void PG::trim_past_intervals()
{
  std::map<epoch_t,pg_interval_t>::iterator pif = past_intervals.begin();
  std::map<epoch_t,pg_interval_t>::iterator end = past_intervals.end();
  while (pif != end) {
    if (pif->second.last >= info.history.last_epoch_clean)
      return;
    dout(10) << __func__ << ": trimming " << pif->second << dendl;
    past_intervals.erase(pif++);
    dirty_big_info = true;
  }
}


bool PG::adjust_need_up_thru(const OSDMapRef osdmap)
{
  epoch_t up_thru = get_osdmap()->get_up_thru(osd->whoami);
  if (need_up_thru &&
      up_thru >= info.history.same_interval_since) {
    dout(10) << "adjust_need_up_thru now " << up_thru << ", need_up_thru now false" << dendl;
    need_up_thru = false;
    return true;
  }
  return false;
}

void PG::remove_down_peer_info(const OSDMapRef osdmap)
{
  // Remove any downed osds from peer_info
  bool removed = false;
  map<int,pg_info_t>::iterator p = peer_info.begin();
  while (p != peer_info.end()) {
    if (!osdmap->is_up(p->first)) {
      dout(10) << " dropping down osd." << p->first << " info " << p->second << dendl;
      peer_missing.erase(p->first);
      peer_log_requested.erase(p->first);
      peer_missing_requested.erase(p->first);
      peer_info.erase(p++);
      removed = true;
    } else
      ++p;
  }

  // if we removed anyone, update peers (which include peer_info)
  if (removed)
    update_heartbeat_peers();
  check_recovery_sources(osdmap);
}

/*
 * Returns true unless there is a non-lost OSD in might_have_unfound.
 */
bool PG::all_unfound_are_queried_or_lost(const OSDMapRef osdmap) const
{
  assert(is_primary());

  set<int>::const_iterator peer = might_have_unfound.begin();
  set<int>::const_iterator mend = might_have_unfound.end();
  for (; peer != mend; ++peer) {
    if (peer_missing.count(*peer))
      continue;
    const osd_info_t &osd_info(osdmap->get_info(*peer));
    if (osd_info.lost_at <= osd_info.up_from) {
      // If there is even one OSD in might_have_unfound that isn't lost, we
      // still might retrieve our unfound.
      return false;
    }
  }
  dout(10) << "all_unfound_are_queried_or_lost all of might_have_unfound " << might_have_unfound 
	   << " have been queried or are marked lost" << dendl;
  return true;
}

void PG::build_prior(std::auto_ptr<PriorSet> &prior_set)
{
  if (1) {
    // sanity check
    for (map<int,pg_info_t>::iterator it = peer_info.begin();
	 it != peer_info.end();
	 ++it) {
      assert(info.history.last_epoch_started >= it->second.history.last_epoch_started);
    }
  }
  prior_set.reset(new PriorSet(*get_osdmap(),
				 past_intervals,
				 up,
				 acting,
				 info,
				 this));
  PriorSet &prior(*prior_set.get());
				 
  if (prior.pg_down) {
    state_set(PG_STATE_DOWN);
  }

  if (get_osdmap()->get_up_thru(osd->whoami) < info.history.same_interval_since) {
    dout(10) << "up_thru " << get_osdmap()->get_up_thru(osd->whoami)
	     << " < same_since " << info.history.same_interval_since
	     << ", must notify monitor" << dendl;
    need_up_thru = true;
  } else {
    dout(10) << "up_thru " << get_osdmap()->get_up_thru(osd->whoami)
	     << " >= same_since " << info.history.same_interval_since
	     << ", all is well" << dendl;
    need_up_thru = false;
  }
  set_probe_targets(prior_set->probe);
}

void PG::clear_primary_state()
{
  dout(10) << "clear_primary_state" << dendl;

  // clear peering state
  stray_set.clear();
  peer_log_requested.clear();
  peer_missing_requested.clear();
  peer_info.clear();
  peer_missing.clear();
  need_up_thru = false;
  peer_last_complete_ondisk.clear();
  peer_activated.clear();
  min_last_complete_ondisk = eversion_t();
  pg_trim_to = eversion_t();
  stray_purged.clear();
  might_have_unfound.clear();

  last_update_ondisk = eversion_t();

  snap_trimq.clear();

  finish_sync_event = 0;  // so that _finish_recvoery doesn't go off in another thread

  missing_loc.clear();
  missing_loc_sources.clear();

  pg_log.reset_recovery_pointers();

  scrubber.reserved_peers.clear();
  scrub_after_recovery = false;

  osd->recovery_wq.dequeue(this);
  osd->snap_trim_wq.dequeue(this);
}

/**
 * find_best_info
 *
 * Returns an iterator to the best info in infos sorted by:
 *  1) Prefer newer last_update
 *  2) Prefer longer tail if it brings another info into contiguity
 *  3) Prefer current primary
 */
map<int, pg_info_t>::const_iterator PG::find_best_info(const map<int, pg_info_t> &infos) const
{
  eversion_t min_last_update_acceptable = eversion_t::max();
  epoch_t max_last_epoch_started_found = 0;
  for (map<int, pg_info_t>::const_iterator i = infos.begin();
       i != infos.end();
       ++i) {
    if (max_last_epoch_started_found < i->second.last_epoch_started) {
      min_last_update_acceptable = eversion_t::max();
      max_last_epoch_started_found = i->second.last_epoch_started;
    }
    if (max_last_epoch_started_found == i->second.last_epoch_started) {
      if (min_last_update_acceptable > i->second.last_update)
	min_last_update_acceptable = i->second.last_update;
    }
  }
  assert(min_last_update_acceptable != eversion_t::max());

  map<int, pg_info_t>::const_iterator best = infos.end();
  // find osd with newest last_update.  if there are multiples, prefer
  //  - a longer tail, if it brings another peer into log contiguity
  //  - the current primary
  for (map<int, pg_info_t>::const_iterator p = infos.begin();
       p != infos.end();
       ++p) {
    // Only consider peers with last_update >= min_last_update_acceptable
    if (p->second.last_update < min_last_update_acceptable)
      continue;
    // Disquality anyone who is incomplete (not fully backfilled)
    if (p->second.is_incomplete())
      continue;
    if (best == infos.end()) {
      best = p;
      continue;
    }
    // Prefer newer last_update
    if (p->second.last_update < best->second.last_update)
      continue;
    if (p->second.last_update > best->second.last_update) {
      best = p;
      continue;
    }
    // Prefer longer tail if it brings another peer into contiguity
    for (map<int, pg_info_t>::const_iterator q = infos.begin();
	 q != infos.end();
	 ++q) {
      if (q->second.is_incomplete())
	continue;  // don't care about log contiguity
      if (q->second.last_update < best->second.log_tail &&
	  q->second.last_update >= p->second.log_tail) {
	dout(10) << "calc_acting prefer osd." << p->first
		 << " because it brings osd." << q->first << " into log contiguity" << dendl;
	best = p;
	continue;
      }
      if (q->second.last_update < p->second.log_tail &&
	  q->second.last_update >= best->second.log_tail) {
	continue;
      }
    }
    // prefer current primary (usually the caller), all things being equal
    if (p->first == acting[0]) {
      dout(10) << "calc_acting prefer osd." << p->first
	       << " because it is current primary" << dendl;
      best = p;
      continue;
    }
  }
  return best;
}

/**
 * calculate the desired acting set.
 *
 * Choose an appropriate acting set.  Prefer up[0], unless it is
 * incomplete, or another osd has a longer tail that allows us to
 * bring other up nodes up to date.
 */
bool PG::calc_acting(int& newest_update_osd_id, vector<int>& want) const
{
  map<int, pg_info_t> all_info(peer_info.begin(), peer_info.end());
  all_info[osd->whoami] = info;

  for (map<int,pg_info_t>::iterator p = all_info.begin(); p != all_info.end(); ++p) {
    dout(10) << "calc_acting osd." << p->first << " " << p->second << dendl;
  }

  map<int, pg_info_t>::const_iterator newest_update_osd = find_best_info(all_info);

  if (newest_update_osd == all_info.end()) {
    if (up != acting) {
      dout(10) << "calc_acting no suitable info found (incomplete backfills?), reverting to up" << dendl;
      want = up;
      return true;
    } else {
      dout(10) << "calc_acting no suitable info found (incomplete backfills?)" << dendl;
      return false;
    }
  }


  dout(10) << "calc_acting newest update on osd." << newest_update_osd->first
	   << " with " << newest_update_osd->second << dendl;
  newest_update_osd_id = newest_update_osd->first;
  
  // select primary
  map<int,pg_info_t>::const_iterator primary;
  if (up.size() &&
      !all_info[up[0]].is_incomplete() &&
      all_info[up[0]].last_update >= newest_update_osd->second.log_tail) {
    dout(10) << "up[0](osd." << up[0] << ") selected as primary" << dendl;
    primary = all_info.find(up[0]);         // prefer up[0], all thing being equal
  } else if (!newest_update_osd->second.is_incomplete()) {
    dout(10) << "up[0] needs backfill, osd." << newest_update_osd_id
	     << " selected as primary instead" << dendl;
    primary = newest_update_osd;
  } else {
    map<int, pg_info_t> complete_infos;
    for (map<int, pg_info_t>::iterator i = all_info.begin();
	 i != all_info.end();
	 ++i) {
      if (!i->second.is_incomplete())
	complete_infos.insert(*i);
    }
    primary = find_best_info(complete_infos);
    if (primary == complete_infos.end() ||
	primary->second.last_update < newest_update_osd->second.log_tail) {
      dout(10) << "calc_acting no acceptable primary, reverting to up " << up << dendl;
      want = up;
      return true;
    } else {
      dout(10) << "up[0] and newest_update_osd need backfill, osd."
	       << newest_update_osd_id
	       << " selected as primary instead" << dendl;
    }
  }


  dout(10) << "calc_acting primary is osd." << primary->first
	   << " with " << primary->second << dendl;
  want.push_back(primary->first);
  unsigned usable = 1;
  unsigned backfill = 0;

  // select replicas that have log contiguity with primary.
  // prefer up, then acting, then any peer_info osds 
  for (vector<int>::const_iterator i = up.begin();
       i != up.end();
       ++i) {
    if (*i == primary->first)
      continue;
    const pg_info_t &cur_info = all_info.find(*i)->second;
    if (cur_info.is_incomplete() || cur_info.last_update < primary->second.log_tail) {
      if (backfill < 1) {
	dout(10) << " osd." << *i << " (up) accepted (backfill) " << cur_info << dendl;
	want.push_back(*i);
	backfill++;
      } else {
	dout(10) << " osd." << *i << " (up) rejected" << cur_info << dendl;
      }
    } else {
      want.push_back(*i);
      usable++;
      dout(10) << " osd." << *i << " (up) accepted " << cur_info << dendl;
    }
  }

  for (vector<int>::const_iterator i = acting.begin();
       i != acting.end();
       ++i) {
    if (usable >= get_osdmap()->get_pg_size(info.pgid))
      break;

    // skip up osds we already considered above
    if (*i == primary->first)
      continue;
    vector<int>::const_iterator up_it = find(up.begin(), up.end(), *i);
    if (up_it != up.end())
      continue;

    const pg_info_t &cur_info = all_info.find(*i)->second;
    if (cur_info.is_incomplete() || cur_info.last_update < primary->second.log_tail) {
      dout(10) << " osd." << *i << " (stray) REJECTED " << cur_info << dendl;
    } else {
      want.push_back(*i);
      dout(10) << " osd." << *i << " (stray) accepted " << cur_info << dendl;
      usable++;
    }
  }

  for (map<int,pg_info_t>::const_iterator i = all_info.begin();
       i != all_info.end();
       ++i) {
    if (usable >= get_osdmap()->get_pg_size(info.pgid))
      break;

    // skip up osds we already considered above
    if (i->first == primary->first)
      continue;
    vector<int>::const_iterator up_it = find(up.begin(), up.end(), i->first);
    if (up_it != up.end())
      continue;
    vector<int>::const_iterator acting_it = find(acting.begin(), acting.end(), i->first);
    if (acting_it != acting.end())
      continue;

    if (i->second.is_incomplete() || i->second.last_update < primary->second.log_tail) {
      dout(10) << " osd." << i->first << " (stray) REJECTED " << i->second << dendl;
    } else {
      want.push_back(i->first);
      dout(10) << " osd." << i->first << " (stray) accepted " << i->second << dendl;
      usable++;
    }
  }

  return true;
}

/**
 * choose acting
 *
 * calculate the desired acting, and request a change with the monitor
 * if it differs from the current acting.
 */
bool PG::choose_acting(int& newest_update_osd)
{
  vector<int> want;

  if (!calc_acting(newest_update_osd, want)) {
    dout(10) << "choose_acting failed" << dendl;
    assert(want_acting.empty());
    return false;
  }

  if (want.size() < pool.info.min_size) {
    want_acting.clear();
    return false;
  }

  if (want != acting) {
    dout(10) << "choose_acting want " << want << " != acting " << acting
	     << ", requesting pg_temp change" << dendl;
    want_acting = want;
    if (want == up) {
      vector<int> empty;
      osd->queue_want_pg_temp(info.pgid, empty);
    } else
      osd->queue_want_pg_temp(info.pgid, want);
    return false;
  } else {
    want_acting.clear();
  }
  dout(10) << "choose_acting want " << want << " (== acting)" << dendl;
  return true;
}

/* Build the might_have_unfound set.
 *
 * This is used by the primary OSD during recovery.
 *
 * This set tracks the OSDs which might have unfound objects that the primary
 * OSD needs. As we receive pg_missing_t from each OSD in might_have_unfound, we
 * will remove the OSD from the set.
 */
void PG::build_might_have_unfound()
{
  assert(might_have_unfound.empty());
  assert(is_primary());

  dout(10) << __func__ << dendl;

  // Make sure that we have past intervals.
  generate_past_intervals();

  // We need to decide who might have unfound objects that we need
  std::map<epoch_t,pg_interval_t>::const_reverse_iterator p = past_intervals.rbegin();
  std::map<epoch_t,pg_interval_t>::const_reverse_iterator end = past_intervals.rend();
  for (; p != end; ++p) {
    const pg_interval_t &interval(p->second);
    // We already have all the objects that exist at last_epoch_clean,
    // so there's no need to look at earlier intervals.
    if (interval.last < info.history.last_epoch_clean)
      break;

    // If nothing changed, we don't care about this interval.
    if (!interval.maybe_went_rw)
      continue;

    std::vector<int>::const_iterator a = interval.acting.begin();
    std::vector<int>::const_iterator a_end = interval.acting.end();
    for (; a != a_end; ++a) {
      if (*a != osd->whoami)
	might_have_unfound.insert(*a);
    }
  }

  // include any (stray) peers
  for (map<int,pg_info_t>::iterator p = peer_info.begin();
       p != peer_info.end();
       ++p)
    might_have_unfound.insert(p->first);

  dout(15) << __func__ << ": built " << might_have_unfound << dendl;
}

struct C_PG_ActivateCommitted : public Context {
  PGRef pg;
  epoch_t epoch;
  C_PG_ActivateCommitted(PG *p, epoch_t e)
    : pg(p), epoch(e) {}
  void finish(int r) {
    pg->_activate_committed(epoch);
  }
};

void PG::activate(ObjectStore::Transaction& t,
		  epoch_t query_epoch,
		  list<Context*>& tfin,
		  map< int, map<pg_t,pg_query_t> >& query_map,
		  map<int, vector<pair<pg_notify_t, pg_interval_map_t> > > *activator_map)
{
  assert(!is_active());
  assert(scrubber.callbacks.empty());
  assert(callbacks_for_degraded_object.empty());

  // -- crash recovery?
  if (is_primary() &&
      pool.info.crash_replay_interval > 0 &&
      may_need_replay(get_osdmap())) {
    replay_until = ceph_clock_now(g_ceph_context);
    replay_until += pool.info.crash_replay_interval;
    dout(10) << "activate starting replay interval for " << pool.info.crash_replay_interval
	     << " until " << replay_until << dendl;
    state_set(PG_STATE_REPLAY);

    // TODOSAM: osd->osd-> is no good
    osd->osd->replay_queue_lock.Lock();
    osd->osd->replay_queue.push_back(pair<pg_t,utime_t>(info.pgid, replay_until));
    osd->osd->replay_queue_lock.Unlock();
  }

  // twiddle pg state
  state_set(PG_STATE_ACTIVE);
  state_clear(PG_STATE_DOWN);

  send_notify = false;

  info.last_epoch_started = query_epoch;

  const pg_missing_t &missing = pg_log.get_missing();

  if (is_primary()) {
    // If necessary, create might_have_unfound to help us find our unfound objects.
    // NOTE: It's important that we build might_have_unfound before trimming the
    // past intervals.
    might_have_unfound.clear();
    if (missing.have_missing()) {
      build_might_have_unfound();
    }
  }
 
  if (role == 0) {    // primary state
    last_update_ondisk = info.last_update;
    min_last_complete_ondisk = eversion_t(0,0);  // we don't know (yet)!
  }
  last_update_applied = info.last_update;


  need_up_thru = false;

  // write pg info, log
  dirty_info = true;
  dirty_big_info = true; // maybe

  // clean up stray objects
  clean_up_local(t); 

  // find out when we commit
  tfin.push_back(new C_PG_ActivateCommitted(this, query_epoch));
  
  // initialize snap_trimq
  if (is_primary()) {
    snap_trimq = pool.cached_removed_snaps;
    snap_trimq.subtract(info.purged_snaps);
    dout(10) << "activate - snap_trimq " << snap_trimq << dendl;
    if (!snap_trimq.empty() && is_clean())
      queue_snap_trim();
  }

  // init complete pointer
  if (missing.num_missing() == 0) {
    dout(10) << "activate - no missing, moving last_complete " << info.last_complete 
	     << " -> " << info.last_update << dendl;
    info.last_complete = info.last_update;
    pg_log.reset_recovery_pointers();
  } else {
    dout(10) << "activate - not complete, " << missing << dendl;
    pg_log.activate_not_complete(info);
    if (is_primary()) {
      dout(10) << "activate - starting recovery" << dendl;
      osd->queue_for_recovery(this);
      if (have_unfound())
	discover_all_missing(query_map);
    }
  }
    
  log_weirdness();

  // if primary..
  if (is_primary()) {
    // start up replicas

    // count replicas that are not backfilling
    unsigned active = 1;

    for (unsigned i=1; i<acting.size(); i++) {
      int peer = acting[i];
      assert(peer_info.count(peer));
      pg_info_t& pi = peer_info[peer];

      dout(10) << "activate peer osd." << peer << " " << pi << dendl;

      MOSDPGLog *m = 0;
      pg_missing_t& pm = peer_missing[peer];

      bool needs_past_intervals = pi.dne();

      if (pi.last_update == info.last_update) {
        // empty log
	if (!pi.is_empty() && activator_map) {
	  dout(10) << "activate peer osd." << peer << " is up to date, queueing in pending_activators" << dendl;
	  (*activator_map)[peer].push_back(
	    make_pair(
	      pg_notify_t(
		get_osdmap()->get_epoch(),
		get_osdmap()->get_epoch(),
		info),
	      past_intervals));
	} else {
	  dout(10) << "activate peer osd." << peer << " is up to date, but sending pg_log anyway" << dendl;
	  m = new MOSDPGLog(get_osdmap()->get_epoch(), info);
	}
      } else if (pg_log.get_tail() > pi.last_update || pi.last_backfill == hobject_t()) {
	// backfill
	osd->clog.info() << info.pgid << " restarting backfill on osd." << peer
			 << " from (" << pi.log_tail << "," << pi.last_update << "] " << pi.last_backfill
			 << " to " << info.last_update;

	pi.last_update = info.last_update;
	pi.last_complete = info.last_update;
	pi.last_backfill = hobject_t();
	pi.history = info.history;
	pi.stats.stats.clear();

	m = new MOSDPGLog(get_osdmap()->get_epoch(), pi);

	// send some recent log, so that op dup detection works well.
	m->log.copy_up_to(pg_log.get_log(), g_conf->osd_min_pg_log_entries);
	m->info.log_tail = m->log.tail;
	pi.log_tail = m->log.tail;  // sigh...

	pm.clear();
      } else {
	// catch up
	assert(pg_log.get_tail() <= pi.last_update);
	m = new MOSDPGLog(get_osdmap()->get_epoch(), info);
	// send new stuff to append to replicas log
	m->log.copy_after(pg_log.get_log(), pi.last_update);
      }

      // share past_intervals if we are creating the pg on the replica
      // based on whether our info for that peer was dne() *before*
      // updating pi.history in the backfill block above.
      if (needs_past_intervals)
	m->past_intervals = past_intervals;

      if (pi.last_backfill == hobject_t::get_max())
	active++;

      // update local version of peer's missing list!
      if (m && pi.last_backfill != hobject_t()) {
        for (list<pg_log_entry_t>::iterator p = m->log.log.begin();
             p != m->log.log.end();
             ++p)
	  if (p->soid <= pi.last_backfill)
	    pm.add_next_event(*p);
      }
      
      if (m) {
	dout(10) << "activate peer osd." << peer << " sending " << m->log << dendl;
	//m->log.print(cout);
	osd->send_message_osd_cluster(peer, m, get_osdmap()->get_epoch());
      }

      // peer now has 
      pi.last_update = info.last_update;

      // update our missing
      if (pm.num_missing() == 0) {
	pi.last_complete = pi.last_update;
        dout(10) << "activate peer osd." << peer << " " << pi << " uptodate" << dendl;
      } else {
        dout(10) << "activate peer osd." << peer << " " << pi << " missing " << pm << dendl;
      }
    }

    // degraded?
    if (get_osdmap()->get_pg_size(info.pgid) > active)
      state_set(PG_STATE_DEGRADED);

    // all clean?
    if (needs_recovery()) {
      dout(10) << "activate not all replicas are up-to-date, queueing recovery" << dendl;
      queue_peering_event(
        CephPeeringEvtRef(
          new CephPeeringEvt(
            get_osdmap()->get_epoch(),
            get_osdmap()->get_epoch(),
            DoRecovery())));
    } else if (needs_backfill()) {
      dout(10) << "activate queueing backfill" << dendl;
      queue_peering_event(
        CephPeeringEvtRef(
          new CephPeeringEvt(
            get_osdmap()->get_epoch(),
            get_osdmap()->get_epoch(),
            RequestBackfill())));
    } else {
      dout(10) << "activate all replicas clean, no recovery" << dendl;
      queue_peering_event(
        CephPeeringEvtRef(
          new CephPeeringEvt(
            get_osdmap()->get_epoch(),
            get_osdmap()->get_epoch(),
            AllReplicasRecovered())));
    }

    publish_stats_to_osd();
  }

  // we need to flush this all out before doing anything else..
  need_flush = true;

  // waiters
  if (!is_replay()) {
    requeue_ops(waiting_for_active);
  }

  on_activate();
}

void PG::do_pending_flush()
{
  assert(is_locked());
  if (need_flush) {
    dout(10) << "do_pending_flush doing pending flush" << dendl;
    osr->flush();
    need_flush = false;
    dout(10) << "do_pending_flush done" << dendl;
  }
}

bool PG::op_has_sufficient_caps(OpRequestRef op)
{
  // only check MOSDOp
  if (op->request->get_type() != CEPH_MSG_OSD_OP)
    return true;

  MOSDOp *req = static_cast<MOSDOp*>(op->request);

  OSD::Session *session = (OSD::Session *)req->get_connection()->get_priv();
  if (!session) {
    dout(0) << "op_has_sufficient_caps: no session for op " << *req << dendl;
    return false;
  }
  OSDCap& caps = session->caps;
  session->put();

  string key = req->get_object_locator().key;
  if (key.length() == 0)
    key = req->get_oid().name;

  bool cap = caps.is_capable(pool.name, pool.auid, key,
			     op->need_read_cap(),
			     op->need_write_cap(),
			     op->need_class_read_cap(),
			     op->need_class_write_cap());

  dout(20) << "op_has_sufficient_caps pool=" << pool.id << " (" << pool.name
	   << ") owner=" << pool.auid
	   << " need_read_cap=" << op->need_read_cap()
	   << " need_write_cap=" << op->need_write_cap()
	   << " need_class_read_cap=" << op->need_class_read_cap()
	   << " need_class_write_cap=" << op->need_class_write_cap()
	   << " -> " << (cap ? "yes" : "NO")
	   << dendl;
  return cap;
}

void PG::take_op_map_waiters()
{
  Mutex::Locker l(map_lock);
  for (list<OpRequestRef>::iterator i = waiting_for_map.begin();
       i != waiting_for_map.end();
       ) {
    if (op_must_wait_for_map(get_osdmap_with_maplock(), *i)) {
      break;
    } else {
      osd->op_wq.queue(make_pair(PGRef(this), *i));
      waiting_for_map.erase(i++);
    }
  }
}

void PG::queue_op(OpRequestRef op)
{
  Mutex::Locker l(map_lock);
  if (!waiting_for_map.empty()) {
    // preserve ordering
    waiting_for_map.push_back(op);
    return;
  }
  if (op_must_wait_for_map(get_osdmap_with_maplock(), op)) {
    waiting_for_map.push_back(op);
    return;
  }
  osd->op_wq.queue(make_pair(PGRef(this), op));
}

void PG::do_request(OpRequestRef op)
{
  // do any pending flush
  do_pending_flush();

  if (!op_has_sufficient_caps(op)) {
    osd->reply_op_error(op, -EPERM);
    return;
  }
  assert(!op_must_wait_for_map(get_osdmap(), op));
  if (can_discard_request(op)) {
    return;
  }
  if (!flushed) {
    dout(20) << " !flushed, waiting for active on " << op << dendl;
    waiting_for_active.push_back(op);
    return;
  }

  switch (op->request->get_type()) {
  case CEPH_MSG_OSD_OP:
    if (is_replay() || !is_active()) {
      dout(20) << " replay, waiting for active on " << op << dendl;
      waiting_for_active.push_back(op);
      return;
    }
    do_op(op); // do it now
    break;

  case MSG_OSD_SUBOP:
    do_sub_op(op);
    break;

  case MSG_OSD_SUBOPREPLY:
    do_sub_op_reply(op);
    break;

  case MSG_OSD_PG_SCAN:
    do_scan(op);
    break;

  case MSG_OSD_PG_BACKFILL:
    do_backfill(op);
    break;

  default:
    assert(0 == "bad message type in do_request");
  }
}


void PG::replay_queued_ops()
{
  assert(is_replay() && is_active());
  eversion_t c = info.last_update;
  list<OpRequestRef> replay;
  dout(10) << "replay_queued_ops" << dendl;
  state_clear(PG_STATE_REPLAY);

  for (map<eversion_t,OpRequestRef>::iterator p = replay_queue.begin();
       p != replay_queue.end();
       ++p) {
    if (p->first.version != c.version+1) {
      dout(10) << "activate replay " << p->first
	       << " skipping " << c.version+1 - p->first.version 
	       << " ops"
	       << dendl;      
      c = p->first;
    }
    dout(10) << "activate replay " << p->first << " "
             << *p->second->request << dendl;
    replay.push_back(p->second);
  }
  replay_queue.clear();
  requeue_ops(replay);
  requeue_ops(waiting_for_active);

  publish_stats_to_osd();
}

void PG::_activate_committed(epoch_t e)
{
  lock();
  if (pg_has_reset_since(e)) {
    dout(10) << "_activate_committed " << e << ", that was an old interval" << dendl;
  } else if (is_primary()) {
    peer_activated.insert(osd->whoami);
    dout(10) << "_activate_committed " << e << " peer_activated now " << peer_activated 
	     << " last_epoch_started " << info.history.last_epoch_started
	     << " same_interval_since " << info.history.same_interval_since << dendl;
    if (peer_activated.size() == acting.size())
      all_activated_and_committed();
  } else {
    dout(10) << "_activate_committed " << e << " telling primary" << dendl;
    MOSDPGInfo *m = new MOSDPGInfo(e);
    pg_notify_t i = pg_notify_t(get_osdmap()->get_epoch(),
				get_osdmap()->get_epoch(),
				info);
    i.info.history.last_epoch_started = e;
    m->pg_list.push_back(make_pair(i, pg_interval_map_t()));
    osd->send_message_osd_cluster(acting[0], m, get_osdmap()->get_epoch());
  }

  if (dirty_info) {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    write_if_dirty(*t);
    int tr = osd->store->queue_transaction(osr.get(), t);
    assert(tr == 0);
  }

  unlock();
}

/*
 * update info.history.last_epoch_started ONLY after we and all
 * replicas have activated AND committed the activate transaction
 * (i.e. the peering results are stable on disk).
 */
void PG::all_activated_and_committed()
{
  dout(10) << "all_activated_and_committed" << dendl;
  assert(is_primary());
  assert(peer_activated.size() == acting.size());

  // info.last_epoch_started is set during activate()
  info.history.last_epoch_started = info.last_epoch_started;

  share_pg_info();
  publish_stats_to_osd();

  queue_peering_event(
    CephPeeringEvtRef(
      new CephPeeringEvt(
        get_osdmap()->get_epoch(),
        get_osdmap()->get_epoch(),
        AllReplicasActivated())));
}

void PG::queue_snap_trim()
{
  if (osd->queue_for_snap_trim(this))
    dout(10) << "queue_snap_trim -- queuing" << dendl;
  else
    dout(10) << "queue_snap_trim -- already trimming" << dendl;
}

bool PG::queue_scrub()
{
  assert(_lock.is_locked());
  if (is_scrubbing()) {
    return false;
  }
  scrubber.must_scrub = false;
  state_set(PG_STATE_SCRUBBING);
  if (scrubber.must_deep_scrub) {
    state_set(PG_STATE_DEEP_SCRUB);
    scrubber.must_deep_scrub = false;
  }
  if (scrubber.must_repair) {
    state_set(PG_STATE_REPAIR);
    scrubber.must_repair = false;
  }
  osd->queue_for_scrub(this);
  return true;
}

struct C_PG_FinishRecovery : public Context {
  PGRef pg;
  C_PG_FinishRecovery(PG *p) : pg(p) {}
  void finish(int r) {
    pg->_finish_recovery(this);
  }
};

void PG::mark_clean()
{
  // only mark CLEAN if we have the desired number of replicas AND we
  // are not remapped.
  if (acting.size() == get_osdmap()->get_pg_size(info.pgid) &&
      up == acting)
    state_set(PG_STATE_CLEAN);

  // NOTE: this is actually a bit premature: we haven't purged the
  // strays yet.
  info.history.last_epoch_clean = get_osdmap()->get_epoch();

  trim_past_intervals();

  if (is_clean() && !snap_trimq.empty())
    queue_snap_trim();

  dirty_info = true;
}

void PG::finish_recovery(list<Context*>& tfin)
{
  dout(10) << "finish_recovery" << dendl;
  assert(info.last_complete == info.last_update);

  clear_recovery_state();

  /*
   * sync all this before purging strays.  but don't block!
   */
  finish_sync_event = new C_PG_FinishRecovery(this);
  tfin.push_back(finish_sync_event);
}

void PG::_finish_recovery(Context *c)
{
  lock();
  if (deleting) {
    unlock();
    return;
  }
  if (c == finish_sync_event) {
    dout(10) << "_finish_recovery" << dendl;
    finish_sync_event = 0;
    purge_strays();

    publish_stats_to_osd();

    if (scrub_after_recovery) {
      dout(10) << "_finish_recovery requeueing for scrub" << dendl;
      scrub_after_recovery = false;
      scrubber.must_deep_scrub = true;
      queue_scrub();
    }
  } else {
    dout(10) << "_finish_recovery -- stale" << dendl;
  }
  unlock();
}

void PG::start_recovery_op(const hobject_t& soid)
{
  dout(10) << "start_recovery_op " << soid
#ifdef DEBUG_RECOVERY_OIDS
	   << " (" << recovering_oids << ")"
#endif
	   << dendl;
  assert(recovery_ops_active >= 0);
  recovery_ops_active++;
#ifdef DEBUG_RECOVERY_OIDS
  assert(recovering_oids.count(soid) == 0);
  recovering_oids.insert(soid);
#endif
  // TODOSAM: osd->osd-> not good
  osd->osd->start_recovery_op(this, soid);
}

void PG::finish_recovery_op(const hobject_t& soid, bool dequeue)
{
  dout(10) << "finish_recovery_op " << soid
#ifdef DEBUG_RECOVERY_OIDS
	   << " (" << recovering_oids << ")" 
#endif
	   << dendl;
  assert(recovery_ops_active > 0);
  recovery_ops_active--;
#ifdef DEBUG_RECOVERY_OIDS
  assert(recovering_oids.count(soid));
  recovering_oids.erase(soid);
#endif
  // TODOSAM: osd->osd-> not good
  osd->osd->finish_recovery_op(this, soid, dequeue);
}

static void split_list(
  list<OpRequestRef> *from,
  list<OpRequestRef> *to,
  unsigned match,
  unsigned bits)
{
  for (list<OpRequestRef>::iterator i = from->begin();
       i != from->end();
    ) {
    if (PG::split_request(*i, match, bits)) {
      to->push_back(*i);
      from->erase(i++);
    } else {
      ++i;
    }
  }
}

static void split_replay_queue(
  map<eversion_t, OpRequestRef> *from,
  map<eversion_t, OpRequestRef> *to,
  unsigned match,
  unsigned bits)
{
  for (map<eversion_t, OpRequestRef>::iterator i = from->begin();
       i != from->end();
       ) {
    if (PG::split_request(i->second, match, bits)) {
      to->insert(*i);
      from->erase(i++);
    } else {
      ++i;
    }
  }
}

void PG::split_ops(PG *child, unsigned split_bits) {
  unsigned match = child->info.pgid.m_seed;
  assert(waiting_for_all_missing.empty());
  assert(waiting_for_missing_object.empty());
  assert(waiting_for_degraded_object.empty());
  assert(waiting_for_ack.empty());
  assert(waiting_for_ondisk.empty());
  split_replay_queue(&replay_queue, &(child->replay_queue), match, split_bits);

  osd->dequeue_pg(this, &waiting_for_active);
  split_list(&waiting_for_active, &(child->waiting_for_active), match, split_bits);
  {
    Mutex::Locker l(map_lock); // to avoid a race with the osd dispatch
    split_list(&waiting_for_map, &(child->waiting_for_map), match, split_bits);
  }
}

void PG::split_into(pg_t child_pgid, PG *child, unsigned split_bits)
{
  child->update_snap_mapper_bits(split_bits);
  child->update_osdmap_ref(get_osdmap());

  child->pool = pool;

  // Log
  pg_log.split_into(child_pgid, split_bits, &(child->pg_log));
  child->info.last_complete = info.last_complete;

  info.last_update = pg_log.get_head();
  child->info.last_update = child->pg_log.get_head();

  info.log_tail = pg_log.get_tail();
  child->info.log_tail = child->pg_log.get_tail();

  if (info.last_complete < pg_log.get_tail())
    info.last_complete = pg_log.get_tail();
  if (child->info.last_complete < child->pg_log.get_tail())
    child->info.last_complete = child->pg_log.get_tail();

  // Info
  child->info.history = info.history;
  child->info.purged_snaps = info.purged_snaps;
  child->info.last_backfill = info.last_backfill;

  child->info.stats = info.stats;
  info.stats.stats_invalid = true;
  child->info.stats.stats_invalid = true;
  child->info.last_epoch_started = info.last_epoch_started;

  child->snap_trimq = snap_trimq;

  get_osdmap()->pg_to_up_acting_osds(child->info.pgid, child->up, child->acting);
  child->role = get_osdmap()->calc_pg_role(osd->whoami, child->acting);
  if (get_primary() != child->get_primary())
    child->info.history.same_primary_since = get_osdmap()->get_epoch();

  // History
  child->past_intervals = past_intervals;

  split_ops(child, split_bits);
  _split_into(child_pgid, child, split_bits);

  child->dirty_info = true;
  child->dirty_big_info = true;
  dirty_info = true;
  dirty_big_info = true;
}

void PG::clear_recovery_state() 
{
  dout(10) << "clear_recovery_state" << dendl;

  pg_log.reset_recovery_pointers();
  finish_sync_event = 0;

  hobject_t soid;
  while (recovery_ops_active > 0) {
#ifdef DEBUG_RECOVERY_OIDS
    soid = *recovering_oids.begin();
#endif
    finish_recovery_op(soid, true);
  }

  backfill_target = -1;
  backfill_info.clear();
  peer_backfill_info.clear();
  waiting_on_backfill = false;
  _clear_recovery_state();  // pg impl specific hook
}

void PG::cancel_recovery()
{
  dout(10) << "cancel_recovery" << dendl;
  clear_recovery_state();
}


void PG::purge_strays()
{
  dout(10) << "purge_strays " << stray_set << dendl;
  
  bool removed = false;
  for (set<int>::iterator p = stray_set.begin();
       p != stray_set.end();
       ++p) {
    if (get_osdmap()->is_up(*p)) {
      dout(10) << "sending PGRemove to osd." << *p << dendl;
      vector<pg_t> to_remove;
      to_remove.push_back(info.pgid);
      MOSDPGRemove *m = new MOSDPGRemove(
	get_osdmap()->get_epoch(),
	to_remove);
      osd->send_message_osd_cluster(*p, m, get_osdmap()->get_epoch());
      stray_purged.insert(*p);
    } else {
      dout(10) << "not sending PGRemove to down osd." << *p << dendl;
    }
    peer_info.erase(*p);
    peer_purged.insert(*p);
    removed = true;
  }

  // if we removed anyone, update peers (which include peer_info)
  if (removed)
    update_heartbeat_peers();

  stray_set.clear();

  // clear _requested maps; we may have to peer() again if we discover
  // (more) stray content
  peer_log_requested.clear();
  peer_missing_requested.clear();
}

void PG::set_probe_targets(const set<int> &probe_set)
{
  Mutex::Locker l(heartbeat_peer_lock);
  probe_targets = probe_set;
}

void PG::clear_probe_targets()
{
  Mutex::Locker l(heartbeat_peer_lock);
  probe_targets.clear();
}

void PG::update_heartbeat_peers()
{
  assert(is_locked());

  set<int> new_peers;
  if (role == 0) {
    for (unsigned i=0; i<acting.size(); i++)
      new_peers.insert(acting[i]);
    for (unsigned i=0; i<up.size(); i++)
      new_peers.insert(up[i]);
    for (map<int,pg_info_t>::iterator p = peer_info.begin(); p != peer_info.end(); ++p)
      new_peers.insert(p->first);
  }

  bool need_update = false;
  heartbeat_peer_lock.Lock();
  if (new_peers == heartbeat_peers) {
    dout(10) << "update_heartbeat_peers " << heartbeat_peers << " unchanged" << dendl;
  } else {
    dout(10) << "update_heartbeat_peers " << heartbeat_peers << " -> " << new_peers << dendl;
    heartbeat_peers.swap(new_peers);
    need_update = true;
  }
  heartbeat_peer_lock.Unlock();

  if (need_update)
    osd->need_heartbeat_peer_update();
}

void PG::publish_stats_to_osd()
{
  pg_stats_publish_lock.Lock();
  if (is_primary()) {
    // update our stat summary
    info.stats.reported.inc(info.history.same_primary_since);
    info.stats.version = info.last_update;
    info.stats.created = info.history.epoch_created;
    info.stats.last_scrub = info.history.last_scrub;
    info.stats.last_scrub_stamp = info.history.last_scrub_stamp;
    info.stats.last_deep_scrub = info.history.last_deep_scrub;
    info.stats.last_deep_scrub_stamp = info.history.last_deep_scrub_stamp;
    info.stats.last_clean_scrub_stamp = info.history.last_clean_scrub_stamp;
    info.stats.last_epoch_clean = info.history.last_epoch_clean;

    if (info.stats.stats.sum.num_scrub_errors)
      state_set(PG_STATE_INCONSISTENT);
    else
      state_clear(PG_STATE_INCONSISTENT);

    utime_t now = ceph_clock_now(g_ceph_context);
    info.stats.last_fresh = now;
    if (info.stats.state != state) {
      info.stats.state = state;
      info.stats.last_change = now;
      if ((state & PG_STATE_ACTIVE) &&
	  !(info.stats.state & PG_STATE_ACTIVE))
	info.stats.last_became_active = now;
    }
    if (info.stats.state & PG_STATE_CLEAN)
      info.stats.last_clean = now;
    if (info.stats.state & PG_STATE_ACTIVE)
      info.stats.last_active = now;
    info.stats.last_unstale = now;

    info.stats.log_size = pg_log.get_head().version - pg_log.get_tail().version;
    info.stats.ondisk_log_size =
      pg_log.get_head().version - pg_log.get_tail().version;
    info.stats.log_start = pg_log.get_tail();
    info.stats.ondisk_log_start = pg_log.get_tail();

    pg_stats_publish_valid = true;
    pg_stats_publish = info.stats;
    pg_stats_publish.stats.add(unstable_stats);

    // calc copies, degraded
    unsigned target = MAX(get_osdmap()->get_pg_size(info.pgid), acting.size());
    pg_stats_publish.stats.calc_copies(target);
    pg_stats_publish.stats.sum.num_objects_degraded = 0;
    if ((is_degraded() || !is_clean()) && is_active()) {
      // NOTE: we only generate copies, degraded, unfound values for
      // the summation, not individual stat categories.
      uint64_t num_objects = pg_stats_publish.stats.sum.num_objects;

      uint64_t degraded = 0;

      // if the acting set is smaller than we want, add in those missing replicas
      if (acting.size() < target)
	degraded += (target - acting.size()) * num_objects;

      // missing on primary
      pg_stats_publish.stats.sum.num_objects_missing_on_primary =
	pg_log.get_missing().num_missing();
      degraded += pg_log.get_missing().num_missing();
      
      for (unsigned i=1; i<acting.size(); i++) {
	assert(peer_missing.count(acting[i]));

	// in missing set
	degraded += peer_missing[acting[i]].num_missing();

	// not yet backfilled
	degraded += num_objects - peer_info[acting[i]].stats.stats.sum.num_objects;
      }
      pg_stats_publish.stats.sum.num_objects_degraded = degraded;
      pg_stats_publish.stats.sum.num_objects_unfound = get_num_unfound();
    }

    dout(15) << "publish_stats_to_osd " << pg_stats_publish.reported << dendl;
  } else {
    pg_stats_publish_valid = false;
    dout(15) << "publish_stats_to_osd -- not primary" << dendl;
  }
  pg_stats_publish_lock.Unlock();

  if (is_primary())
    osd->pg_stat_queue_enqueue(this);
}

void PG::clear_publish_stats()
{
  dout(15) << "clear_stats" << dendl;
  pg_stats_publish_lock.Lock();
  pg_stats_publish_valid = false;
  pg_stats_publish_lock.Unlock();

  osd->pg_stat_queue_dequeue(this);
}

/**
 * initialize a newly instantiated pg
 *
 * Initialize PG state, as when a PG is initially created, or when it
 * is first instantiated on the current node.
 *
 * @param role our role/rank
 * @param newup up set
 * @param newacting acting set
 * @param history pg history
 * @param pi past_intervals
 * @param backfill true if info should be marked as backfill
 * @param t transaction to write out our new state in
 */
void PG::init(int role, vector<int>& newup, vector<int>& newacting,
	      pg_history_t& history,
	      pg_interval_map_t& pi,
	      bool backfill,
	      ObjectStore::Transaction *t)
{
  dout(10) << "init role " << role << " up " << newup << " acting " << newacting
	   << " history " << history
	   << " " << pi.size() << " past_intervals"
	   << dendl;

  set_role(role);
  acting = newacting;
  up = newup;

  info.history = history;
  past_intervals.swap(pi);

  info.stats.up = up;
  info.stats.acting = acting;
  info.stats.mapping_epoch = info.history.same_interval_since;

  if (backfill) {
    dout(10) << __func__ << ": Setting backfill" << dendl;
    info.last_backfill = hobject_t();
    info.last_complete = info.last_update;
  }

  reg_next_scrub();

  dirty_info = true;
  dirty_big_info = true;
  write_if_dirty(*t);
}

void PG::upgrade(ObjectStore *store, const interval_set<snapid_t> &snapcolls)
{
  unsigned removed = 0;
  for (interval_set<snapid_t>::const_iterator i = snapcolls.begin();
       i != snapcolls.end();
       ++i) {
    for (snapid_t next_dir = i.get_start();
	 next_dir != i.get_start() + i.get_len();
	 ++next_dir) {
      ++removed;
      coll_t cid(info.pgid, next_dir);
      dout(1) << "Removing collection " << cid
	      << " (" << removed << "/" << snapcolls.size()
	      << ")" << dendl;

      hobject_t cur;
      vector<hobject_t> objects;
      while (1) {
	int r = store->collection_list_partial(
	  cid,
	  cur,
	  store->get_ideal_list_min(),
	  store->get_ideal_list_max(),
	  0,
	  &objects,
	  &cur);
	if (r != 0) {
	  derr << __func__ << ": collection_list_partial returned "
	       << cpp_strerror(r) << dendl;
	  assert(0);
	}
	if (objects.empty()) {
	  assert(cur.is_max());
	  break;
	}
	ObjectStore::Transaction t;
	for (vector<hobject_t>::iterator j = objects.begin();
	     j != objects.end();
	     ++j) {
	  t.remove(cid, *j);
	}
	r = store->apply_transaction(t);
	if (r != 0) {
	  derr << __func__ << ": apply_transaction returned "
	       << cpp_strerror(r) << dendl;
	  assert(0);
	}
	objects.clear();
      }
      ObjectStore::Transaction t;
      t.remove_collection(cid);
      int r = store->apply_transaction(t);
      if (r != 0) {
	derr << __func__ << ": apply_transaction returned "
	     << cpp_strerror(r) << dendl;
	assert(0);
      }
    }
  }

  hobject_t cur;
  coll_t cid(info.pgid);
  unsigned done = 0;
  vector<hobject_t> objects;
  while (1) {
    dout(1) << "Updating snap_mapper from main collection, "
	    << done << " objects done" << dendl;
    int r = store->collection_list_partial(
      cid,
      cur,
      store->get_ideal_list_min(),
      store->get_ideal_list_max(),
      0,
      &objects,
      &cur);
    if (r != 0) {
      derr << __func__ << ": collection_list_partial returned "
	   << cpp_strerror(r) << dendl;
      assert(0);
    }
    if (objects.empty()) {
      assert(cur.is_max());
      break;
    }
    done += objects.size();
    ObjectStore::Transaction t;
    for (vector<hobject_t>::iterator j = objects.begin();
	 j != objects.end();
	 ++j) {
      if (j->snap < CEPH_MAXSNAP) {
	OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
	bufferptr bp;
	r = store->getattr(
	  cid,
	  *j,
	  OI_ATTR,
	  bp);
	if (r < 0) {
	  derr << __func__ << ": getattr returned "
	       << cpp_strerror(r) << dendl;
	  assert(0);
	}
	bufferlist bl;
	bl.push_back(bp);
	object_info_t oi(bl);
	set<snapid_t> oi_snaps(oi.snaps.begin(), oi.snaps.end());
	set<snapid_t> cur_snaps;
	r = snap_mapper.get_snaps(*j, &cur_snaps);
	if (r == 0) {
	  assert(cur_snaps == oi_snaps);
	} else if (r == -ENOENT) {
	  snap_mapper.add_oid(*j, oi_snaps, &_t);
	} else {
	  derr << __func__ << ": get_snaps returned "
	       << cpp_strerror(r) << dendl;
	  assert(0);
	}
      }
    }
    r = store->apply_transaction(t);
    if (r != 0) {
      derr << __func__ << ": apply_transaction returned "
	   << cpp_strerror(r) << dendl;
      assert(0);
    }
    objects.clear();
  }
  ObjectStore::Transaction t;
  snap_collections.clear();
  dirty_info = true;
  write_if_dirty(t);
  int r = store->apply_transaction(t);
  if (r != 0) {
    derr << __func__ << ": apply_transaction returned "
	 << cpp_strerror(r) << dendl;
    assert(0);
  }
  assert(r == 0);
}

int PG::_write_info(ObjectStore::Transaction& t, epoch_t epoch,
    pg_info_t &info, coll_t coll,
    map<epoch_t,pg_interval_t> &past_intervals,
    interval_set<snapid_t> &snap_collections,
    hobject_t &infos_oid,
    __u8 info_struct_v, bool dirty_big_info, bool force_ver)
{
  // pg state

  if (info_struct_v > cur_struct_v)
    return -EINVAL;

  // Only need to write struct_v to attr when upgrading
  if (force_ver || info_struct_v < cur_struct_v) {
    bufferlist attrbl;
    info_struct_v = cur_struct_v;
    ::encode(info_struct_v, attrbl);
    t.collection_setattr(coll, "info", attrbl);
    dirty_big_info = true;
  }

  // info.  store purged_snaps separately.
  interval_set<snapid_t> purged_snaps;
  map<string,bufferlist> v;
  ::encode(epoch, v[get_epoch_key(info.pgid)]);
  purged_snaps.swap(info.purged_snaps);
  ::encode(info, v[get_info_key(info.pgid)]);
  purged_snaps.swap(info.purged_snaps);

  if (dirty_big_info) {
    // potentially big stuff
    bufferlist& bigbl = v[get_biginfo_key(info.pgid)];
    ::encode(past_intervals, bigbl);
    ::encode(snap_collections, bigbl);
    ::encode(info.purged_snaps, bigbl);
    //dout(20) << "write_info bigbl " << bigbl.length() << dendl;
  }

  t.omap_setkeys(coll_t::META_COLL, infos_oid, v);

  return 0;
}

void PG::write_info(ObjectStore::Transaction& t)
{
  info.stats.stats.add(unstable_stats);
  unstable_stats.clear();

  int ret = _write_info(t, get_osdmap()->get_epoch(), info, coll,
     past_intervals, snap_collections, osd->infos_oid,
     info_struct_v, dirty_big_info);
  assert(ret == 0);
  last_persisted_osdmap_ref = osdmap_ref;

  dirty_info = false;
  dirty_big_info = false;
}

epoch_t PG::peek_map_epoch(ObjectStore *store, coll_t coll, hobject_t &infos_oid, bufferlist *bl)
{
  assert(bl);
  pg_t pgid;
  snapid_t snap;
  bool ok = coll.is_pg(pgid, snap);
  assert(ok);
  store->collection_getattr(coll, "info", *bl);
  bufferlist::iterator bp = bl->begin();
  __u8 struct_v = 0;
  ::decode(struct_v, bp);
  if (struct_v < 5)
    return 0;
  epoch_t cur_epoch = 0;
  if (struct_v < 6) {
    ::decode(cur_epoch, bp);
  } else {
    // get epoch out of leveldb
    bufferlist tmpbl;
    string ek = get_epoch_key(pgid);
    set<string> keys;
    keys.insert(get_epoch_key(pgid));
    map<string,bufferlist> values;
    store->omap_get_values(coll_t::META_COLL, infos_oid, keys, &values);
    assert(values.size() == 1);
    tmpbl = values[ek];
    bufferlist::iterator p = tmpbl.begin();
    ::decode(cur_epoch, p);
  }
  return cur_epoch;
}

void PG::write_if_dirty(ObjectStore::Transaction& t)
{
  if (dirty_big_info || dirty_info)
    write_info(t);
  pg_log.write_log(t, log_oid);
}

void PG::trim_peers()
{
  calc_trim_to();
  dout(10) << "trim_peers " << pg_trim_to << dendl;
  if (pg_trim_to != eversion_t()) {
    for (unsigned i=1; i<acting.size(); i++)
      osd->send_message_osd_cluster(acting[i],
				    new MOSDPGTrim(get_osdmap()->get_epoch(), info.pgid,
						   pg_trim_to),
				    get_osdmap()->get_epoch());
  }
}

void PG::add_log_entry(pg_log_entry_t& e, bufferlist& log_bl)
{
  // raise last_complete only if we were previously up to date
  if (info.last_complete == info.last_update)
    info.last_complete = e.version;
  
  // raise last_update.
  assert(e.version > info.last_update);
  info.last_update = e.version;

  // log mutation
  pg_log.add(e);
  dout(10) << "add_log_entry " << e << dendl;

  e.encode_with_checksum(log_bl);
}


void PG::append_log(
  vector<pg_log_entry_t>& logv, eversion_t trim_to, ObjectStore::Transaction &t)
{
  dout(10) << "append_log " << pg_log.get_log() << " " << logv << dendl;

  map<string,bufferlist> keys;
  for (vector<pg_log_entry_t>::iterator p = logv.begin();
       p != logv.end();
       ++p) {
    p->offset = 0;
    add_log_entry(*p, keys[p->get_key_name()]);
  }

  dout(10) << "append_log  adding " << keys.size() << " keys" << dendl;
  t.omap_setkeys(coll_t::META_COLL, log_oid, keys);

  pg_log.trim(trim_to, info);

  // update the local pg, pg log
  dirty_info = true;
  write_if_dirty(t);
}

bool PG::check_log_for_corruption(ObjectStore *store)
{
  /// TODO: this method needs to work with the omap log
  return true;
}

//! Get the name we're going to save our corrupt page log as
std::string PG::get_corrupt_pg_log_name() const
{
  const int MAX_BUF = 512;
  char buf[MAX_BUF];
  struct tm tm_buf;
  time_t my_time(time(NULL));
  const struct tm *t = localtime_r(&my_time, &tm_buf);
  int ret = strftime(buf, sizeof(buf), "corrupt_log_%Y-%m-%d_%k:%M_", t);
  if (ret == 0) {
    dout(0) << "strftime failed" << dendl;
    return "corrupt_log_unknown_time";
  }
  info.pgid.print(buf + ret, MAX_BUF - ret);
  return buf;
}

int PG::read_info(
  ObjectStore *store, const coll_t coll, bufferlist &bl,
  pg_info_t &info, map<epoch_t,pg_interval_t> &past_intervals,
  hobject_t &biginfo_oid, hobject_t &infos_oid,
  interval_set<snapid_t>  &snap_collections, __u8 &struct_v)
{
  bufferlist::iterator p = bl.begin();
  bufferlist lbl;

  // info
  ::decode(struct_v, p);
  if (struct_v < 4)
    ::decode(info, p);
  if (struct_v < 2) {
    ::decode(past_intervals, p);
  
    // snap_collections
    store->collection_getattr(coll, "snap_collections", lbl);
    p = lbl.begin();
    ::decode(struct_v, p);
  } else {
    if (struct_v < 6) {
      int r = store->read(coll_t::META_COLL, biginfo_oid, 0, 0, lbl);
      if (r < 0)
        return r;
      p = lbl.begin();
      ::decode(past_intervals, p);
    } else {
      // get info out of leveldb
      string k = get_info_key(info.pgid);
      string bk = get_biginfo_key(info.pgid);
      set<string> keys;
      keys.insert(k);
      keys.insert(bk);
      map<string,bufferlist> values;
      store->omap_get_values(coll_t::META_COLL, infos_oid, keys, &values);
      assert(values.size() == 2);
      lbl = values[k];
      p = lbl.begin();
      ::decode(info, p);

      lbl = values[bk];
      p = lbl.begin();
      ::decode(past_intervals, p);
    }
  }

  if (struct_v < 3) {
    set<snapid_t> snap_collections_temp;
    ::decode(snap_collections_temp, p);
    snap_collections.clear();
    for (set<snapid_t>::iterator i = snap_collections_temp.begin();
	 i != snap_collections_temp.end();
	 ++i) {
      snap_collections.insert(*i);
    }
  } else {
    ::decode(snap_collections, p);
    if (struct_v >= 4 && struct_v < 6)
      ::decode(info, p);
    else if (struct_v >= 6)
      ::decode(info.purged_snaps, p);
  }
  return 0;
}

void PG::read_state(ObjectStore *store, bufferlist &bl)
{
  int r = read_info(store, coll, bl, info, past_intervals, biginfo_oid,
    osd->infos_oid, snap_collections, info_struct_v);
  assert(r >= 0);

  ostringstream oss;
  if (pg_log.read_log(
      store, coll, log_oid, info,
      oss)) {
    /* We don't want to leave the old format around in case the next log
     * write happens to be an append_log()
     */
    ObjectStore::Transaction t;
    pg_log.write_log(t, log_oid);
    int r = osd->store->apply_transaction(t);
    assert(!r);
  }
  if (oss.str().length())
    osd->clog.error() << oss;

  // log any weirdness
  log_weirdness();
}

void PG::log_weirdness()
{
  if (pg_log.get_tail() != info.log_tail)
    osd->clog.error() << info.pgid
		      << " info mismatch, log.tail " << pg_log.get_tail()
		      << " != info.log_tail " << info.log_tail
		      << "\n";
  if (pg_log.get_head() != info.last_update)
    osd->clog.error() << info.pgid
		      << " info mismatch, log.head " << pg_log.get_head()
		      << " != info.last_update " << info.last_update
		      << "\n";

  if (pg_log.get_log().empty()) {
    // shoudl it be?
    if (pg_log.get_head() != pg_log.get_tail())
      osd->clog.error() << info.pgid
			<< " log bound mismatch, empty but (" << pg_log.get_tail() << ","
			<< pg_log.get_head() << "]\n";
  } else {
    if ((pg_log.get_log().log.begin()->version <= pg_log.get_tail()) || // sloppy check
	(pg_log.get_log().log.rbegin()->version != pg_log.get_head() &&
	 !(pg_log.get_head() == pg_log.get_tail())))
      osd->clog.error() << info.pgid
			<< " log bound mismatch, info (" << pg_log.get_tail() << ","
			<< pg_log.get_head() << "]"
			<< " actual ["
			<< pg_log.get_log().log.begin()->version << ","
			<< pg_log.get_log().log.rbegin()->version << "]"
			<< "\n";
  }
  
  if (pg_log.get_log().caller_ops.size() > pg_log.get_log().log.size()) {
    osd->clog.error() << info.pgid
		      << " caller_ops.size " << pg_log.get_log().caller_ops.size()
		      << " > log size " << pg_log.get_log().log.size()
		      << "\n";
  }
}

void PG::update_snap_map(
  vector<pg_log_entry_t> &log_entries,
  ObjectStore::Transaction &t)
{
  for (vector<pg_log_entry_t>::iterator i = log_entries.begin();
       i != log_entries.end();
       ++i) {
    OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
    if (i->soid.snap < CEPH_MAXSNAP) {
      if (i->is_delete()) {
	int r = snap_mapper.remove_oid(
	  i->soid,
	  &_t);
	assert(r == 0);
      } else {
	assert(i->snaps.length() > 0);
	vector<snapid_t> snaps;
	bufferlist::iterator p = i->snaps.begin();
	try {
	  ::decode(snaps, p);
	} catch (...) {
	  snaps.clear();
	}
	set<snapid_t> _snaps(snaps.begin(), snaps.end());

	if (i->is_clone()) {
	  snap_mapper.add_oid(
	    i->soid,
	    _snaps,
	    &_t);
	} else {
	  assert(i->is_modify());
	  int r = snap_mapper.update_snaps(
	    i->soid,
	    _snaps,
	    0,
	    &_t);
	  assert(r == 0);
	}
      }
    }
  }
}

/**
 * filter trimming|trimmed snaps out of snapcontext
 */
void PG::filter_snapc(SnapContext& snapc)
{
  bool filtering = false;
  vector<snapid_t> newsnaps;
  for (vector<snapid_t>::iterator p = snapc.snaps.begin();
       p != snapc.snaps.end();
       ++p) {
    if (snap_trimq.contains(*p) || info.purged_snaps.contains(*p)) {
      if (!filtering) {
	// start building a new vector with what we've seen so far
	dout(10) << "filter_snapc filtering " << snapc << dendl;
	newsnaps.insert(newsnaps.begin(), snapc.snaps.begin(), p);
	filtering = true;
      }
      dout(20) << "filter_snapc  removing trimq|purged snap " << *p << dendl;
    } else {
      if (filtering)
	newsnaps.push_back(*p);  // continue building new vector
    }
  }
  if (filtering) {
    snapc.snaps.swap(newsnaps);
    dout(10) << "filter_snapc  result " << snapc << dendl;
  }
}

void PG::requeue_object_waiters(map<hobject_t, list<OpRequestRef> >& m)
{
  for (map<hobject_t, list<OpRequestRef> >::iterator it = m.begin();
       it != m.end();
       ++it)
    requeue_ops(it->second);
  m.clear();
}

void PG::requeue_ops(list<OpRequestRef> &ls)
{
  dout(15) << " requeue_ops " << ls << dendl;
  for (list<OpRequestRef>::reverse_iterator i = ls.rbegin();
       i != ls.rend();
       ++i) {
    osd->op_wq.queue_front(make_pair(PGRef(this), *i));
  }
  ls.clear();
}


// ==========================================================================================
// SCRUB

/*
 * when holding pg and sched_scrub_lock, then the states are:
 *   scheduling:
 *     scrubber.reserved = true
 *     scrub_rserved_peers includes whoami
 *     osd->scrub_pending++
 *   scheduling, replica declined:
 *     scrubber.reserved = true
 *     scrubber.reserved_peers includes -1
 *     osd->scrub_pending++
 *   pending:
 *     scrubber.reserved = true
 *     scrubber.reserved_peers.size() == acting.size();
 *     pg on scrub_wq
 *     osd->scrub_pending++
 *   scrubbing:
 *     scrubber.reserved = false;
 *     scrubber.reserved_peers empty
 *     osd->scrubber.active++
 */

// returns true if a scrub has been newly kicked off
bool PG::sched_scrub()
{
  assert(_lock.is_locked());
  if (!(is_primary() && is_active() && is_clean() && !is_scrubbing())) {
    return false;
  }

  bool time_for_deep = (ceph_clock_now(g_ceph_context) >
    info.history.last_deep_scrub_stamp + g_conf->osd_deep_scrub_interval);
 
  //NODEEP_SCRUB so ignore time initiated deep-scrub
  if (osd->osd->get_osdmap()->test_flag(CEPH_OSDMAP_NODEEP_SCRUB))
    time_for_deep = false;

  if (!scrubber.must_scrub) {
    assert(!scrubber.must_deep_scrub);

    //NOSCRUB so skip regular scrubs
    if (osd->osd->get_osdmap()->test_flag(CEPH_OSDMAP_NOSCRUB) && !time_for_deep)
      return false;
  }

  bool ret = true;
  if (!scrubber.reserved) {
    assert(scrubber.reserved_peers.empty());
    if (osd->inc_scrubs_pending()) {
      dout(20) << "sched_scrub: reserved locally, reserving replicas" << dendl;
      scrubber.reserved = true;
      scrubber.reserved_peers.insert(osd->whoami);
      scrub_reserve_replicas();
    } else {
      dout(20) << "sched_scrub: failed to reserve locally" << dendl;
      ret = false;
    }
  }
  if (scrubber.reserved) {
    if (scrubber.reserve_failed) {
      dout(20) << "sched_scrub: failed, a peer declined" << dendl;
      clear_scrub_reserved();
      scrub_unreserve_replicas();
      ret = false;
    } else if (scrubber.reserved_peers.size() == acting.size()) {
      dout(20) << "sched_scrub: success, reserved self and replicas" << dendl;
      if (time_for_deep) {
	dout(10) << "sched_scrub: scrub will be deep" << dendl;
	state_set(PG_STATE_DEEP_SCRUB);
      }
      queue_scrub();
    } else {
      // none declined, since scrubber.reserved is set
      dout(20) << "sched_scrub: reserved " << scrubber.reserved_peers << ", waiting for replicas" << dendl;
    }
  }

  return ret;
}

void PG::reg_next_scrub()
{
  if (scrubber.must_scrub) {
    scrubber.scrub_reg_stamp = utime_t();
  } else {
    scrubber.scrub_reg_stamp = info.history.last_scrub_stamp;
  }
  osd->reg_last_pg_scrub(info.pgid, scrubber.scrub_reg_stamp);
}

void PG::unreg_next_scrub()
{
  osd->unreg_last_pg_scrub(info.pgid, scrubber.scrub_reg_stamp);
}

void PG::sub_op_scrub_map(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp *>(op->request);
  assert(m->get_header().type == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_map" << dendl;

  if (m->map_epoch < info.history.same_interval_since) {
    dout(10) << "sub_op_scrub discarding old sub_op from "
	     << m->map_epoch << " < " << info.history.same_interval_since << dendl;
    return;
  }

  op->mark_started();

  int from = m->get_source().num();

  dout(10) << " got osd." << from << " scrub map" << dendl;
  bufferlist::iterator p = m->get_data().begin();

  if (scrubber.is_chunky) { // chunky scrub
    scrubber.received_maps[from].decode(p, info.pgid.pool());
    dout(10) << "map version is " << scrubber.received_maps[from].valid_through << dendl;
  } else {               // classic scrub
    if (scrubber.received_maps.count(from)) {
      ScrubMap incoming;
      incoming.decode(p, info.pgid.pool());
      dout(10) << "from replica " << from << dendl;
      dout(10) << "map version is " << incoming.valid_through << dendl;
      scrubber.received_maps[from].merge_incr(incoming);
    } else {
      scrubber.received_maps[from].decode(p, info.pgid.pool());
    }
  }

  --scrubber.waiting_on;
  scrubber.waiting_on_whom.erase(from);

  if (scrubber.waiting_on == 0) {
    if (scrubber.is_chunky) { // chunky scrub
      osd->scrub_wq.queue(this);
    } else {                  // classic scrub
      if (scrubber.finalizing) { // incremental lists received
        osd->scrub_finalize_wq.queue(this);
      } else {                // initial lists received
        scrubber.block_writes = true;
        if (last_update_applied == info.last_update) {
          scrubber.finalizing = true;
          scrub_gather_replica_maps();
          ++scrubber.waiting_on;
          scrubber.waiting_on_whom.insert(osd->whoami);
          osd->scrub_wq.queue(this);
        }
      }
    }
  }
}

/* 
 * pg lock may or may not be held
 */
void PG::_scan_list(
  ScrubMap &map, vector<hobject_t> &ls, bool deep,
  ThreadPool::TPHandle &handle)
{
  dout(10) << "_scan_list scanning " << ls.size() << " objects"
           << (deep ? " deeply" : "") << dendl;
  int i = 0;
  for (vector<hobject_t>::iterator p = ls.begin(); 
       p != ls.end(); 
       ++p, i++) {
    handle.reset_tp_timeout();
    hobject_t poid = *p;

    struct stat st;
    int r = osd->store->stat(coll, poid, &st, true);
    if (r == 0) {
      ScrubMap::object &o = map.objects[poid];
      o.size = st.st_size;
      assert(!o.negative);
      osd->store->getattrs(coll, poid, o.attrs);

      // calculate the CRC32 on deep scrubs
      if (deep) {
        bufferhash h, oh;
        bufferlist bl, hdrbl;
        int r;
        __u64 pos = 0;
        while ( (r = osd->store->read(coll, poid, pos,
                                      g_conf->osd_deep_scrub_stride, bl,
		                      true)) > 0) {
	  handle.reset_tp_timeout();
          h << bl;
          pos += bl.length();
          bl.clear();
        }
	if (r == -EIO) {
	  dout(25) << "_scan_list  " << poid << " got "
		   << r << " on read, read_error" << dendl;
	  o.read_error = true;
	}
        o.digest = h.digest();
        o.digest_present = true;

        bl.clear();
        r = osd->store->omap_get_header(coll, poid, &hdrbl, true);
        if (r == 0) {
          dout(25) << "CRC header " << string(hdrbl.c_str(), hdrbl.length())
             << dendl;
          ::encode(hdrbl, bl);
          oh << bl;
          bl.clear();
        } else if (r == -EIO) {
	  dout(25) << "_scan_list  " << poid << " got "
		   << r << " on omap header read, read_error" << dendl;
	  o.read_error = true;
	}

        ObjectMap::ObjectMapIterator iter = osd->store->get_omap_iterator(
          coll, poid);
        assert(iter);
	uint64_t keys_scanned = 0;
        for (iter->seek_to_first(); iter->valid() ; iter->next()) {
	  if (g_conf->osd_scan_list_ping_tp_interval &&
	      (keys_scanned % g_conf->osd_scan_list_ping_tp_interval == 0)) {
	    handle.reset_tp_timeout();
	  }
	  ++keys_scanned;

          dout(25) << "CRC key " << iter->key() << " value "
            << string(iter->value().c_str(), iter->value().length()) << dendl;

          ::encode(iter->key(), bl);
          ::encode(iter->value(), bl);
          oh << bl;
          bl.clear();
        }
	if (iter->status() == -EIO) {
	  dout(25) << "_scan_list  " << poid << " got "
		   << r << " on omap scan, read_error" << dendl;
	  o.read_error = true;
	  break;
	}

        //Store final calculated CRC32 of omap header & key/values
        o.omap_digest = oh.digest();
        o.omap_digest_present = true;
      }

      dout(25) << "_scan_list  " << poid << dendl;
    } else if (r == -ENOENT) {
      dout(25) << "_scan_list  " << poid << " got " << r << ", skipping" << dendl;
    } else if (r == -EIO) {
      dout(25) << "_scan_list  " << poid << " got " << r << ", read_error" << dendl;
      ScrubMap::object &o = map.objects[poid];
      o.read_error = true;
    } else {
      derr << "_scan_list got: " << cpp_strerror(r) << dendl;
      assert(0);
    }
  }
}

// send scrub v2-compatible messages (classic scrub)
void PG::_request_scrub_map_classic(int replica, eversion_t version)
{
  assert(replica != osd->whoami);
  dout(10) << "scrub  requesting scrubmap from osd." << replica << dendl;
  MOSDRepScrub *repscrubop = new MOSDRepScrub(info.pgid, version,
					      last_update_applied,
                                              get_osdmap()->get_epoch());
  osd->send_message_osd_cluster(replica, repscrubop, get_osdmap()->get_epoch());
}

// send scrub v3 messages (chunky scrub)
void PG::_request_scrub_map(int replica, eversion_t version,
                            hobject_t start, hobject_t end,
                            bool deep)
{
  assert(replica != osd->whoami);
  dout(10) << "scrub  requesting scrubmap from osd." << replica << dendl;
  MOSDRepScrub *repscrubop = new MOSDRepScrub(info.pgid, version,
                                              get_osdmap()->get_epoch(),
                                              start, end, deep);
  osd->send_message_osd_cluster(replica, repscrubop, get_osdmap()->get_epoch());
}

void PG::sub_op_scrub_reserve(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->request);
  assert(m->get_header().type == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_reserve" << dendl;

  if (scrubber.reserved) {
    dout(10) << "Ignoring reserve request: Already reserved" << dendl;
    return;
  }

  op->mark_started();

  scrubber.reserved = osd->inc_scrubs_pending();

  MOSDSubOpReply *reply = new MOSDSubOpReply(m, 0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
  ::encode(scrubber.reserved, reply->get_data());
  osd->send_message_osd_cluster(reply, m->get_connection());
}

void PG::sub_op_scrub_reserve_reply(OpRequestRef op)
{
  MOSDSubOpReply *reply = static_cast<MOSDSubOpReply*>(op->request);
  assert(reply->get_header().type == MSG_OSD_SUBOPREPLY);
  dout(7) << "sub_op_scrub_reserve_reply" << dendl;

  if (!scrubber.reserved) {
    dout(10) << "ignoring obsolete scrub reserve reply" << dendl;
    return;
  }

  op->mark_started();

  int from = reply->get_source().num();
  bufferlist::iterator p = reply->get_data().begin();
  bool reserved;
  ::decode(reserved, p);

  if (scrubber.reserved_peers.find(from) != scrubber.reserved_peers.end()) {
    dout(10) << " already had osd." << from << " reserved" << dendl;
  } else {
    if (reserved) {
      dout(10) << " osd." << from << " scrub reserve = success" << dendl;
      scrubber.reserved_peers.insert(from);
    } else {
      /* One decline stops this pg from being scheduled for scrubbing. */
      dout(10) << " osd." << from << " scrub reserve = fail" << dendl;
      scrubber.reserve_failed = true;
    }
    sched_scrub();
  }
}

void PG::sub_op_scrub_unreserve(OpRequestRef op)
{
  assert(op->request->get_header().type == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_unreserve" << dendl;

  op->mark_started();

  clear_scrub_reserved();
}

void PG::sub_op_scrub_stop(OpRequestRef op)
{
  op->mark_started();

  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->request);
  assert(m->get_header().type == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_stop" << dendl;

  // see comment in sub_op_scrub_reserve
  scrubber.reserved = false;

  MOSDSubOpReply *reply = new MOSDSubOpReply(m, 0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
  osd->send_message_osd_cluster(reply, m->get_connection());
}

void PG::reject_reservation()
{
  osd->send_message_osd_cluster(
    acting[0],
    new MBackfillReserve(
      MBackfillReserve::REJECT,
      info.pgid,
      get_osdmap()->get_epoch()),
    get_osdmap()->get_epoch());
}

void PG::schedule_backfill_full_retry()
{
  Mutex::Locker lock(osd->backfill_request_lock);
  osd->backfill_request_timer.add_event_after(
    g_conf->osd_backfill_retry_interval,
    new QueuePeeringEvt<RequestBackfill>(
      this, get_osdmap()->get_epoch(),
      RequestBackfill()));
}

void PG::clear_scrub_reserved()
{
  osd->scrub_wq.dequeue(this);
  scrubber.reserved_peers.clear();
  scrubber.reserve_failed = false;

  if (scrubber.reserved) {
    scrubber.reserved = false;
    osd->dec_scrubs_pending();
  }
}

void PG::scrub_reserve_replicas()
{
  for (unsigned i=1; i<acting.size(); i++) {
    dout(10) << "scrub requesting reserve from osd." << acting[i] << dendl;
    vector<OSDOp> scrub(1);
    scrub[0].op.op = CEPH_OSD_OP_SCRUB_RESERVE;
    hobject_t poid;
    eversion_t v;
    osd_reqid_t reqid;
    MOSDSubOp *subop = new MOSDSubOp(reqid, info.pgid, poid, false, 0,
                                     get_osdmap()->get_epoch(), osd->get_tid(), v);
    subop->ops = scrub;
    osd->send_message_osd_cluster(acting[i], subop, get_osdmap()->get_epoch());
  }
}

void PG::scrub_unreserve_replicas()
{
  for (unsigned i=1; i<acting.size(); i++) {
    dout(10) << "scrub requesting unreserve from osd." << acting[i] << dendl;
    vector<OSDOp> scrub(1);
    scrub[0].op.op = CEPH_OSD_OP_SCRUB_UNRESERVE;
    hobject_t poid;
    eversion_t v;
    osd_reqid_t reqid;
    MOSDSubOp *subop = new MOSDSubOp(reqid, info.pgid, poid, false, 0,
                                     get_osdmap()->get_epoch(), osd->get_tid(), v);
    subop->ops = scrub;
    osd->send_message_osd_cluster(acting[i], subop, get_osdmap()->get_epoch());
  }
}

void PG::_scan_snaps(ScrubMap &smap) 
{
  for (map<hobject_t, ScrubMap::object>::iterator i = smap.objects.begin();
       i != smap.objects.end();
       ++i) {
    const hobject_t &hoid = i->first;
    ScrubMap::object &o = i->second;

    if (hoid.snap < CEPH_MAXSNAP) {
      // fake nlinks for old primaries
      bufferlist bl;
      bl.push_back(o.attrs[OI_ATTR]);
      object_info_t oi(bl);
      if (oi.snaps.empty()) {
	// Just head
	o.nlinks = 1;
      } else if (oi.snaps.size() == 1) {
	// Just head + only snap
	o.nlinks = 2;
      } else {
	// Just head + 1st and last snaps
	o.nlinks = 3;
      }

      // check and if necessary fix snap_mapper
      set<snapid_t> oi_snaps(oi.snaps.begin(), oi.snaps.end());
      set<snapid_t> cur_snaps;
      int r = snap_mapper.get_snaps(hoid, &cur_snaps);
      if (r != 0 && r != -ENOENT) {
	derr << __func__ << ": get_snaps returned " << cpp_strerror(r) << dendl;
	assert(0);
      }
      if (r == -ENOENT || cur_snaps != oi_snaps) {
	ObjectStore::Transaction t;
	OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
	if (r == 0) {
	  r = snap_mapper.remove_oid(hoid, &_t);
	  if (r != 0) {
	    derr << __func__ << ": remove_oid returned " << cpp_strerror(r)
		 << dendl;
	    assert(0);
	  }
	  osd->clog.error() << "osd." << osd->whoami
			    << " found snap mapper error on pg "
			    << info.pgid
			    << " oid " << hoid << " snaps in mapper: "
			    << cur_snaps << ", oi: "
			    << oi_snaps
			    << "...repaired";
	} else {
	  osd->clog.error() << "osd." << osd->whoami
			    << " found snap mapper error on pg "
			    << info.pgid
			    << " oid " << hoid << " snaps missing in mapper"
			    << ", should be: "
			    << oi_snaps
			    << "...repaired";
	}
	snap_mapper.add_oid(hoid, oi_snaps, &_t);
	r = osd->store->apply_transaction(t);
	if (r != 0) {
	  derr << __func__ << ": apply_transaction got " << cpp_strerror(r)
	       << dendl;
	}
      }
    } else {
      o.nlinks = 1;
    }
  }
}

/*
 * build a scrub map over a chunk without releasing the lock
 * only used by chunky scrub
 */
int PG::build_scrub_map_chunk(
  ScrubMap &map,
  hobject_t start, hobject_t end, bool deep,
  ThreadPool::TPHandle &handle)
{
  dout(10) << "build_scrub_map" << dendl;
  dout(20) << "scrub_map_chunk [" << start << "," << end << ")" << dendl;

  map.valid_through = info.last_update;

  // objects
  vector<hobject_t> ls;
  int ret = osd->store->collection_list_range(coll, start, end, 0, &ls);
  if (ret < 0) {
    dout(5) << "collection_list_range error: " << ret << dendl;
    return ret;
  }

  _scan_list(map, ls, deep, handle);
  _scan_snaps(map);

  // pg attrs
  osd->store->collection_getattrs(coll, map.attrs);
  dout(10) << __func__ << " done." << dendl;

  return 0;
}

/*
 * build a (sorted) summary of pg content for purposes of scrubbing
 * called while holding pg lock
 */ 
void PG::build_scrub_map(ScrubMap &map, ThreadPool::TPHandle &handle)
{
  dout(10) << "build_scrub_map" << dendl;

  map.valid_through = info.last_update;
  epoch_t epoch = get_osdmap()->get_epoch();

  unlock();

  // wait for any writes on our pg to flush to disk first.  this avoids races
  // with scrub starting immediately after trim or recovery completion.
  osr->flush();

  // objects
  vector<hobject_t> ls;
  osd->store->collection_list(coll, ls);

  _scan_list(map, ls, false, handle);
  lock();
  _scan_snaps(map);

  if (pg_has_reset_since(epoch)) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    return;
  }


  dout(10) << "PG relocked, finalizing" << dendl;

  // pg attrs
  osd->store->collection_getattrs(coll, map.attrs);

  dout(10) << __func__ << " done." << dendl;
}


/* 
 * build a summary of pg content changed starting after v
 * called while holding pg lock
 */
void PG::build_inc_scrub_map(
  ScrubMap &map, eversion_t v,
  ThreadPool::TPHandle &handle)
{
  map.valid_through = last_update_applied;
  map.incr_since = v;
  vector<hobject_t> ls;
  list<pg_log_entry_t>::const_iterator p;
  if (v == pg_log.get_tail()) {
    p = pg_log.get_log().log.begin();
  } else if (v > pg_log.get_tail()) {
    p = pg_log.get_log().find_entry(v);
    ++p;
  } else {
    assert(0);
  }
  
  for (; p != pg_log.get_log().log.end(); ++p) {
    if (p->is_update()) {
      ls.push_back(p->soid);
      map.objects[p->soid].negative = false;
    } else if (p->is_delete()) {
      map.objects[p->soid].negative = true;
    }
  }

  _scan_list(map, ls, false, handle);
  // pg attrs
  osd->store->collection_getattrs(coll, map.attrs);
}

void PG::repair_object(const hobject_t& soid, ScrubMap::object *po, int bad_peer, int ok_peer)
{
  dout(10) << "repair_object " << soid << " bad_peer osd." << bad_peer << " ok_peer osd." << ok_peer << dendl;
  eversion_t v;
  bufferlist bv;
  bv.push_back(po->attrs[OI_ATTR]);
  object_info_t oi(bv);
  if (bad_peer != acting[0]) {
    peer_missing[bad_peer].add(soid, oi.version, eversion_t());
  } else {
    // We should only be scrubbing if the PG is clean.
    assert(waiting_for_missing_object.empty());

    pg_log.missing_add(soid, oi.version, eversion_t());
    missing_loc[soid].insert(ok_peer);
    missing_loc_sources.insert(ok_peer);

    pg_log.set_last_requested(0);
  }
}

/* replica_scrub
 *
 * Classic behavior:
 *
 * If msg->scrub_from is not set, replica_scrub calls build_scrubmap to
 * build a complete map (with the pg lock dropped).
 *
 * If msg->scrub_from is set, replica_scrub sets scrubber.finalizing.
 * Similarly to scrub, if last_update_applied is behind info.last_update
 * replica_scrub returns to be requeued by sub_op_modify_applied.
 * replica_scrub then builds an incremental scrub map with the 
 * pg lock held.
 *
 * Chunky behavior:
 *
 * Wait for last_update_applied to match msg->scrub_to as above. Wait
 * for pushes to complete in case of recent recovery. Build a single
 * scrubmap of objects that are in the range [msg->start, msg->end).
 */
void PG::replica_scrub(
  MOSDRepScrub *msg,
  ThreadPool::TPHandle &handle)
{
  assert(!scrubber.active_rep_scrub);
  dout(7) << "replica_scrub" << dendl;

  if (msg->map_epoch < info.history.same_interval_since) {
    if (scrubber.finalizing) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      scrubber.finalizing = 0;
    } else {
      dout(10) << "replica_scrub discarding old replica_scrub from "
	       << msg->map_epoch << " < " << info.history.same_interval_since 
	       << dendl;
    }
    return;
  }

  ScrubMap map;

  if (msg->chunky) { // chunky scrub
    if (last_update_applied < msg->scrub_to) {
      dout(10) << "waiting for last_update_applied to catch up" << dendl;
      scrubber.active_rep_scrub = msg;
      msg->get();
      return;
    }

    if (active_pushes > 0) {
      dout(10) << "waiting for active pushes to finish" << dendl;
      scrubber.active_rep_scrub = msg;
      msg->get();
      return;
    }

    build_scrub_map_chunk(
      map, msg->start, msg->end, msg->deep,
      handle);

  } else {
    if (msg->scrub_from > eversion_t()) {
      if (scrubber.finalizing) {
        assert(last_update_applied == info.last_update);
        assert(last_update_applied == msg->scrub_to);
      } else {
        scrubber.finalizing = 1;
        if (last_update_applied != msg->scrub_to) {
          scrubber.active_rep_scrub = msg;
          msg->get();
          return;
        }
      }
      build_inc_scrub_map(map, msg->scrub_from, handle);
      scrubber.finalizing = 0;
    } else {
      build_scrub_map(map, handle);
    }

    if (msg->map_epoch < info.history.same_interval_since) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      return;
    }
  }

  vector<OSDOp> scrub(1);
  scrub[0].op.op = CEPH_OSD_OP_SCRUB_MAP;
  hobject_t poid;
  eversion_t v;
  osd_reqid_t reqid;
  MOSDSubOp *subop = new MOSDSubOp(reqid, info.pgid, poid, false, 0,
				   msg->map_epoch, osd->get_tid(), v);
  ::encode(map, subop->get_data());
  subop->ops = scrub;

  osd->send_message_osd_cluster(subop, msg->get_connection());
}

/* Scrub:
 * PG_STATE_SCRUBBING is set when the scrub is queued
 * 
 * scrub will be chunky if all OSDs in PG support chunky scrub
 * scrub will fall back to classic in any other case
 */
void PG::scrub(ThreadPool::TPHandle &handle)
{
  lock();
  if (deleting) {
    unlock();
    return;
  }

  if (!is_primary() || !is_active() || !is_clean() || !is_scrubbing()) {
    dout(10) << "scrub -- not primary or active or not clean" << dendl;
    state_clear(PG_STATE_SCRUBBING);
    state_clear(PG_STATE_REPAIR);
    state_clear(PG_STATE_DEEP_SCRUB);
    publish_stats_to_osd();
    unlock();
    return;
  }

  // when we're starting a scrub, we need to determine which type of scrub to do
  if (!scrubber.active) {
    OSDMapRef curmap = osd->get_osdmap();
    scrubber.is_chunky = true;
    for (unsigned i=1; i<acting.size(); i++) {
      ConnectionRef con = osd->get_con_osd_cluster(acting[i], get_osdmap()->get_epoch());
      if (!con)
	continue;
      if (!(con->features & CEPH_FEATURE_CHUNKY_SCRUB)) {
        dout(20) << "OSD " << acting[i]
                 << " does not support chunky scrubs, falling back to classic"
                 << dendl;
        scrubber.is_chunky = false;
        break;
      }
    }

    if (scrubber.is_chunky) {
      scrubber.deep = state_test(PG_STATE_DEEP_SCRUB);
    } else {
      state_clear(PG_STATE_DEEP_SCRUB);
    }

    dout(10) << "starting a new " << (scrubber.is_chunky ? "chunky" : "classic") << " scrub" << dendl;
  }

  if (scrubber.is_chunky) {
    chunky_scrub(handle);
  } else {
    classic_scrub(handle);
  }

  unlock();
}

/*
 * Classic scrub is a two stage scrub: an initial scrub with writes enabled
 * followed by a finalize with writes blocked.
 *
 * A request is sent out to all replicas for initial scrub maps. Once they reply
 * (sub_op_scrub_map) writes are blocked for all objects in the PG.
 *
 * Finalize: Primaries and replicas wait for all writes in the log to be applied
 * (op_applied), then builds an incremental scrub of all the changes since the
 * beginning of the scrub.
 *
 * Once the primary has received all maps, it compares them and performs
 * repairs.
 *
 * The initial stage of the scrub is handled by scrub_wq and the final stage by
 * scrub_finalize_wq.
 *
 * Relevant variables:
 *
 * scrubber.waiting_on (int)
 * scrubber.waiting_on_whom
 *    Number of people who still need to build an initial/incremental scrub map.
 *    This is decremented in sub_op_scrub_map.
 *
 * last_update_applied
 *    The last update that's hit the disk. In the finalize stage, we block
 *    writes and wait for all writes to flush by checking:
 *
 *      last_update_appied == info.last_update
 *
 *    This is checked in op_applied.
 *
 *  scrubber.block_writes
 *    Flag to determine if writes are blocked.
 *
 *  finalizing scrub
 *    Flag set when we're in the finalize stage.
 *
 */
void PG::classic_scrub(ThreadPool::TPHandle &handle)
{
  if (!scrubber.active) {
    dout(10) << "scrub start" << dendl;
    scrubber.active = true;
    scrubber.classic = true;

    publish_stats_to_osd();
    scrubber.received_maps.clear();
    scrubber.epoch_start = info.history.same_interval_since;

    osd->inc_scrubs_active(scrubber.reserved);
    if (scrubber.reserved) {
      scrubber.reserved = false;
      scrubber.reserved_peers.clear();
    }

    /* scrubber.waiting_on == 0 iff all replicas have sent the requested maps and
     * the primary has done a final scrub (which in turn can only happen if
     * last_update_applied == info.last_update)
     */
    scrubber.waiting_on = acting.size();
    scrubber.waiting_on_whom.insert(acting.begin(), acting.end());

    // request maps from replicas
    for (unsigned i=1; i<acting.size(); i++) {
      _request_scrub_map_classic(acting[i], eversion_t());
    }

    // Unlocks and relocks...
    scrubber.primary_scrubmap = ScrubMap();
    build_scrub_map(scrubber.primary_scrubmap, handle);

    if (scrubber.epoch_start != info.history.same_interval_since) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      scrub_clear_state();
      scrub_unreserve_replicas();
      return;
    }

    --scrubber.waiting_on;
    scrubber.waiting_on_whom.erase(osd->whoami);

    if (scrubber.waiting_on == 0) {
      // the replicas have completed their scrub map, so lock out writes
      scrubber.block_writes = true;
    } else {
      dout(10) << "wait for replicas to build initial scrub map" << dendl;
      return;
    }

    if (last_update_applied != info.last_update) {
      dout(10) << "wait for cleanup" << dendl;
      return;
    }

    // fall through if last_update_applied == info.last_update and scrubber.waiting_on == 0

    // request incrementals from replicas
    scrub_gather_replica_maps();
    ++scrubber.waiting_on;
    scrubber.waiting_on_whom.insert(osd->whoami);
  }
    
  dout(10) << "clean up scrub" << dendl;
  assert(last_update_applied == info.last_update);

  scrubber.finalizing = true;

  if (scrubber.epoch_start != info.history.same_interval_since) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    scrub_clear_state();
    scrub_unreserve_replicas();
    return;
  }
  
  if (scrubber.primary_scrubmap.valid_through != pg_log.get_head()) {
    ScrubMap incr;
    build_inc_scrub_map(incr, scrubber.primary_scrubmap.valid_through, handle);
    scrubber.primary_scrubmap.merge_incr(incr);
  }
  
  --scrubber.waiting_on;
  scrubber.waiting_on_whom.erase(osd->whoami);
  if (scrubber.waiting_on == 0) {
    assert(last_update_applied == info.last_update);
    osd->scrub_finalize_wq.queue(this);
  }
}

/*
 * Chunky scrub scrubs objects one chunk at a time with writes blocked for that
 * chunk.
 *
 * The object store is partitioned into chunks which end on hash boundaries. For
 * each chunk, the following logic is performed:
 *
 *  (1) Block writes on the chunk
 *  (2) Request maps from replicas
 *  (3) Wait for pushes to be applied (after recovery)
 *  (4) Wait for writes to flush on the chunk
 *  (5) Wait for maps from replicas
 *  (6) Compare / repair all scrub maps
 *
 * This logic is encoded in the very linear state machine:
 *
 *           +------------------+
 *  _________v__________        |
 * |                    |       |
 * |      INACTIVE      |       |
 * |____________________|       |
 *           |                  |
 *           |   +----------+   |
 *  _________v___v______    |   |
 * |                    |   |   |
 * |      NEW_CHUNK     |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |     WAIT_PUSHES    |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |  WAIT_LAST_UPDATE  |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |      BUILD_MAP     |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |    WAIT_REPLICAS   |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |    COMPARE_MAPS    |   |   |
 * |____________________|   |   |
 *           |   |          |   |
 *           |   +----------+   |
 *  _________v__________        |
 * |                    |       |
 * |       FINISH       |       |
 * |____________________|       |
 *           |                  |
 *           +------------------+
 *
 * The primary determines the last update from the subset by walking the log. If
 * it sees a log entry pertaining to a file in the chunk, it tells the replicas
 * to wait until that update is applied before building a scrub map. Both the
 * primary and replicas will wait for any active pushes to be applied.
 *
 * In contrast to classic_scrub, chunky_scrub is entirely handled by scrub_wq.
 *
 * scrubber.state encodes the current state of the scrub (refer to state diagram
 * for details).
 */
void PG::chunky_scrub(ThreadPool::TPHandle &handle)
{
  // check for map changes
  if (scrubber.is_chunky_scrub_active()) {
    if (scrubber.epoch_start != info.history.same_interval_since) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      scrub_clear_state();
      scrub_unreserve_replicas();
      return;
    }
  }

  bool done = false;
  int ret;

  while (!done) {
    dout(20) << "scrub state " << PGScrubber::state_string(scrubber.state) << dendl;

    switch (scrubber.state) {
      case PGScrubber::INACTIVE:
        dout(10) << "scrub start" << dendl;

        publish_stats_to_osd();
        scrubber.epoch_start = info.history.same_interval_since;
        scrubber.active = true;

	osd->inc_scrubs_active(scrubber.reserved);
	if (scrubber.reserved) {
	  scrubber.reserved = false;
	  scrubber.reserved_peers.clear();
	}

        scrubber.start = hobject_t();
        scrubber.state = PGScrubber::NEW_CHUNK;

        break;

      case PGScrubber::NEW_CHUNK:
        scrubber.primary_scrubmap = ScrubMap();
        scrubber.received_maps.clear();

        {

          // get the start and end of our scrub chunk
          //
          // start and end need to lie on a hash boundary. We test for this by
          // requesting a list and searching backward from the end looking for a
          // boundary. If there's no boundary, we request a list after the first
          // list, and so forth.

          bool boundary_found = false;
          hobject_t start = scrubber.start;
          while (!boundary_found) {
            vector<hobject_t> objects;
            ret = osd->store->collection_list_partial(coll, start,
                                                      g_conf->osd_scrub_chunk_min,
						      g_conf->osd_scrub_chunk_max,
						      0,
                                                      &objects, &scrubber.end);
            assert(ret >= 0);

            // in case we don't find a boundary: start again at the end
            start = scrubber.end;

            // special case: reached end of file store, implicitly a boundary
            if (objects.empty()) {
              break;
            }

            // search backward from the end looking for a boundary
            objects.push_back(scrubber.end);
            while (!boundary_found && objects.size() > 1) {
              hobject_t end = objects.back().get_boundary();
              objects.pop_back();

              if (objects.back().get_filestore_key() != end.get_filestore_key()) {
                scrubber.end = end;
                boundary_found = true;
              }
            }
          }
        }

        scrubber.block_writes = true;

        // walk the log to find the latest update that affects our chunk
        scrubber.subset_last_update = pg_log.get_tail();
        for (list<pg_log_entry_t>::const_iterator p = pg_log.get_log().log.begin();
             p != pg_log.get_log().log.end();
             ++p) {
          if (p->soid >= scrubber.start && p->soid < scrubber.end)
            scrubber.subset_last_update = p->version;
        }

        // ask replicas to wait until last_update_applied >= scrubber.subset_last_update and then scan
        scrubber.waiting_on_whom.insert(osd->whoami);
        ++scrubber.waiting_on;

        // request maps from replicas
        for (unsigned i=1; i<acting.size(); i++) {
          _request_scrub_map(acting[i], scrubber.subset_last_update,
                             scrubber.start, scrubber.end, scrubber.deep);
          scrubber.waiting_on_whom.insert(acting[i]);
          ++scrubber.waiting_on;
        }

        scrubber.state = PGScrubber::WAIT_PUSHES;

        break;

      case PGScrubber::WAIT_PUSHES:
        if (active_pushes == 0) {
          scrubber.state = PGScrubber::WAIT_LAST_UPDATE;
        } else {
          dout(15) << "wait for pushes to apply" << dendl;
          done = true;
        }
        break;

      case PGScrubber::WAIT_LAST_UPDATE:
        if (last_update_applied >= scrubber.subset_last_update) {
          scrubber.state = PGScrubber::BUILD_MAP;
        } else {
          // will be requeued by op_applied
          dout(15) << "wait for writes to flush" << dendl;
          done = true;
        }
        break;

      case PGScrubber::BUILD_MAP:
        assert(last_update_applied >= scrubber.subset_last_update);

        // build my own scrub map
        ret = build_scrub_map_chunk(scrubber.primary_scrubmap,
                                    scrubber.start, scrubber.end,
                                    scrubber.deep,
				    handle);
        if (ret < 0) {
          dout(5) << "error building scrub map: " << ret << ", aborting" << dendl;
          scrub_clear_state();
          scrub_unreserve_replicas();
          return;
        }

        --scrubber.waiting_on;
        scrubber.waiting_on_whom.erase(osd->whoami);

        scrubber.state = PGScrubber::WAIT_REPLICAS;
        break;

      case PGScrubber::WAIT_REPLICAS:
        if (scrubber.waiting_on > 0) {
          // will be requeued by sub_op_scrub_map
          dout(10) << "wait for replicas to build scrub map" << dendl;
          done = true;
        } else {
          scrubber.state = PGScrubber::COMPARE_MAPS;
        }
        break;

      case PGScrubber::COMPARE_MAPS:
        assert(last_update_applied >= scrubber.subset_last_update);
        assert(scrubber.waiting_on == 0);

        scrub_compare_maps();
        scrubber.block_writes = false;
	scrubber.run_callbacks();

        // requeue the writes from the chunk that just finished
        requeue_ops(waiting_for_active);

        if (scrubber.end < hobject_t::get_max()) {
          // schedule another leg of the scrub
          scrubber.start = scrubber.end;

          scrubber.state = PGScrubber::NEW_CHUNK;
          osd->scrub_wq.queue(this);
          done = true;
        } else {
          scrubber.state = PGScrubber::FINISH;
        }

        break;

      case PGScrubber::FINISH:
        scrub_finish();
        scrubber.state = PGScrubber::INACTIVE;
        done = true;

        break;

      default:
        assert(0);
    }
  }
}

void PG::scrub_clear_state()
{
  assert(_lock.is_locked());
  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_REPAIR);
  state_clear(PG_STATE_DEEP_SCRUB);
  publish_stats_to_osd();

  // active -> nothing.
  if (scrubber.active)
    osd->dec_scrubs_active();

  requeue_ops(waiting_for_active);

  if (scrubber.queue_snap_trim) {
    dout(10) << "scrub finished, requeuing snap_trimmer" << dendl;
    queue_snap_trim();
  }

  scrubber.reset();

  // type-specific state clear
  _scrub_clear_state();
}

bool PG::scrub_gather_replica_maps()
{
  assert(scrubber.waiting_on == 0);
  assert(_lock.is_locked());

  for (map<int,ScrubMap>::iterator p = scrubber.received_maps.begin();
       p != scrubber.received_maps.end();
       ++p) {
    
    if (scrubber.received_maps[p->first].valid_through != pg_log.get_head()) {
      scrubber.waiting_on++;
      scrubber.waiting_on_whom.insert(p->first);
      // Need to request another incremental map
      _request_scrub_map_classic(p->first, p->second.valid_through);
    }
  }
  
  if (scrubber.waiting_on > 0) {
    return false;
  } else {
    return true;
  }
}

enum PG::error_type PG::_compare_scrub_objects(ScrubMap::object &auth,
				ScrubMap::object &candidate,
				ostream &errorstream)
{
  enum PG::error_type error = CLEAN;
  if (candidate.read_error) {
    // This can occur on stat() of a shallow scrub, but in that case size will
    // be invalid, and this will be over-ridden below.
    error = DEEP_ERROR;
    errorstream << "candidate had a read error";
  }
  if (auth.digest_present && candidate.digest_present) {
    if (auth.digest != candidate.digest) {
      if (error != CLEAN)
        errorstream << ", ";
      error = DEEP_ERROR;

      errorstream << "digest " << candidate.digest
                  << " != known digest " << auth.digest;
    }
  }
  if (auth.omap_digest_present && candidate.omap_digest_present) {
    if (auth.omap_digest != candidate.omap_digest) {
      if (error != CLEAN)
        errorstream << ", ";
      error = DEEP_ERROR;

      errorstream << "omap_digest " << candidate.omap_digest
                  << " != known omap_digest " << auth.omap_digest;
    }
  }
  // Shallow error takes precendence because this will be seen by
  // both types of scrubs.
  if (auth.size != candidate.size) {
    if (error != CLEAN)
      errorstream << ", ";
    error = SHALLOW_ERROR;
    errorstream << "size " << candidate.size 
		<< " != known size " << auth.size;
  }
  for (map<string,bufferptr>::const_iterator i = auth.attrs.begin();
       i != auth.attrs.end();
       ++i) {
    if (!candidate.attrs.count(i->first)) {
      if (error != CLEAN)
        errorstream << ", ";
      error = SHALLOW_ERROR;
      errorstream << "missing attr " << i->first;
    } else if (candidate.attrs.find(i->first)->second.cmp(i->second)) {
      if (error != CLEAN)
        errorstream << ", ";
      error = SHALLOW_ERROR;
      errorstream << "attr value mismatch " << i->first;
    }
  }
  for (map<string,bufferptr>::const_iterator i = candidate.attrs.begin();
       i != candidate.attrs.end();
       ++i) {
    if (!auth.attrs.count(i->first)) {
      if (error != CLEAN)
        errorstream << ", ";
      error = SHALLOW_ERROR;
      errorstream << "extra attr " << i->first;
    }
  }
  return error;
}



map<int, ScrubMap *>::const_iterator PG::_select_auth_object(
  const hobject_t &obj,
  const map<int,ScrubMap*> &maps)
{
  map<int, ScrubMap *>::const_iterator auth = maps.end();
  for (map<int, ScrubMap *>::const_iterator j = maps.begin();
       j != maps.end();
       ++j) {
    map<hobject_t, ScrubMap::object>::iterator i =
      j->second->objects.find(obj);
    if (i == j->second->objects.end()) {
      continue;
    }
    if (auth == maps.end()) {
      // Something is better than nothing
      // TODO: something is NOT better than nothing, do something like
      // unfound_lost if no valid copies can be found, or just mark unfound
      auth = j;
      dout(10) << __func__ << ": selecting osd " << j->first
	       << " for obj " << obj
	       << ", auth == maps.end()"
	       << dendl;
      continue;
    }
    if (i->second.read_error) {
      // scrub encountered read error, probably corrupt
      dout(10) << __func__ << ": rejecting osd " << j->first
	       << " for obj " << obj
	       << ", read_error"
	       << dendl;
      continue;
    }
    map<string, bufferptr>::iterator k = i->second.attrs.find(OI_ATTR);
    if (k == i->second.attrs.end()) {
      // no object info on object, probably corrupt
      dout(10) << __func__ << ": rejecting osd " << j->first
	       << " for obj " << obj
	       << ", no oi attr"
	       << dendl;
      continue;
    }
    bufferlist bl;
    bl.push_back(k->second);
    object_info_t oi;
    try {
      bufferlist::iterator bliter = bl.begin();
      ::decode(oi, bliter);
    } catch (...) {
      dout(10) << __func__ << ": rejecting osd " << j->first
	       << " for obj " << obj
	       << ", corrupt oi attr"
	       << dendl;
      // invalid object info, probably corrupt
      continue;
    }
    if (oi.size != i->second.size) {
      // invalid size, probably corrupt
      dout(10) << __func__ << ": rejecting osd " << j->first
	       << " for obj " << obj
	       << ", size mismatch"
	       << dendl;
      // invalid object info, probably corrupt
      continue;
    }
    dout(10) << __func__ << ": selecting osd " << j->first
	     << " for obj " << obj
	     << dendl;
    auth = j;
  }
  return auth;
}

void PG::_compare_scrubmaps(const map<int,ScrubMap*> &maps,  
			    map<hobject_t, set<int> > &missing,
			    map<hobject_t, set<int> > &inconsistent,
			    map<hobject_t, int> &authoritative,
			    map<hobject_t, set<int> > &invalid_snapcolls,
			    ostream &errorstream)
{
  map<hobject_t,ScrubMap::object>::const_iterator i;
  map<int, ScrubMap *>::const_iterator j;
  set<hobject_t> master_set;

  // Construct master set
  for (j = maps.begin(); j != maps.end(); ++j) {
    for (i = j->second->objects.begin(); i != j->second->objects.end(); ++i) {
      master_set.insert(i->first);
    }
  }

  // Check maps against master set and each other
  for (set<hobject_t>::const_iterator k = master_set.begin();
       k != master_set.end();
       ++k) {
    map<int, ScrubMap *>::const_iterator auth = _select_auth_object(*k, maps);
    assert(auth != maps.end());
    set<int> cur_missing;
    set<int> cur_inconsistent;
    for (j = maps.begin(); j != maps.end(); ++j) {
      if (j == auth)
	continue;
      if (j->second->objects.count(*k)) {
	// Compare
	stringstream ss;
	enum PG::error_type error = _compare_scrub_objects(auth->second->objects[*k],
	    j->second->objects[*k],
	    ss);
        if (error != CLEAN) {
	  cur_inconsistent.insert(j->first);
          if (error == SHALLOW_ERROR)
	    ++scrubber.shallow_errors;
          else
	    ++scrubber.deep_errors;
	  errorstream << info.pgid << " osd." << acting[j->first]
		      << ": soid " << *k << " " << ss.str() << std::endl;
	}
      } else {
	cur_missing.insert(j->first);
	++scrubber.shallow_errors;
	errorstream << info.pgid
		    << " osd." << acting[j->first] 
		    << " missing " << *k << std::endl;
      }
    }
    assert(auth != maps.end());
    if (!cur_missing.empty()) {
      missing[*k] = cur_missing;
    }
    if (!cur_inconsistent.empty()) {
      inconsistent[*k] = cur_inconsistent;
    }
    if (!cur_inconsistent.empty() || !cur_missing.empty()) {
      authoritative[*k] = auth->first;
    }
  }
}

void PG::scrub_compare_maps() 
{
  dout(10) << "scrub_compare_maps has maps, analyzing" << dendl;

  // construct authoritative scrub map for type specific scrubbing
  ScrubMap authmap(scrubber.primary_scrubmap);

  if (acting.size() > 1) {
    dout(10) << "scrub  comparing replica scrub maps" << dendl;

    stringstream ss;

    // Map from object with errors to good peer
    map<hobject_t, int> authoritative;
    map<int,ScrubMap *> maps;

    dout(2) << "scrub   osd." << acting[0] << " has " 
	    << scrubber.primary_scrubmap.objects.size() << " items" << dendl;
    maps[0] = &scrubber.primary_scrubmap;
    for (unsigned i=1; i<acting.size(); i++) {
      dout(2) << "scrub   osd." << acting[i] << " has " 
	      << scrubber.received_maps[acting[i]].objects.size() << " items" << dendl;
      maps[i] = &scrubber.received_maps[acting[i]];
    }

    _compare_scrubmaps(
      maps,
      scrubber.missing,
      scrubber.inconsistent,
      authoritative,
      scrubber.inconsistent_snapcolls,
      ss);
    dout(2) << ss.str() << dendl;

    if (!authoritative.empty() || !scrubber.inconsistent_snapcolls.empty()) {
      osd->clog.error(ss);
    }

    for (map<hobject_t, int>::iterator i = authoritative.begin();
	 i != authoritative.end();
	 ++i) {
      scrubber.authoritative.insert(
	make_pair(
	  i->first,
	  make_pair(maps[i->second]->objects[i->first], i->second)));
    }

    for (map<hobject_t, int>::iterator i = authoritative.begin();
	 i != authoritative.end();
	 ++i) {
      authmap.objects.erase(i->first);
      authmap.objects.insert(*(maps[i->second]->objects.find(i->first)));
    }
  }

  // ok, do the pg-type specific scrubbing
  _scrub(authmap);
}

void PG::scrub_process_inconsistent()
{
  dout(10) << "process_inconsistent() checking authoritative" << dendl;
  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));

  if (!scrubber.authoritative.empty() || !scrubber.inconsistent.empty()) {
    stringstream ss;
    for (map<hobject_t, set<int> >::iterator obj =
	   scrubber.inconsistent_snapcolls.begin();
	 obj != scrubber.inconsistent_snapcolls.end();
	 ++obj) {
      for (set<int>::iterator j = obj->second.begin();
	   j != obj->second.end();
	   ++j) {
	++scrubber.shallow_errors;
	ss << info.pgid << " " << mode << " " << " object " << obj->first
	   << " has inconsistent snapcolls on " << *j << std::endl;
      }
    }

    ss << info.pgid << " " << mode << " " << scrubber.missing.size() << " missing, "
       << scrubber.inconsistent.size() << " inconsistent objects\n";
    dout(2) << ss.str() << dendl;
    osd->clog.error(ss);
    if (repair) {
      state_clear(PG_STATE_CLEAN);
      for (map<hobject_t, pair<ScrubMap::object, int> >::iterator i =
	     scrubber.authoritative.begin();
	   i != scrubber.authoritative.end();
	   ++i) {
	set<int>::iterator j;
	
	if (scrubber.missing.count(i->first)) {
	  for (j = scrubber.missing[i->first].begin();
	       j != scrubber.missing[i->first].end(); 
	       ++j) {
	    repair_object(i->first, 
	      &(i->second.first),
	      acting[*j],
	      acting[i->second.second]);
	    ++scrubber.fixed;
	  }
	}
	if (scrubber.inconsistent.count(i->first)) {
	  for (j = scrubber.inconsistent[i->first].begin(); 
	       j != scrubber.inconsistent[i->first].end(); 
	       ++j) {
	    repair_object(i->first, 
	      &(i->second.first),
	      acting[*j],
	      acting[i->second.second]);
	    ++scrubber.fixed;
	  }
	}
      }
    }
  }
}

void PG::scrub_finalize()
{
  lock();
  if (deleting) {
    unlock();
    return;
  }

  assert(last_update_applied == info.last_update);

  if (scrubber.epoch_start != info.history.same_interval_since) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    scrub_clear_state();
    scrub_unreserve_replicas();
    unlock();
    return;
  }

  if (!scrub_gather_replica_maps()) {
    dout(10) << "maps not yet up to date, sent out new requests" << dendl;
    unlock();
    return;
  }

  scrub_compare_maps();

  scrub_finish();

  dout(10) << "scrub done" << dendl;
  unlock();
}

// the part that actually finalizes a scrub
void PG::scrub_finish() 
{
  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));

  // type-specific finish (can tally more errors)
  _scrub_finish();

  scrub_process_inconsistent();

  {
    stringstream oss;
    oss << info.pgid << " " << mode << " ";
    int total_errors = scrubber.shallow_errors + scrubber.deep_errors;
    if (total_errors)
      oss << total_errors << " errors";
    else
      oss << "ok";
    if (!deep_scrub && info.stats.stats.sum.num_deep_scrub_errors)
      oss << " ( " << info.stats.stats.sum.num_deep_scrub_errors
          << " remaining deep scrub error(s) )";
    if (repair)
      oss << ", " << scrubber.fixed << " fixed";
    oss << "\n";
    if (total_errors)
      osd->clog.error(oss);
    else
      osd->clog.info(oss);
  }

  // finish up
  unreg_next_scrub();
  utime_t now = ceph_clock_now(g_ceph_context);
  info.history.last_scrub = info.last_update;
  info.history.last_scrub_stamp = now;
  if (scrubber.deep) {
    info.history.last_deep_scrub = info.last_update;
    info.history.last_deep_scrub_stamp = now;
  }
  // Since we don't know which errors were fixed, we can only clear them
  // when every one has been fixed.
  if (repair) {
    if (scrubber.fixed == scrubber.shallow_errors + scrubber.deep_errors) {
      assert(deep_scrub);
      scrubber.shallow_errors = scrubber.deep_errors = 0;
    } else {
      // Deep scrub in order to get corrected error counts
      scrub_after_recovery = true;
    }
  }
  if (deep_scrub) {
    if ((scrubber.shallow_errors == 0) && (scrubber.deep_errors == 0))
      info.history.last_clean_scrub_stamp = now;
    info.stats.stats.sum.num_shallow_scrub_errors = scrubber.shallow_errors;
    info.stats.stats.sum.num_deep_scrub_errors = scrubber.deep_errors;
  } else {
    info.stats.stats.sum.num_shallow_scrub_errors = scrubber.shallow_errors;
    // XXX: last_clean_scrub_stamp doesn't mean the pg is not inconsistent
    // because of deep-scrub errors
    if (scrubber.shallow_errors == 0)
      info.history.last_clean_scrub_stamp = now;
  }
  info.stats.stats.sum.num_scrub_errors = 
    info.stats.stats.sum.num_shallow_scrub_errors +
    info.stats.stats.sum.num_deep_scrub_errors;
  reg_next_scrub();

  {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    dirty_info = true;
    write_if_dirty(*t);
    int tr = osd->store->queue_transaction(osr.get(), t);
    assert(tr == 0);
  }


  if (repair) {
    queue_peering_event(
      CephPeeringEvtRef(
	new CephPeeringEvt(
	  get_osdmap()->get_epoch(),
	  get_osdmap()->get_epoch(),
	  DoRecovery())));
  }

  scrub_clear_state();
  scrub_unreserve_replicas();

  if (is_active() && is_primary()) {
    share_pg_info();
  }
}

void PG::share_pg_info()
{
  dout(10) << "share_pg_info" << dendl;

  // share new pg_info_t with replicas
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    if (peer_info.count(i)) {
      peer_info[i].last_epoch_started = info.last_epoch_started;
      peer_info[i].history.merge(info.history);
    }
    MOSDPGInfo *m = new MOSDPGInfo(get_osdmap()->get_epoch());
    m->pg_list.push_back(
      make_pair(
	pg_notify_t(
	  get_osdmap()->get_epoch(),
	  get_osdmap()->get_epoch(),
	  info),
	pg_interval_map_t()));
    osd->send_message_osd_cluster(peer, m, get_osdmap()->get_epoch());
  }
}

/*
 * Share a new segment of this PG's log with some replicas, after PG is active.
 *
 * Updates peer_missing and peer_info.
 */
void PG::share_pg_log()
{
  dout(10) << __func__ << dendl;
  assert(is_primary());

  vector<int>::const_iterator a = acting.begin();
  assert(a != acting.end());
  vector<int>::const_iterator end = acting.end();
  while (++a != end) {
    int peer(*a);
    pg_missing_t& pmissing(peer_missing[peer]);
    pg_info_t& pinfo(peer_info[peer]);

    MOSDPGLog *m = new MOSDPGLog(info.last_update.epoch, info);
    m->log.copy_after(pg_log.get_log(), pinfo.last_update);

    for (list<pg_log_entry_t>::const_iterator i = m->log.log.begin();
	 i != m->log.log.end();
	 ++i) {
      pmissing.add_next_event(*i);
    }
    pinfo.last_update = m->log.head;

    osd->send_message_osd_cluster(peer, m, get_osdmap()->get_epoch());
  }
}

void PG::update_history_from_master(pg_history_t new_history)
{
  unreg_next_scrub();
  info.history.merge(new_history);
  reg_next_scrub();
}

void PG::fulfill_info(int from, const pg_query_t &query, 
		      pair<int, pg_info_t> &notify_info)
{
  assert(!acting.empty());
  assert(from == acting[0]);
  assert(query.type == pg_query_t::INFO);

  // info
  dout(10) << "sending info" << dendl;
  notify_info = make_pair(from, info);
}

void PG::fulfill_log(int from, const pg_query_t &query, epoch_t query_epoch)
{
  assert(!acting.empty());
  assert(from == acting[0]);
  assert(query.type != pg_query_t::INFO);

  MOSDPGLog *mlog = new MOSDPGLog(get_osdmap()->get_epoch(),
				  info, query_epoch);
  mlog->missing = pg_log.get_missing();

  // primary -> other, when building master log
  if (query.type == pg_query_t::LOG) {
    dout(10) << " sending info+missing+log since " << query.since
	     << dendl;
    if (query.since != eversion_t() && query.since < pg_log.get_tail()) {
      osd->clog.error() << info.pgid << " got broken pg_query_t::LOG since " << query.since
			<< " when my log.tail is " << pg_log.get_tail()
			<< ", sending full log instead\n";
      mlog->log = pg_log.get_log();           // primary should not have requested this!!
    } else
      mlog->log.copy_after(pg_log.get_log(), query.since);
  }
  else if (query.type == pg_query_t::FULLLOG) {
    dout(10) << " sending info+missing+full log" << dendl;
    mlog->log = pg_log.get_log();
  }

  dout(10) << " sending " << mlog->log << " " << mlog->missing << dendl;

  ConnectionRef con = osd->get_con_osd_cluster(from, get_osdmap()->get_epoch());
  if (con) {
    osd->osd->_share_map_outgoing(from, con.get(), get_osdmap());
    osd->send_message_osd_cluster(mlog, con.get());
  } else {
    mlog->put();
  }
}


// true if all OSDs in prior intervals may have crashed, and we need to replay
// false positives are okay, false negatives are not.
bool PG::may_need_replay(const OSDMapRef osdmap) const
{
  bool crashed = false;

  for (map<epoch_t,pg_interval_t>::const_reverse_iterator p = past_intervals.rbegin();
       p != past_intervals.rend();
       ++p) {
    const pg_interval_t &interval = p->second;
    dout(10) << "may_need_replay " << interval << dendl;

    if (interval.last < info.history.last_epoch_started)
      break;  // we don't care

    if (interval.acting.empty())
      continue;

    if (!interval.maybe_went_rw)
      continue;

    // look at whether any of the osds during this interval survived
    // past the end of the interval (i.e., didn't crash and
    // potentially fail to COMMIT a write that it ACKed).
    bool any_survived_interval = false;

    // consider ACTING osds
    for (unsigned i=0; i<interval.acting.size(); i++) {
      int o = interval.acting[i];

      const osd_info_t *pinfo = 0;
      if (osdmap->exists(o))
	pinfo = &osdmap->get_info(o);

      // does this osd appear to have survived through the end of the
      // interval?
      if (pinfo) {
	if (pinfo->up_from <= interval.first && pinfo->up_thru > interval.last) {
	  dout(10) << "may_need_replay  osd." << o
		   << " up_from " << pinfo->up_from << " up_thru " << pinfo->up_thru
		   << " survived the interval" << dendl;
	  any_survived_interval = true;
	}
	else if (pinfo->up_from <= interval.first &&
		 (std::find(acting.begin(), acting.end(), o) != acting.end() ||
		  std::find(up.begin(), up.end(), o) != up.end())) {
	  dout(10) << "may_need_replay  osd." << o
		   << " up_from " << pinfo->up_from << " and is in acting|up,"
		   << " assumed to have survived the interval" << dendl;
	  // (if it hasn't, we will rebuild PriorSet)
	  any_survived_interval = true;
	}
	else if (pinfo->up_from > interval.last &&
		 pinfo->last_clean_begin <= interval.first &&
		 pinfo->last_clean_end > interval.last) {
	  dout(10) << "may_need_replay  prior osd." << o
		   << " up_from " << pinfo->up_from
		   << " and last clean interval ["
		   << pinfo->last_clean_begin << "," << pinfo->last_clean_end
		   << ") survived the interval" << dendl;
	  any_survived_interval = true;
	}
      }
    }

    if (!any_survived_interval) {
      dout(3) << "may_need_replay  no known survivors of interval "
	      << interval.first << "-" << interval.last
	      << ", may need replay" << dendl;
      crashed = true;
      break;
    }
  }

  return crashed;
}

bool PG::is_split(OSDMapRef lastmap, OSDMapRef nextmap)
{
  return info.pgid.is_split(
    lastmap->get_pg_num(pool.id),
    nextmap->get_pg_num(pool.id),
    0);
}

bool PG::acting_up_affected(const vector<int>& newup, const vector<int>& newacting)
{
  if (acting != newacting || up != newup) {
    dout(20) << "acting_up_affected newup " << newup << " newacting " << newacting << dendl;
    return true;
  } else {
    return false;
  }
}

bool PG::old_peering_msg(epoch_t reply_epoch, epoch_t query_epoch)
{
  if (last_peering_reset > reply_epoch ||
      last_peering_reset > query_epoch) {
    dout(10) << "old_peering_msg reply_epoch " << reply_epoch << " query_epoch " << query_epoch
	     << " last_peering_reset " << last_peering_reset
	     << dendl;
    return true;
  }
  return false;
}

void PG::set_last_peering_reset()
{
  dout(20) << "set_last_peering_reset " << get_osdmap()->get_epoch() << dendl;
  last_peering_reset = get_osdmap()->get_epoch();
}

struct FlushState {
  PGRef pg;
  epoch_t epoch;
  FlushState(PG *pg, epoch_t epoch) : pg(pg), epoch(epoch) {}
  ~FlushState() {
    pg->lock();
    if (!pg->pg_has_reset_since(epoch))
      pg->queue_flushed(epoch);
    pg->unlock();
  }
};
typedef std::tr1::shared_ptr<FlushState> FlushStateRef;

void PG::start_flush(ObjectStore::Transaction *t,
		     list<Context *> *on_applied,
		     list<Context *> *on_safe)
{
  // flush in progress ops
  FlushStateRef flush_trigger(
    new FlushState(this, get_osdmap()->get_epoch()));
  t->nop();
  flushed = false;
  on_applied->push_back(new ContainerContext<FlushStateRef>(flush_trigger));
  on_safe->push_back(new ContainerContext<FlushStateRef>(flush_trigger));
}

/* Called before initializing peering during advance_map */
void PG::start_peering_interval(const OSDMapRef lastmap,
				const vector<int>& newup,
				const vector<int>& newacting)
{
  const OSDMapRef osdmap = get_osdmap();

  // -- there was a change! --
  kick();

  set_last_peering_reset();

  vector<int> oldacting, oldup;
  int oldrole = get_role();
  int oldprimary = get_primary();
  acting.swap(oldacting);
  up.swap(oldup);

  up = newup;
  acting = newacting;

  if (info.stats.up != up ||
      info.stats.acting != acting) {
    info.stats.up = up;
    info.stats.acting = acting;
    info.stats.mapping_epoch = info.history.same_interval_since;
  }

  if (up != acting)
    state_set(PG_STATE_REMAPPED);
  else
    state_clear(PG_STATE_REMAPPED);

  int role = osdmap->calc_pg_role(osd->whoami, acting, acting.size());
  set_role(role);

  // did acting, up, primary|acker change?
  if (!lastmap) {
    dout(10) << " no lastmap" << dendl;
    dirty_info = true;
    dirty_big_info = true;
  } else {
    std::stringstream debug;
    bool new_interval = pg_interval_t::check_new_interval(
      oldacting, newacting,
      oldup, newup,
      info.history.same_interval_since,
      info.history.last_epoch_clean,
      osdmap,
      lastmap,
      info.pgid.pool(),
      info.pgid,
      &past_intervals,
      &debug);
    dout(10) << __func__ << ": check_new_interval output: "
	     << debug.str() << dendl;
    if (new_interval) {
      dout(10) << " noting past " << past_intervals.rbegin()->second << dendl;
      dirty_info = true;
      dirty_big_info = true;
    }
  }

  if (oldacting != acting || oldup != up || is_split(lastmap, osdmap)) {
    info.history.same_interval_since = osdmap->get_epoch();
  }
  if (oldup != up) {
    info.history.same_up_since = osdmap->get_epoch();
  }
  if (oldprimary != get_primary()) {
    info.history.same_primary_since = osdmap->get_epoch();
  }

  dout(10) << " up " << oldup << " -> " << up 
	   << ", acting " << oldacting << " -> " << acting 
	   << ", role " << oldrole << " -> " << role << dendl; 

  // deactivate.
  state_clear(PG_STATE_ACTIVE);
  state_clear(PG_STATE_DOWN);
  state_clear(PG_STATE_RECOVERY_WAIT);
  state_clear(PG_STATE_RECOVERING);

  peer_missing.clear();
  peer_purged.clear();

  // reset primary state?
  if (oldrole == 0 || get_role() == 0)
    clear_primary_state();

    
  // pg->on_*
  on_change();

  assert(!deleting);

  if (role != oldrole) {
    // old primary?
    if (oldrole == 0) {
      state_clear(PG_STATE_CLEAN);
      clear_publish_stats();
	
      // take replay queue waiters
      list<OpRequestRef> ls;
      for (map<eversion_t,OpRequestRef>::iterator it = replay_queue.begin();
	   it != replay_queue.end();
	   ++it)
	ls.push_back(it->second);
      replay_queue.clear();
      requeue_ops(ls);
    }

    on_role_change();

    // take active waiters
    requeue_ops(waiting_for_active);

    // should we tell the primary we are here?
    send_notify = (role != 0);
      
  } else {
    // no role change.
    // did primary change?
    if (get_primary() != oldprimary) {    
      // we need to announce
      send_notify = true;
        
      dout(10) << *this << " " << oldacting << " -> " << acting 
	       << ", acting primary " 
	       << oldprimary << " -> " << get_primary() 
	       << dendl;
    } else {
      // primary is the same.
      if (role == 0) {
	// i am (still) primary. but my replica set changed.
	state_clear(PG_STATE_CLEAN);
	  
	dout(10) << oldacting << " -> " << acting
		 << ", replicas changed" << dendl;
      }
    }
  }
  // make sure we clear out any pg_temp change requests
  osd->remove_want_pg_temp(info.pgid);
  cancel_recovery();

  if (acting.empty() && !up.empty() && up[0] == osd->whoami) {
    dout(10) << " acting empty, but i am up[0], clearing pg_temp" << dendl;
    osd->queue_want_pg_temp(info.pgid, acting);
  }
}

void PG::proc_primary_info(ObjectStore::Transaction &t, const pg_info_t &oinfo)
{
  assert(!is_primary());

  unreg_next_scrub();
  if (info.history.merge(oinfo.history))
    dirty_info = true;
  reg_next_scrub();

  if (last_complete_ondisk.epoch >= info.history.last_epoch_started) {
    // DEBUG: verify that the snaps are empty in snap_mapper
    if (g_conf->osd_debug_verify_snaps_on_info) {
      interval_set<snapid_t> p;
      p.union_of(oinfo.purged_snaps, info.purged_snaps);
      p.subtract(info.purged_snaps);
      if (!p.empty()) {
	for (interval_set<snapid_t>::iterator i = p.begin();
	     i != p.end();
	     ++i) {
	  for (snapid_t snap = i.get_start();
	       snap != i.get_len() + i.get_start();
	       ++snap) {
	    hobject_t hoid;
	    int r = snap_mapper.get_next_object_to_trim(snap, &hoid);
	    if (r != 0 && r != -ENOENT) {
	      derr << __func__ << ": snap_mapper get_next_object_to_trim returned "
		   << cpp_strerror(r) << dendl;
	      assert(0);
	    } else if (r != -ENOENT) {
	      derr << __func__ << ": snap_mapper get_next_object_to_trim returned "
		   << cpp_strerror(r) << " for object "
		   << hoid << " on snap " << snap
		   << " which should have been fully trimmed " << dendl;
	      assert(0);
	    }
	  }
	}
      }
    }
    info.purged_snaps = oinfo.purged_snaps;
    dirty_info = true;
    dirty_big_info = true;
  }
}

ostream& operator<<(ostream& out, const PG& pg)
{
  return pg.operator<<(out);
}

ostream& PG::operator<<(ostream& out) const
{
  out << "pg[" << info
      << " " << up;
  if (acting != up)
    out << "/" << acting;
  out << " r=" << get_role();
  out << " lpr=" << get_last_peering_reset();

  if (!past_intervals.empty()) {
    out << " pi=" << past_intervals.begin()->first << "-" << past_intervals.rbegin()->second.last
	<< "/" << past_intervals.size();
  }

  if (is_active() &&
      last_update_ondisk != info.last_update)
    out << " luod=" << last_update_ondisk;

  if (recovery_ops_active)
    out << " rops=" << recovery_ops_active;

  if (pg_log.get_tail() != info.log_tail ||
      pg_log.get_head() != info.last_update)
    out << " (info mismatch, " << pg_log.get_log() << ")";

  if (pg_log.get_log().empty()) {
    // shoudl it be?
    if (pg_log.get_head().version - pg_log.get_tail().version != 0) {
      out << " (log bound mismatch, empty)";
    }
  } else {
    if ((pg_log.get_log().log.begin()->version <= pg_log.get_tail()) || // sloppy check
        (pg_log.get_log().log.rbegin()->version != pg_log.get_head() &&
	 !(pg_log.get_head() == pg_log.get_tail()))) {
      out << " (log bound mismatch, actual=["
	  << pg_log.get_log().log.begin()->version << ","
	  << pg_log.get_log().log.rbegin()->version << "]";
      //out << "len=" << log.log.size();
      out << ")";
    }
  }

  if (get_backfill_target() >= 0)
    out << " bft=" << get_backfill_target();

  if (last_complete_ondisk != info.last_complete)
    out << " lcod " << last_complete_ondisk;

  if (get_role() == 0) {
    out << " mlcod " << min_last_complete_ondisk;
  }

  out << " " << pg_state_string(get_state());
  if (should_send_notify())
    out << " NOTIFY";

  if (scrubber.must_repair)
    out << " MUST_REPAIR";
  if (scrubber.must_deep_scrub)
    out << " MUST_DEEP_SCRUB";
  if (scrubber.must_scrub)
    out << " MUST_SCRUB";

  //out << " (" << pg_log.get_tail() << "," << pg_log.get_head() << "]";
  if (pg_log.get_missing().num_missing()) {
    out << " m=" << pg_log.get_missing().num_missing();
    if (is_primary()) {
      int unfound = get_num_unfound();
      if (unfound)
	out << " u=" << unfound;
    }
  }
  if (snap_trimq.size())
    out << " snaptrimq=" << snap_trimq;

  out << "]";


  return out;
}

bool PG::can_discard_op(OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->request);
  if (OSD::op_is_discardable(m)) {
    dout(20) << " discard " << *m << dendl;
    return true;
  } else if (op->may_write() &&
	     (!is_primary() ||
	      !same_for_modify_since(m->get_map_epoch()))) {
    osd->handle_misdirected_op(this, op);
    return true;
  } else if (op->may_read() &&
	     !same_for_read_since(m->get_map_epoch())) {
    osd->handle_misdirected_op(this, op);
    return true;
  } else if (is_replay()) {
    if (m->get_version().version > 0) {
      dout(7) << " queueing replay at " << m->get_version()
	      << " for " << *m << dendl;
      replay_queue[m->get_version()] = op;
      op->mark_delayed("waiting for replay");
      return true;
    }
  }
  return false;
}

bool PG::can_discard_subop(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp *>(op->request);
  assert(m->get_header().type == MSG_OSD_SUBOP);

  // same pg?
  //  if pg changes _at all_, we reset and repeer!
  if (old_peering_msg(m->map_epoch, m->map_epoch)) {
    dout(10) << "handle_sub_op pg changed " << info.history
	     << " after " << m->map_epoch
	     << ", dropping" << dendl;
    return true;
  }
  return false;
}

bool PG::can_discard_scan(OpRequestRef op)
{
  MOSDPGScan *m = static_cast<MOSDPGScan *>(op->request);
  assert(m->get_header().type == MSG_OSD_PG_SCAN);

  if (old_peering_msg(m->map_epoch, m->query_epoch)) {
    dout(10) << " got old scan, ignoring" << dendl;
    return true;
  }
  return false;
}

bool PG::can_discard_backfill(OpRequestRef op)
{
  MOSDPGBackfill *m = static_cast<MOSDPGBackfill *>(op->request);
  assert(m->get_header().type == MSG_OSD_PG_BACKFILL);

  if (old_peering_msg(m->map_epoch, m->query_epoch)) {
    dout(10) << " got old backfill, ignoring" << dendl;
    return true;
  }

  return false;

}

bool PG::can_discard_request(OpRequestRef op)
{
  switch (op->request->get_type()) {
  case CEPH_MSG_OSD_OP:
    return can_discard_op(op);
  case MSG_OSD_SUBOP:
    return can_discard_subop(op);
  case MSG_OSD_SUBOPREPLY:
    return false;
  case MSG_OSD_PG_SCAN:
    return can_discard_scan(op);

  case MSG_OSD_PG_BACKFILL:
    return can_discard_backfill(op);
  }
  return true;
}

bool PG::split_request(OpRequestRef op, unsigned match, unsigned bits)
{
  unsigned mask = ~((~0)<<bits);
  switch (op->request->get_type()) {
  case CEPH_MSG_OSD_OP:
    return (static_cast<MOSDOp*>(op->request)->get_pg().m_seed & mask) == match;
  case MSG_OSD_SUBOP:
    return false;
  case MSG_OSD_SUBOPREPLY:
    return false;
  case MSG_OSD_PG_SCAN:
    return false;
  case MSG_OSD_PG_BACKFILL:
    return false;
  }
  return false;
}

bool PG::op_must_wait_for_map(OSDMapRef curmap, OpRequestRef op)
{
  switch (op->request->get_type()) {
  case CEPH_MSG_OSD_OP:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDOp*>(op->request)->get_map_epoch());

  case MSG_OSD_SUBOP:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDSubOp*>(op->request)->map_epoch);

  case MSG_OSD_SUBOPREPLY:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDSubOpReply*>(op->request)->map_epoch);

  case MSG_OSD_PG_SCAN:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDPGScan*>(op->request)->map_epoch);

  case MSG_OSD_PG_BACKFILL:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDPGBackfill*>(op->request)->map_epoch);
  }
  assert(0);
  return false;
}

void PG::take_waiters()
{
  dout(10) << "take_waiters" << dendl;
  take_op_map_waiters();
  for (list<CephPeeringEvtRef>::iterator i = peering_waiters.begin();
       i != peering_waiters.end();
       ++i) osd->queue_for_peering(this);
  peering_queue.splice(peering_queue.begin(), peering_waiters,
		       peering_waiters.begin(), peering_waiters.end());
}

void PG::handle_peering_event(CephPeeringEvtRef evt, RecoveryCtx *rctx)
{
  dout(10) << "handle_peering_event: " << evt->get_desc() << dendl;
  if (!have_same_or_newer_map(evt->get_epoch_sent())) {
    dout(10) << "deferring event " << evt->get_desc() << dendl;
    peering_waiters.push_back(evt);
    return;
  }
  if (old_peering_evt(evt))
    return;
  recovery_state.handle_event(evt, rctx);
}

void PG::queue_peering_event(CephPeeringEvtRef evt)
{
  if (old_peering_evt(evt))
    return;
  peering_queue.push_back(evt);
  osd->queue_for_peering(this);
}

void PG::queue_notify(epoch_t msg_epoch,
		      epoch_t query_epoch,
		      int from, pg_notify_t& i)
{
  dout(10) << "notify " << i << " from osd." << from << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 MNotifyRec(from, i))));
}

void PG::queue_info(epoch_t msg_epoch,
		     epoch_t query_epoch,
		     int from, pg_info_t& i)
{
  dout(10) << "info " << i << " from osd." << from << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 MInfoRec(from, i, msg_epoch))));
}

void PG::queue_log(epoch_t msg_epoch,
		   epoch_t query_epoch,
		   int from,
		   MOSDPGLog *msg)
{
  dout(10) << "log " << *msg << " from osd." << from << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 MLogRec(from, msg))));
}

void PG::queue_null(epoch_t msg_epoch,
		    epoch_t query_epoch)
{
  dout(10) << "null" << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 NullEvt())));
}

void PG::queue_flushed(epoch_t e)
{
  dout(10) << "flushed" << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(e, e,
					 FlushedEvt())));
}

void PG::queue_query(epoch_t msg_epoch,
		     epoch_t query_epoch,
		     int from, const pg_query_t& q)
{
  dout(10) << "handle_query " << q << " from osd." << from << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 MQuery(from, q, query_epoch))));
}

void PG::handle_advance_map(OSDMapRef osdmap, OSDMapRef lastmap,
			    vector<int>& newup, vector<int>& newacting,
			    RecoveryCtx *rctx)
{
  assert(osdmap->get_epoch() == (lastmap->get_epoch() + 1));
  assert(lastmap->get_epoch() == osdmap_ref->get_epoch());
  assert(lastmap == osdmap_ref);
  dout(10) << "handle_advance_map " << newup << "/" << newacting << dendl;
  update_osdmap_ref(osdmap);
  pool.update(osdmap);
  AdvMap evt(osdmap, lastmap, newup, newacting);
  recovery_state.handle_event(evt, rctx);
}

void PG::handle_activate_map(RecoveryCtx *rctx)
{
  dout(10) << "handle_activate_map " << dendl;
  ActMap evt;
  recovery_state.handle_event(evt, rctx);
  if (osdmap_ref->get_epoch() - last_persisted_osdmap_ref->get_epoch() >
      g_conf->osd_pg_epoch_persisted_max_stale) {
    dout(20) << __func__ << ": Dirtying info: last_persisted is "
	     << last_persisted_osdmap_ref->get_epoch()
	     << " while current is " << osdmap_ref->get_epoch() << dendl;
    dirty_info = true;
  } else {
    dout(20) << __func__ << ": Not dirtying info: last_persisted is "
	     << last_persisted_osdmap_ref->get_epoch()
	     << " while current is " << osdmap_ref->get_epoch() << dendl;
  }
  if (osdmap_ref->check_new_blacklist_entries()) check_blacklisted_watchers();
}

void PG::handle_loaded(RecoveryCtx *rctx)
{
  dout(10) << "handle_loaded" << dendl;
  Load evt;
  recovery_state.handle_event(evt, rctx);
}

void PG::handle_create(RecoveryCtx *rctx)
{
  dout(10) << "handle_create" << dendl;
  Initialize evt;
  recovery_state.handle_event(evt, rctx);
  ActMap evt2;
  recovery_state.handle_event(evt2, rctx);
}

void PG::handle_query_state(Formatter *f)
{
  dout(10) << "handle_query_state" << dendl;
  QueryState q(f);
  recovery_state.handle_event(q, 0);
}

void intrusive_ptr_add_ref(PG *pg) { pg->get("intptr"); }
void intrusive_ptr_release(PG *pg) { pg->put("intptr"); }

#ifdef PG_DEBUG_REFS
  uint64_t get_with_id(PG *pg) { return pg->get_with_id(); }
  void put_with_id(PG *pg, uint64_t id) { return pg->put_with_id(id); }
#endif
