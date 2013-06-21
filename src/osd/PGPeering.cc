// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "PGPeering.h"
#include "OSD.h"

#include "include/types.h"

using namespace PGRecoveryState;

#define dout_subsys ceph_subsys_osd

/*---------Peering--------*/
Peering::Peering(my_context ctx)
  : my_base(ctx), flushed(false)
{
  state_name = "Started/Primary/Peering";
  context< RecoveryState::RecoveryMachine >().log_enter(state_name);

  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  assert(!pg->is_active());
  assert(!pg->is_peering());
  assert(pg->is_primary());
  pg->state_set(PG_STATE_PEERING);
}

boost::statechart::result Peering::react(const QueryState& q)
{
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("past_intervals");
  for (map<epoch_t,pg_interval_t>::const_iterator p = pg->get_past_intervals().begin();
       p != pg->get_past_intervals().end();
       ++p) {
    q.f->open_object_section("past_interval");
    p->second.dump(q.f);
    q.f->close_section();
  }
  q.f->close_section();

  q.f->open_array_section("probing_osds");
  for (set<int>::iterator p = prior_set->probe.begin(); p != prior_set->probe.end(); ++p)
    q.f->dump_int("osd", *p);
  q.f->close_section();

  if (prior_set->pg_down)
    q.f->dump_string("blocked", "peering is blocked due to down osds");

  q.f->open_array_section("down_osds_we_would_probe");
  for (set<int>::iterator p = prior_set->down.begin(); p != prior_set->down.end(); ++p)
    q.f->dump_int("osd", *p);
  q.f->close_section();

  q.f->open_array_section("peering_blocked_by");
  for (map<int,epoch_t>::iterator p = prior_set->blocked_by.begin();
       p != prior_set->blocked_by.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_int("osd", p->first);
    q.f->dump_int("current_lost_at", p->second);
    q.f->dump_string("comment", "starting or marking this osd lost may let us proceed");
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

boost::statechart::result Peering::react(const AdvMap& advmap) 
{
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  dout(10) << "Peering advmap" << dendl;
  if (prior_set.get()->affected_by_map(advmap.osdmap, pg)) {
    dout(1) << "Peering, affected_by_map, going to Reset" << dendl;
    post_event(advmap);
    return transit< RecoveryState::Reset >();
  }
  
  pg->adjust_need_up_thru(advmap.osdmap);
  
  return forward_event();
}


void Peering::exit()
{
  dout(10) << "Leaving Peering" << dendl;
  context< RecoveryState::RecoveryMachine >().log_exit(state_name, enter_time);
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_PEERING);
  pg->clear_probe_targets();
}

/*--------GetInfo---------*/
GetInfo::GetInfo(my_context ctx)
  : my_base(ctx) 
{
  state_name = "Started/Primary/Peering/GetInfo";
  context< RecoveryState::RecoveryMachine >().log_enter(state_name);

  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  pg->generate_past_intervals();
  auto_ptr<PriorSet> &prior_set = context< Peering >().prior_set;

  if (!prior_set.get())
    pg->build_prior(prior_set);

  pg->publish_stats_to_osd();

  get_infos();
  if (peer_info_requested.empty() && !prior_set->pg_down) {
    post_event(GotInfo());
  }
}

void GetInfo::get_infos()
{
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  auto_ptr<PriorSet> &prior_set = context< Peering >().prior_set;

  for (set<int>::const_iterator it = prior_set->probe.begin();
       it != prior_set->probe.end();
       ++it) {
    int peer = *it;
    if (peer == pg->get_osd()->whoami) {
      continue;
    }
    if (pg->get_peer_info().count(peer)) {
      dout(10) << " have osd." << peer << " info " << pg->get_peer_info().find(peer)->second << dendl;
      continue;
    }
    if (peer_info_requested.count(peer)) {
      dout(10) << " already requested info from osd." << peer << dendl;
    } else if (!pg->get_osdmap()->is_up(peer)) {
      dout(10) << " not querying info from down osd." << peer << dendl;
    } else {
      dout(10) << " querying info from osd." << peer << dendl;
      context< RecoveryState::RecoveryMachine >().send_query(
	peer, pg_query_t(pg_query_t::INFO,
			 pg->get_info().history,
			 pg->get_osdmap()->get_epoch()));
      peer_info_requested.insert(peer);
    }
  }
}

boost::statechart::result GetInfo::react(const MNotifyRec& infoevt) 
{
  set<int>::iterator p = peer_info_requested.find(infoevt.from);
  if (p != peer_info_requested.end())
    peer_info_requested.erase(p);

  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  epoch_t old_start = pg->get_info().history.last_epoch_started;
  if (pg->proc_replica_info(infoevt.from, infoevt.notify.info)) {
    // we got something new ...
    auto_ptr<PriorSet> &prior_set = context< Peering >().prior_set;
    if (old_start < pg->get_info().history.last_epoch_started) {
      dout(10) << " last_epoch_started moved forward, rebuilding prior" << dendl;
      pg->build_prior(prior_set);

      // filter out any osds that got dropped from the probe set from
      // peer_info_requested.  this is less expensive than restarting
      // peering (which would re-probe everyone).
      set<int>::iterator p = peer_info_requested.begin();
      while (p != peer_info_requested.end()) {
	if (prior_set->probe.count(*p) == 0) {
	  dout(20) << " dropping osd." << *p << " from info_requested, no longer in probe set" << dendl;
	  peer_info_requested.erase(p++);
	} else {
	  ++p;
	}
      }
      get_infos();
    }

    // are we done getting everything?
    if (peer_info_requested.empty() && !prior_set->pg_down) {
      /*
       * make sure we have at least one !incomplete() osd from the
       * last rw interval.  the incomplete (backfilling) replicas
       * get a copy of the log, but they don't get all the object
       * updates, so they are insufficient to recover changes during
       * that interval.
       */
      if (pg->get_info().history.last_epoch_started) {
	for (map<epoch_t,pg_interval_t>::const_reverse_iterator p = pg->get_past_intervals().rbegin();
	     p != pg->get_past_intervals().rend();
	     ++p) {
	  if (p->first < pg->get_info().history.last_epoch_started)
	    break;
	  if (!p->second.maybe_went_rw)
	    continue;
	  const pg_interval_t& interval = p->second;
	  dout(10) << " last maybe_went_rw interval was " << interval << dendl;
	  OSDMapRef osdmap = pg->get_osdmap();

	  /*
	   * this mirrors the PriorSet calculation: we wait if we
	   * don't have an up (AND !incomplete) node AND there are
	   * nodes down that might be usable.
	   */
	  bool any_up_complete_now = false;
	  bool any_down_now = false;
	  for (unsigned i=0; i<interval.acting.size(); i++) {
	    int o = interval.acting[i];
	    if (!osdmap->exists(o) || osdmap->get_info(o).lost_at > interval.first)
	      continue;  // dne or lost
	    if (osdmap->is_up(o)) {
	      const pg_info_t *pinfo;
	      if (o == pg->get_osd()->whoami) {
		pinfo = &pg->get_info();
	      } else {
		assert(pg->get_peer_info().count(o));
		pinfo = &pg->get_peer_info().find(o)->second;
	      }
	      if (!pinfo->is_incomplete())
		any_up_complete_now = true;
	    } else {
	      any_down_now = true;
	    }
	  }
	  if (!any_up_complete_now && any_down_now) {
	    dout(10) << " no osds up+complete from interval " << interval << dendl;
	    pg->state_set(PG_STATE_DOWN);
	    return discard_event();
	  }
	  break;
	}
      }
      post_event(GotInfo());
    }
  }
  return discard_event();
}

boost::statechart::result GetInfo::react(const QueryState& q)
{
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("requested_info_from");
  for (set<int>::iterator p = peer_info_requested.begin(); p != peer_info_requested.end(); ++p) {
    q.f->open_object_section("osd");
    q.f->dump_int("osd", *p);
    if (pg->get_peer_info().count(*p)) {
      q.f->open_object_section("got_info");
      pg->get_peer_info().find(*p)->second.dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void GetInfo::exit()
{
  context< RecoveryState::RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------GetLog------------*/
GetLog::GetLog(my_context ctx) : 
  my_base(ctx), newest_update_osd(-1), msg(0)
{
  state_name = "Started/Primary/Peering/GetLog";
  context< RecoveryState::RecoveryMachine >().log_enter(state_name);

  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;

  // adjust acting?
  if (!pg->choose_acting(newest_update_osd)) {
    if (!pg->get_want_acting().empty()) {
      post_event(RecoveryState::NeedActingChange());
    } else {
      post_event(RecoveryState::IsIncomplete());
    }
    return;
  }

  // am i the best?
  if (newest_update_osd == pg->get_osd()->whoami) {
    post_event(GotLog());
    return;
  }

  const pg_info_t& best = pg->get_peer_info().find(newest_update_osd)->second;

  // am i broken?
  if (pg->get_info().last_update < best.log_tail) {
    dout(10) << " not contiguous with osd." << newest_update_osd << ", down" << dendl;
    post_event(RecoveryState::IsIncomplete());
    return;
  }

  // how much log to request?
  eversion_t request_log_from = pg->get_info().last_update;
  for (vector<int>::const_iterator p = pg->get_acting().begin() + 1; p != pg->get_acting().end(); ++p) {
    const pg_info_t& ri = pg->get_peer_info().find(*p)->second;
    if (ri.last_update >= best.log_tail && ri.last_update < request_log_from)
      request_log_from = ri.last_update;
  }

  // how much?
  dout(10) << " requesting log from osd." << newest_update_osd << dendl;
  context<RecoveryState::RecoveryMachine>().send_query(
    newest_update_osd,
    pg_query_t(pg_query_t::LOG, request_log_from, pg->get_info().history,
	       pg->get_osdmap()->get_epoch()));
}

boost::statechart::result GetLog::react(const AdvMap& advmap)
{
  // make sure our log source didn't go down.  we need to check
  // explicitly because it may not be part of the prior set, which
  // means the Peering state check won't catch it going down.
  if (!advmap.osdmap->is_up(newest_update_osd)) {
    dout(10) << "GetLog: newest_update_osd osd." << newest_update_osd << " went down" << dendl;
    post_event(advmap);
    return transit< RecoveryState::Reset >();
  }

  // let the Peering state do its checks.
  return forward_event();
}

boost::statechart::result GetLog::react(const MLogRec& logevt)
{
  assert(!msg);
  if (logevt.from != newest_update_osd) {
    dout(10) << "GetLog: discarding log from "
	     << "non-newest_update_osd osd." << logevt.from << dendl;
    return discard_event();
  }
  dout(10) << "GetLog: recieved master log from osd" 
	   << logevt.from << dendl;
  msg = logevt.msg;
  post_event(GotLog());
  return discard_event();
}

boost::statechart::result GetLog::react(const GotLog&)
{
  dout(10) << "leaving GetLog" << dendl;
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  if (msg) {
    dout(10) << "processing master log" << dendl;
    pg->proc_master_log(*context<RecoveryState::RecoveryMachine>().get_cur_transaction(),
			msg->info, msg->log, msg->missing, 
			newest_update_osd);
  }
  return transit< GetMissing >();
}

boost::statechart::result GetLog::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_int("newest_update_osd", newest_update_osd);
  q.f->close_section();
  return forward_event();
}

void GetLog::exit()
{
  context< RecoveryState::RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------GetMissing--------*/
GetMissing::GetMissing(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/GetMissing";
  context< RecoveryState::RecoveryMachine >().log_enter(state_name);

  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  for (vector<int>::const_iterator i = pg->get_acting().begin() + 1;
       i != pg->get_acting().end();
       ++i) {
    const pg_info_t& pi = pg->get_peer_info().find(*i)->second;

    if (pi.is_empty())
      continue;                                // no pg data, nothing divergent

    if (pi.last_update < pg->get_pg_log().get_tail()) {
      dout(10) << " osd." << *i << " is not contiguous, will restart backfill" << dendl;
      pg->set_peer_missing(*i);
      continue;
    }
    if (pi.last_backfill == hobject_t()) {
      dout(10) << " osd." << *i << " will fully backfill; can infer empty missing set" << dendl;
      pg->set_peer_missing(*i);
      continue;
    }

    if (pi.last_update == pi.last_complete &&  // peer has no missing
	pi.last_update == pg->get_info().last_update) {  // peer is up to date
      // replica has no missing and identical log as us.  no need to
      // pull anything.
      // FIXME: we can do better here.  if last_update==last_complete we
      //        can infer the rest!
      dout(10) << " osd." << *i << " has no missing, identical log" << dendl;
      pg->set_peer_missing(*i);
      pg->search_for_missing(pi, &pg->get_peer_missing(*i), *i);
      continue;
    }

    // We pull the log from the peer's last_epoch_started to ensure we
    // get enough log to detect divergent updates.
    eversion_t since(pi.last_epoch_started, 0);
    assert(pi.last_update >= pg->get_info().log_tail);  // or else choose_acting() did a bad thing
    if (pi.log_tail <= since) {
      dout(10) << " requesting log+missing since " << since << " from osd." << *i << dendl;
      context< RecoveryState::RecoveryMachine >().send_query(
	*i,
	pg_query_t(pg_query_t::LOG, since, pg->get_info().history,
		   pg->get_osdmap()->get_epoch()));
    } else {
      dout(10) << " requesting fulllog+missing from osd." << *i
	       << " (want since " << since << " < log.tail " << pi.log_tail << ")"
	       << dendl;
      context< RecoveryState::RecoveryMachine >().send_query(
	*i, pg_query_t(pg_query_t::FULLLOG,
		       pg->get_info().history, pg->get_osdmap()->get_epoch()));
    }
    peer_missing_requested.insert(*i);
  }

  if (peer_missing_requested.empty()) {
    if (pg->get_need_up_thru()) {
      dout(10) << " still need up_thru update before going active" << dendl;
      post_event(NeedUpThru());
      return;
    }

    // all good!
    post_event(CheckRepops());
  }
}

boost::statechart::result GetMissing::react(const MLogRec& logevt)
{
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;

  peer_missing_requested.erase(logevt.from);
  pg->proc_replica_log(*context<RecoveryState::RecoveryMachine>().get_cur_transaction(),
		       logevt.msg->info, logevt.msg->log, logevt.msg->missing, logevt.from);
  
  if (peer_missing_requested.empty()) {
    if (pg->get_need_up_thru()) {
      dout(10) << " still need up_thru update before going active" << dendl;
      post_event(NeedUpThru());
    } else {
      dout(10) << "Got last missing, don't need missing "
	       << "posting CheckRepops" << dendl;
      post_event(CheckRepops());
    }
  }
  return discard_event();
};

boost::statechart::result GetMissing::react(const QueryState& q)
{
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("peer_missing_requested");
  for (set<int>::iterator p = peer_missing_requested.begin(); p != peer_missing_requested.end(); ++p) {
    q.f->open_object_section("osd");
    q.f->dump_int("osd", *p);
    if (pg->get_peer_missing().count(*p)) {
      q.f->open_object_section("got_missing");
      pg->get_peer_missing().find(*p)->second.dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void GetMissing::exit()
{
  context< RecoveryState::RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---WaitFlushedPeering---*/
WaitFlushedPeering::WaitFlushedPeering(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/WaitFlushedPeering";
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  context< RecoveryState::RecoveryMachine >().log_enter(state_name);
  if (context< RecoveryState::RecoveryMachine >().pg->get_flushed())
    post_event(Activate(pg->get_osdmap()->get_epoch()));
}

boost::statechart::result
WaitFlushedPeering::react(const FlushedEvt &evt)
{
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  pg->set_flushed(true);
  pg->requeue_waiting_for_active();
  return transit< WaitFlushedPeering >();
}

boost::statechart::result
WaitFlushedPeering::react(const QueryState &q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for flush");
  return forward_event();
}

/*------WaitUpThru--------*/
WaitUpThru::WaitUpThru(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/WaitUpThru";
  context< RecoveryState::RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result WaitUpThru::react(const ActMap& am)
{
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  if (!pg->get_need_up_thru()) {
    post_event(CheckRepops());
  }
  return forward_event();
}

boost::statechart::result WaitUpThru::react(const MLogRec& logevt)
{
  dout(10) << "searching osd." << logevt.from
           << " log for unfound items" << dendl;
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  bool got_missing = pg->search_for_missing(logevt.msg->info,
                                            &logevt.msg->missing, logevt.from);

  // hmm.. should we?
  (void)got_missing;
  //if (got_missing)
  //pg->osd->queue_for_recovery(pg);

  return discard_event();
}

boost::statechart::result WaitUpThru::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for osdmap to reflect a new up_thru for this osd");
  q.f->close_section();
  return forward_event();
}

void WaitUpThru::exit()
{
  context< RecoveryState::RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------Incomplete--------*/
Incomplete::Incomplete(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/Incomplete";
  context< RecoveryState::RecoveryMachine >().log_enter(state_name);
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;

  pg->state_clear(PG_STATE_PEERING);
  pg->state_set(PG_STATE_INCOMPLETE);
  pg->publish_stats_to_osd();
}

boost::statechart::result Incomplete::react(const AdvMap &advmap) {
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;
  int64_t poolnum = pg->get_info().pgid.pool();

  // Reset if min_size changed, pg might now be able to go active
  if (advmap.lastmap->get_pools().find(poolnum)->second.min_size !=
      advmap.osdmap->get_pools().find(poolnum)->second.min_size) {
    post_event(advmap);
    return transit< RecoveryState::Reset >();
  }

  return forward_event();
}

void Incomplete::exit()
{
  context< RecoveryState::RecoveryMachine >().log_exit(state_name, enter_time);
  PGRecoveryStateInterface *pg = context< RecoveryState::RecoveryMachine >().pg;

  pg->state_clear(PG_STATE_INCOMPLETE);
}

/*---------------------------------------------------*/
#undef dout_prefix
#define dout_prefix (*_dout << (debug_pg ? debug_pg->gen_prefix() : string()) << " PriorSet: ")

PriorSet::PriorSet(const OSDMap &osdmap,
		       const map<epoch_t, pg_interval_t> &past_intervals,
		       const vector<int> &up,
		       const vector<int> &acting,
		       const pg_info_t &info,
		       const PGRecoveryStateInterface *debug_pg)
  : pg_down(false)
{
  /*
   * We have to be careful to gracefully deal with situations like
   * so. Say we have a power outage or something that takes out both
   * OSDs, but the monitor doesn't mark them down in the same epoch.
   * The history may look like
   *
   *  1: A B
   *  2:   B
   *  3:       let's say B dies for good, too (say, from the power spike) 
   *  4: A
   *
   * which makes it look like B may have applied updates to the PG
   * that we need in order to proceed.  This sucks...
   *
   * To minimize the risk of this happening, we CANNOT go active if
   * _any_ OSDs in the prior set are down until we send an MOSDAlive
   * to the monitor such that the OSDMap sets osd_up_thru to an epoch.
   * Then, we have something like
   *
   *  1: A B
   *  2:   B   up_thru[B]=0
   *  3:
   *  4: A
   *
   * -> we can ignore B, bc it couldn't have gone active (alive_thru
   *    still 0).
   *
   * or,
   *
   *  1: A B
   *  2:   B   up_thru[B]=0
   *  3:   B   up_thru[B]=2
   *  4:
   *  5: A    
   *
   * -> we must wait for B, bc it was alive through 2, and could have
   *    written to the pg.
   *
   * If B is really dead, then an administrator will need to manually
   * intervene by marking the OSD as "lost."
   */

  // Include current acting and up nodes... not because they may
  // contain old data (this interval hasn't gone active, obviously),
  // but because we want their pg_info to inform choose_acting(), and
  // so that we know what they do/do not have explicitly before
  // sending them any new info/logs/whatever.
  for (unsigned i=0; i<acting.size(); i++)
    probe.insert(acting[i]);
  // It may be possible to exlude the up nodes, but let's keep them in
  // there for now.
  for (unsigned i=0; i<up.size(); i++)
    probe.insert(up[i]);

  for (map<epoch_t,pg_interval_t>::const_reverse_iterator p = past_intervals.rbegin();
       p != past_intervals.rend();
       ++p) {
    const pg_interval_t &interval = p->second;
    dout(10) << "build_prior " << interval << dendl;

    if (interval.last < info.history.last_epoch_started)
      break;  // we don't care

    if (interval.acting.empty())
      continue;

    if (!interval.maybe_went_rw)
      continue;

    // look at candidate osds during this interval.  each falls into
    // one of three categories: up, down (but potentially
    // interesting), or lost (down, but we won't wait for it).
    bool any_up_now = false;    // any candidates up now
    bool any_down_now = false;  // any candidates down now (that might have useful data)

    // consider ACTING osds
    for (unsigned i=0; i<interval.acting.size(); i++) {
      int o = interval.acting[i];

      const osd_info_t *pinfo = 0;
      if (osdmap.exists(o))
	pinfo = &osdmap.get_info(o);

      if (osdmap.is_up(o)) {
	// include past acting osds if they are up.
	probe.insert(o);
	any_up_now = true;
      } else if (!pinfo) {
	dout(10) << "build_prior  prior osd." << o << " no longer exists" << dendl;
	down.insert(o);
      } else if (pinfo->lost_at > interval.first) {
	dout(10) << "build_prior  prior osd." << o << " is down, but lost_at " << pinfo->lost_at << dendl;
	down.insert(o);
      } else {
	dout(10) << "build_prior  prior osd." << o << " is down" << dendl;
	down.insert(o);
	any_down_now = true;
      }
    }

    // if nobody survived this interval, and we may have gone rw,
    // then we need to wait for one of those osds to recover to
    // ensure that we haven't lost any information.
    if (!any_up_now && any_down_now) {
      // fixme: how do we identify a "clean" shutdown anyway?
      dout(10) << "build_prior  possibly went active+rw, none up; including down osds" << dendl;
      for (vector<int>::const_iterator i = interval.acting.begin();
	   i != interval.acting.end();
	   ++i) {
	if (osdmap.exists(*i) &&   // if it doesn't exist, we already consider it lost.
	    osdmap.is_down(*i)) {
	  probe.insert(*i);
	  pg_down = true;

	  // make note of when any down osd in the cur set was lost, so that
	  // we can notice changes in prior_set_affected.
	  blocked_by[*i] = osdmap.get_info(*i).lost_at;
	}
      }
    }
  }

#if 0 
  For some reason I cannot figure out cout << set<int> fails although include/types is included...

  dout(10) << "build_prior final: probe " << probe
	   << " down " << down
	   << " blocked_by " << blocked_by
	   << (pg_down ? " pg_down":"")
	   << dendl;
#endif
}

// true if the given map affects the prior set
bool PriorSet::affected_by_map(const OSDMapRef osdmap, const PGRecoveryStateInterface *debug_pg) const
{
  for (set<int>::iterator p = probe.begin();
       p != probe.end();
       ++p) {
    int o = *p;

    // did someone in the prior set go down?
    if (osdmap->is_down(o) && down.count(o) == 0) {
      dout(10) << "affected_by_map osd." << o << " now down" << dendl;
      return true;
    }

    // did a down osd in cur get (re)marked as lost?
    map<int,epoch_t>::const_iterator r = blocked_by.find(o);
    if (r != blocked_by.end()) {
      if (!osdmap->exists(o)) {
	dout(10) << "affected_by_map osd." << o << " no longer exists" << dendl;
	return true;
      }
      if (osdmap->get_info(o).lost_at != r->second) {
	dout(10) << "affected_by_map osd." << o << " (re)marked as lost" << dendl;
	return true;
      }
    }
  }

  // did someone in the prior down set go up?
  for (set<int>::const_iterator p = down.begin();
       p != down.end();
       ++p) {
    int o = *p;

    if (osdmap->is_up(o)) {
      dout(10) << "affected_by_map osd." << *p << " now up" << dendl;
      return true;
    }

    // did someone in the prior set get lost or destroyed?
    if (!osdmap->exists(o)) {
      dout(10) << "affected_by_map osd." << o << " no longer exists" << dendl;
      return true;
    }
  }

  return false;
}

std::ostream& operator<<(std::ostream& oss,
			 const struct PriorSet &prior)
{
  oss << "PriorSet[probe=" << prior.probe << " "
      << "down=" << prior.down << " "
      << "blocked_by=" << prior.blocked_by << "]";
  return oss;
}
