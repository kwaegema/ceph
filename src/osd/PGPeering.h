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

#ifndef CEPH_PGPEERING_H
#define CEPH_PGPEERING_H

#include "PGRecoveryState.h"

namespace PGRecoveryState {

  template <class EVT>
  struct QueuePeeringEvt : Context {
    PGRecoveryStateInterfaceRef pg;
    epoch_t epoch;
    EVT evt;
    QueuePeeringEvt(PGRecoveryStateInterface *pg, epoch_t epoch, EVT evt) :
      pg(pg), epoch(epoch), evt(evt) {}
    void finish(int r) {
      pg->lock();
      pg->queue_peering_event(PGRecoveryState::CephPeeringEvtRef(
						    new PGRecoveryState::CephPeeringEvt(
									   epoch,
									   epoch,
									   evt)));
      pg->unlock();
    }
  };

  class CephPeeringEvt {
    epoch_t epoch_sent;
    epoch_t epoch_requested;
    boost::intrusive_ptr< const boost::statechart::event_base > evt;
    string desc;
  public:
    template <class T>
    CephPeeringEvt(epoch_t epoch_sent,
		   epoch_t epoch_requested,
		   const T &evt_) :
      epoch_sent(epoch_sent), epoch_requested(epoch_requested),
      evt(evt_.intrusive_from_this()) {
      stringstream out;
      out << "epoch_sent: " << epoch_sent
	  << " epoch_requested: " << epoch_requested << " ";
      evt_.print(&out);
      desc = out.str();
    }
    epoch_t get_epoch_sent() { return epoch_sent; }
    epoch_t get_epoch_requested() { return epoch_requested; }
    const boost::statechart::event_base &get_event() { return *evt; }
    string get_desc() { return desc; }
  };

  struct GetInfo;

  struct PriorSet {
    set<int> probe; /// current+prior OSDs we need to probe.
    set<int> down;  /// down osds that would normally be in @a probe and might be interesting.
    map<int,epoch_t> blocked_by;  /// current lost_at values for any OSDs in cur set for which (re)marking them lost would affect cur set

    bool pg_down;   /// some down osds are included in @a cur; the DOWN pg state bit should be set.
    PriorSet(const OSDMap &osdmap,
	     const map<epoch_t, pg_interval_t> &past_intervals,
	     const vector<int> &up,
	     const vector<int> &acting,
	     const pg_info_t &info,
	     const PGRecoveryStateInterface *debug_pg=NULL);

    bool affected_by_map(const OSDMapRef osdmap, const PGRecoveryStateInterface *debug_pg=0) const;
  };

  std::ostream& operator<<(std::ostream& oss,
			   const struct PriorSet &prior);

  struct Peering : boost::statechart::state< Peering, RecoveryState::Primary, GetInfo >, NamedState {
    std::auto_ptr< PriorSet > prior_set;
    bool flushed;

    Peering(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::transition< Activate, RecoveryState::Active >,
      boost::statechart::custom_reaction< AdvMap >
      > reactions;
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const AdvMap &advmap);
  };


  struct GetLog;

  struct GetInfo : boost::statechart::state< GetInfo, Peering >, NamedState {
    set<int> peer_info_requested;

    GetInfo(my_context ctx);
    void exit();
    void get_infos();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::transition< GotInfo, GetLog >,
      boost::statechart::custom_reaction< MNotifyRec >
      > reactions;
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const MNotifyRec& infoevt);
  };

  struct Incomplete;

  struct GetLog : boost::statechart::state< GetLog, Peering >, NamedState {
    int newest_update_osd;
    boost::intrusive_ptr<MOSDPGLog> msg;

    GetLog(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::custom_reaction< MLogRec >,
      boost::statechart::custom_reaction< GotLog >,
      boost::statechart::custom_reaction< AdvMap >,
      boost::statechart::transition< RecoveryState::IsIncomplete, Incomplete >
      > reactions;
    boost::statechart::result react(const AdvMap&);
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const MLogRec& logevt);
    boost::statechart::result react(const GotLog&);
  };

  struct WaitUpThru;
  struct WaitFlushedPeering;

  struct GetMissing : boost::statechart::state< GetMissing, Peering >, NamedState {
    set<int> peer_missing_requested;

    GetMissing(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::custom_reaction< MLogRec >,
      boost::statechart::transition< NeedUpThru, WaitUpThru >,
      boost::statechart::transition< CheckRepops, WaitFlushedPeering>
      > reactions;
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const MLogRec& logevt);
  };

  struct WaitFlushedPeering :
    boost::statechart::state< WaitFlushedPeering, Peering>, NamedState {
    WaitFlushedPeering(my_context ctx);
    void exit() {}
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::custom_reaction< FlushedEvt >
      > reactions;
    boost::statechart::result react(const FlushedEvt& evt);
    boost::statechart::result react(const QueryState& q);
  };

  struct WaitUpThru : boost::statechart::state< WaitUpThru, Peering >, NamedState {
    WaitUpThru(my_context ctx);
    void exit();

    typedef boost::mpl::list <
      boost::statechart::custom_reaction< QueryState >,
      boost::statechart::custom_reaction< ActMap >,
      boost::statechart::transition< CheckRepops, WaitFlushedPeering>,
      boost::statechart::custom_reaction< MLogRec >
      > reactions;
    boost::statechart::result react(const QueryState& q);
    boost::statechart::result react(const ActMap& am);
    boost::statechart::result react(const MLogRec& logrec);
  };

  struct Incomplete : boost::statechart::state< Incomplete, Peering>, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< AdvMap >
      > reactions;
    Incomplete(my_context ctx);
    boost::statechart::result react(const AdvMap &advmap);
    void exit();
  };
}

#endif // CEPH_PGPEERING_H
