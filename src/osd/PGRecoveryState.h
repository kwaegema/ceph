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

#ifndef CEPH_PGRECOVERYSTATE_H
#define CEPH_PGRECOVERYSTATE_H

#include <boost/statechart/custom_reaction.hpp>
#include <boost/statechart/event.hpp>
#include <boost/statechart/simple_state.hpp>
#include <boost/statechart/state.hpp>
#include <boost/statechart/state_machine.hpp>
#include <boost/statechart/transition.hpp>
#include <boost/statechart/event_base.hpp>

#include "PGRecoveryStateInterface.h"

#include "osd_types.h"
#include "messages/MOSDPGLog.h"
#include "OSDMap.h"
#include "os/ObjectStore.h"

namespace PGRecoveryState {

  struct PGRecoveryStats {
    struct per_state_info {
      uint64_t enter, exit;     // enter/exit counts
      uint64_t events;
      utime_t event_time;       // time spent processing events
      utime_t total_time;       // total time in state
      utime_t min_time, max_time;

      per_state_info() : enter(0), exit(0), events(0) {}
    };
    map<const char *,per_state_info> info;
    Mutex lock;

    PGRecoveryStats() : lock("PGRecoverStats::lock") {}

    void reset() {
      Mutex::Locker l(lock);
      info.clear();
    }
    void dump(ostream& out) {
      Mutex::Locker l(lock);
      for (map<const char *,per_state_info>::iterator p = info.begin(); p != info.end(); ++p) {
	per_state_info& i = p->second;
	out << i.enter << "\t" << i.exit << "\t"
	    << i.events << "\t" << i.event_time << "\t"
	    << i.total_time << "\t"
	    << i.min_time << "\t" << i.max_time << "\t"
	    << p->first << "\n";
	       
      }
    }

    void log_enter(const char *s) {
      Mutex::Locker l(lock);
      info[s].enter++;
    }
    void log_exit(const char *s, utime_t dur, uint64_t events, utime_t event_dur) {
      Mutex::Locker l(lock);
      per_state_info &i = info[s];
      i.exit++;
      i.total_time += dur;
      if (dur > i.max_time)
	i.max_time = dur;
      if (dur < i.min_time || i.min_time == utime_t())
	i.min_time = dur;
      i.events += events;
      i.event_time += event_dur;
    }
  };

  struct RecoveryCtx {
    utime_t start_time;
    map< int, map<pg_t, pg_query_t> > *query_map;
    map< int, vector<pair<pg_notify_t, pg_interval_map_t> > > *info_map;
    map< int, vector<pair<pg_notify_t, pg_interval_map_t> > > *notify_list;
    C_Contexts *on_applied;
    C_Contexts *on_safe;
    ObjectStore::Transaction *transaction;
    RecoveryCtx(map< int, map<pg_t, pg_query_t> > *query_map,
		map< int, vector<pair<pg_notify_t, pg_interval_map_t> > > *info_map,
		map< int, vector<pair<pg_notify_t, pg_interval_map_t> > > *notify_list,
		C_Contexts *on_applied,
		C_Contexts *on_safe,
		ObjectStore::Transaction *transaction)
      : query_map(query_map), info_map(info_map), 
	notify_list(notify_list),
	on_applied(on_applied),
	on_safe(on_safe),
	transaction(transaction) {}
  };

  struct NamedState {
    const char *state_name;
    utime_t enter_time;
    const char *get_state_name() { return state_name; }
    NamedState() : state_name(0), enter_time(ceph_clock_now(g_ceph_context)) {}
    virtual ~NamedState() {}
  };

  struct QueryState : boost::statechart::event< QueryState > {
    Formatter *f;
    QueryState(Formatter *f) : f(f) {}
    void print(std::ostream *out) const {
      *out << "Query";
    }
  };

  struct MInfoRec : boost::statechart::event< MInfoRec > {
    int from;
    pg_info_t info;
    epoch_t msg_epoch;
    MInfoRec(int from, pg_info_t &info, epoch_t msg_epoch) :
      from(from), info(info), msg_epoch(msg_epoch) {}
    void print(std::ostream *out) const {
      *out << "MInfoRec from " << from << " info: " << info;
    }
  };

  struct MLogRec : boost::statechart::event< MLogRec > {
    int from;
    boost::intrusive_ptr<MOSDPGLog> msg;
    MLogRec(int from, MOSDPGLog *msg) :
      from(from), msg(msg) {}
    void print(std::ostream *out) const {
      *out << "MLogRec from " << from;
    }
  };

  struct MNotifyRec : boost::statechart::event< MNotifyRec > {
    int from;
    pg_notify_t notify;
    MNotifyRec(int from, pg_notify_t &notify) :
      from(from), notify(notify) {}
    void print(std::ostream *out) const {
      *out << "MNotifyRec from " << from << " notify: " << notify;
    }
  };

  struct MQuery : boost::statechart::event< MQuery > {
    int from;
    pg_query_t query;
    epoch_t query_epoch;
    MQuery(int from, const pg_query_t &query, epoch_t query_epoch):
      from(from), query(query), query_epoch(query_epoch) {}
    void print(std::ostream *out) const {
      *out << "MQuery from " << from
	   << " query_epoch " << query_epoch
	   << " query: " << query;
    }
  };

  struct AdvMap : boost::statechart::event< AdvMap > {
    OSDMapRef osdmap;
    OSDMapRef lastmap;
    vector<int> newup, newacting;
    AdvMap(OSDMapRef osdmap, OSDMapRef lastmap, vector<int>& newup, vector<int>& newacting):
      osdmap(osdmap), lastmap(lastmap), newup(newup), newacting(newacting) {}
    void print(std::ostream *out) const {
      *out << "AdvMap";
    }
  };

  struct ActMap : boost::statechart::event< ActMap > {
    ActMap() : boost::statechart::event< ActMap >() {}
    void print(std::ostream *out) const {
      *out << "ActMap";
    }
  };
  struct Activate : boost::statechart::event< Activate > {
    epoch_t query_epoch;
    Activate(epoch_t q) : boost::statechart::event< Activate >(),
			  query_epoch(q) {}
    void print(std::ostream *out) const {
      *out << "Activate from " << query_epoch;
    }
  };
  struct RequestBackfillPrio : boost::statechart::event< RequestBackfillPrio > {
    unsigned priority;
    RequestBackfillPrio(unsigned prio) :
              boost::statechart::event< RequestBackfillPrio >(),
			  priority(prio) {}
    void print(std::ostream *out) const {
      *out << "RequestBackfillPrio: priority " << priority;
    }
  };

    struct GetMissing;
    struct GotLog : boost::statechart::event< GotLog > {
      GotLog() : boost::statechart::event< GotLog >() {}
    };


#define TrivialEvent(T) struct T : boost::statechart::event< T > { \
    T() : boost::statechart::event< T >() {}			   \
    void print(std::ostream *out) const {			   \
      *out << #T;						   \
    }								   \
  };
  TrivialEvent(Initialize)
  TrivialEvent(Load)
  TrivialEvent(GotInfo)
  TrivialEvent(NeedUpThru)
  TrivialEvent(CheckRepops)
  TrivialEvent(NullEvt)
  TrivialEvent(FlushedEvt)
  TrivialEvent(Backfilled)
  TrivialEvent(LocalBackfillReserved)
  TrivialEvent(RemoteBackfillReserved)
  TrivialEvent(RemoteReservationRejected)
  TrivialEvent(RequestBackfill)
  TrivialEvent(RequestRecovery)
  TrivialEvent(RecoveryDone)
  TrivialEvent(BackfillTooFull)

  TrivialEvent(AllReplicasRecovered)
  TrivialEvent(DoRecovery)
  TrivialEvent(LocalRecoveryReserved)
  TrivialEvent(RemoteRecoveryReserved)
  TrivialEvent(AllRemotesReserved)
  TrivialEvent(Recovering)
  TrivialEvent(WaitRemoteBackfillReserved)
  TrivialEvent(GoClean)

  TrivialEvent(AllReplicasActivated)

  struct Peering;

  /* Encapsulates PG recovery process */
  struct RecoveryState {
    void start_handle(RecoveryCtx *new_ctx) {
      assert(!rctx);
      rctx = new_ctx;
      if (rctx)
	rctx->start_time = ceph_clock_now(g_ceph_context);
    }

    void end_handle() {
      if (rctx) {
	utime_t dur = ceph_clock_now(g_ceph_context) - rctx->start_time;
	machine.event_time += dur;
      }
      machine.event_count++;
      rctx = 0;
    }

    /* States */
    struct Initial;
    class RecoveryMachine : public boost::statechart::state_machine< RecoveryMachine, Initial > {
      RecoveryState *state;
    public:
      PGRecoveryStateInterface *pg;

      utime_t event_time;
      uint64_t event_count;
      
      void clear_event_counters() {
	event_time = utime_t();
	event_count = 0;
      }

      void log_enter(const char *state_name);
      void log_exit(const char *state_name, utime_t duration);

      RecoveryMachine(RecoveryState *state, PGRecoveryStateInterface *pg) : state(state), pg(pg), event_count(0) {}

      /* Accessor functions for state methods */
      ObjectStore::Transaction* get_cur_transaction() {
	assert(state->rctx->transaction);
	return state->rctx->transaction;
      }

      void send_query(int to, const pg_query_t &query) {
	assert(state->rctx->query_map);
	(*state->rctx->query_map)[to][pg->get_info().pgid] = query;
      }

      map<int, map<pg_t, pg_query_t> > *get_query_map() {
	assert(state->rctx->query_map);
	return state->rctx->query_map;
      }

      map<int, vector<pair<pg_notify_t, pg_interval_map_t> > > *get_info_map() {
	assert(state->rctx->info_map);
	return state->rctx->info_map;
      }

      list< Context* > *get_on_safe_context_list() {
	assert(state->rctx->on_safe);
	return &(state->rctx->on_safe->contexts);
      }

      list< Context * > *get_on_applied_context_list() {
	assert(state->rctx->on_applied);
	return &(state->rctx->on_applied->contexts);
      }

      void send_notify(int to, const pg_notify_t &info, const pg_interval_map_t &pi) {
	assert(state->rctx->notify_list);
	(*state->rctx->notify_list)[to].push_back(make_pair(info, pi));
      }
    };
    friend class RecoveryMachine;

    /* States */

    struct Crashed : boost::statechart::state< Crashed, RecoveryMachine >, NamedState {
      Crashed(my_context ctx);
    };

    struct Started;
    struct Reset;

    struct Initial : boost::statechart::state< Initial, RecoveryMachine >, NamedState {
      Initial(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::transition< Initialize, Reset >,
	boost::statechart::custom_reaction< Load >,
	boost::statechart::custom_reaction< NullEvt >,
	boost::statechart::transition< boost::statechart::event_base, Crashed >
	> reactions;

      boost::statechart::result react(const Load&);
      boost::statechart::result react(const MNotifyRec&);
      boost::statechart::result react(const MInfoRec&);
      boost::statechart::result react(const MLogRec&);
      boost::statechart::result react(const boost::statechart::event_base&) {
	return discard_event();
      }
    };

    struct Reset : boost::statechart::state< Reset, RecoveryMachine >, NamedState {
      Reset(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< NullEvt >,
	boost::statechart::custom_reaction< FlushedEvt >,
	boost::statechart::transition< boost::statechart::event_base, Crashed >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const FlushedEvt&);
      boost::statechart::result react(const boost::statechart::event_base&) {
	return discard_event();
      }
    };

    struct Start;

    struct Started : boost::statechart::state< Started, RecoveryMachine, Start >, NamedState {
      Started(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< NullEvt >,
	boost::statechart::custom_reaction< FlushedEvt >,
	boost::statechart::transition< boost::statechart::event_base, Crashed >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const FlushedEvt&);
      boost::statechart::result react(const boost::statechart::event_base&) {
	return discard_event();
      }
    };

    struct MakePrimary : boost::statechart::event< MakePrimary > {
      MakePrimary() : boost::statechart::event< MakePrimary >() {}
    };
    struct MakeStray : boost::statechart::event< MakeStray > {
      MakeStray() : boost::statechart::event< MakeStray >() {}
    };
    struct Primary;
    struct Stray;

    struct Start : boost::statechart::state< Start, Started >, NamedState {
      Start(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::transition< MakePrimary, Primary >,
	boost::statechart::transition< MakeStray, Stray >
	> reactions;
    };

    struct WaitActingChange;
    struct NeedActingChange : boost::statechart::event< NeedActingChange > {
      NeedActingChange() : boost::statechart::event< NeedActingChange >() {}
    };
    struct Incomplete;
    struct IsIncomplete : boost::statechart::event< IsIncomplete > {
      IsIncomplete() : boost::statechart::event< IsIncomplete >() {}
    };

    struct Primary : boost::statechart::state< Primary, Started, Peering >, NamedState {
      Primary(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< MNotifyRec >,
	boost::statechart::transition< NeedActingChange, WaitActingChange >,
	boost::statechart::custom_reaction< AdvMap>
	> reactions;
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const MNotifyRec&);
    };

    struct WaitActingChange : boost::statechart::state< WaitActingChange, Primary>,
			      NamedState {
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::custom_reaction< MInfoRec >,
	boost::statechart::custom_reaction< MNotifyRec >
	> reactions;
      WaitActingChange(my_context ctx);
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const MLogRec&);
      boost::statechart::result react(const MInfoRec&);
      boost::statechart::result react(const MNotifyRec&);
      void exit();
    };

    struct WaitLocalRecoveryReserved;
    struct Activating;
    struct Active : boost::statechart::state< Active, Primary, Activating >, NamedState {
      Active(my_context ctx);
      void exit();

      const set<int> sorted_acting_set;
      bool all_replicas_activated;

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< MInfoRec >,
	boost::statechart::custom_reaction< MNotifyRec >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::custom_reaction< Backfilled >,
	boost::statechart::custom_reaction< AllReplicasActivated >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const MInfoRec& infoevt);
      boost::statechart::result react(const MNotifyRec& notevt);
      boost::statechart::result react(const MLogRec& logevt);
      boost::statechart::result react(const Backfilled&) {
	return discard_event();
      }
      boost::statechart::result react(const AllReplicasActivated&);
    };

    struct Clean : boost::statechart::state< Clean, Active >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< DoRecovery, WaitLocalRecoveryReserved >
      > reactions;
      Clean(my_context ctx);
      void exit();
    };

    struct Recovered : boost::statechart::state< Recovered, Active >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< GoClean, Clean >,
	boost::statechart::custom_reaction< AllReplicasActivated >
      > reactions;
      Recovered(my_context ctx);
      void exit();
      boost::statechart::result react(const AllReplicasActivated&) {
	post_event(GoClean());
	return forward_event();
      }
    };

    struct Backfilling : boost::statechart::state< Backfilling, Active >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< Backfilled, Recovered >,
	boost::statechart::custom_reaction< RemoteReservationRejected >
	> reactions;
      Backfilling(my_context ctx);
      boost::statechart::result react(const RemoteReservationRejected& evt);
      void exit();
    };

    struct WaitRemoteBackfillReserved : boost::statechart::state< WaitRemoteBackfillReserved, Active >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::custom_reaction< RemoteBackfillReserved >,
	boost::statechart::custom_reaction< RemoteReservationRejected >
	> reactions;
      WaitRemoteBackfillReserved(my_context ctx);
      void exit();
      boost::statechart::result react(const RemoteBackfillReserved& evt);
      boost::statechart::result react(const RemoteReservationRejected& evt);
    };

    struct WaitLocalBackfillReserved : boost::statechart::state< WaitLocalBackfillReserved, Active >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< LocalBackfillReserved, WaitRemoteBackfillReserved >
	> reactions;
      WaitLocalBackfillReserved(my_context ctx);
      void exit();
    };

    struct NotBackfilling : boost::statechart::state< NotBackfilling, Active>, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< RequestBackfill, WaitLocalBackfillReserved>
	> reactions;
      NotBackfilling(my_context ctx);
      void exit();
    };

    struct RepNotRecovering;
    struct ReplicaActive : boost::statechart::state< ReplicaActive, Started, RepNotRecovering >, NamedState {
      ReplicaActive(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< MQuery >,
	boost::statechart::custom_reaction< MInfoRec >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::custom_reaction< Activate >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const MInfoRec& infoevt);
      boost::statechart::result react(const MLogRec& logevt);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const MQuery&);
      boost::statechart::result react(const Activate&);
    };

    struct RepRecovering : boost::statechart::state< RepRecovering, ReplicaActive >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< RecoveryDone, RepNotRecovering >,
	boost::statechart::custom_reaction< BackfillTooFull >
	> reactions;
      RepRecovering(my_context ctx);
      boost::statechart::result react(const BackfillTooFull &evt);
      void exit();
    };

    struct RepWaitBackfillReserved : boost::statechart::state< RepWaitBackfillReserved, ReplicaActive >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::custom_reaction< RemoteBackfillReserved >,
	boost::statechart::custom_reaction< RemoteReservationRejected >
	> reactions;
      RepWaitBackfillReserved(my_context ctx);
      void exit();
      boost::statechart::result react(const RemoteBackfillReserved &evt);
      boost::statechart::result react(const RemoteReservationRejected &evt);
    };

    struct RepWaitRecoveryReserved : boost::statechart::state< RepWaitRecoveryReserved, ReplicaActive >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::custom_reaction< RemoteRecoveryReserved >
	> reactions;
      RepWaitRecoveryReserved(my_context ctx);
      void exit();
      boost::statechart::result react(const RemoteRecoveryReserved &evt);
    };

    struct RepNotRecovering : boost::statechart::state< RepNotRecovering, ReplicaActive>, NamedState {
      typedef boost::mpl::list<
	boost::statechart::custom_reaction< RequestBackfillPrio >,
        boost::statechart::transition< RequestRecovery, RepWaitRecoveryReserved >,
	boost::statechart::transition< RecoveryDone, RepNotRecovering >  // for compat with pre-reservation peers
	> reactions;
      RepNotRecovering(my_context ctx);
      boost::statechart::result react(const RequestBackfillPrio &evt);
      void exit();
    };

    struct Recovering : boost::statechart::state< Recovering, Active >, NamedState {
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< AllReplicasRecovered >,
	boost::statechart::custom_reaction< RequestBackfill >
	> reactions;
      Recovering(my_context ctx);
      void exit();
      void release_reservations();
      boost::statechart::result react(const AllReplicasRecovered &evt);
      boost::statechart::result react(const RequestBackfill &evt);
    };

    struct WaitRemoteRecoveryReserved : boost::statechart::state< WaitRemoteRecoveryReserved, Active >, NamedState {
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< RemoteRecoveryReserved >,
	boost::statechart::transition< AllRemotesReserved, Recovering >
	> reactions;
      set<int>::const_iterator acting_osd_it;
      WaitRemoteRecoveryReserved(my_context ctx);
      boost::statechart::result react(const RemoteRecoveryReserved &evt);
      void exit();
    };

    struct WaitLocalRecoveryReserved : boost::statechart::state< WaitLocalRecoveryReserved, Active >, NamedState {
      typedef boost::mpl::list <
	boost::statechart::transition< LocalRecoveryReserved, WaitRemoteRecoveryReserved >
	> reactions;
      WaitLocalRecoveryReserved(my_context ctx);
      void exit();
    };

    struct Activating : boost::statechart::state< Activating, Active >, NamedState {
      typedef boost::mpl::list <
	boost::statechart::transition< AllReplicasRecovered, Recovered >,
	boost::statechart::transition< DoRecovery, WaitLocalRecoveryReserved >,
	boost::statechart::transition< RequestBackfill, WaitLocalBackfillReserved >
	> reactions;
      Activating(my_context ctx);
      void exit();
    };

    struct Stray : boost::statechart::state< Stray, Started >, NamedState {
      map<int, pair<pg_query_t, epoch_t> > pending_queries;

      Stray(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< MQuery >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::custom_reaction< MInfoRec >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< RecoveryDone >
	> reactions;
      boost::statechart::result react(const MQuery& query);
      boost::statechart::result react(const MLogRec& logevt);
      boost::statechart::result react(const MInfoRec& infoevt);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const RecoveryDone&) {
	return discard_event();
      }
    };

    RecoveryMachine machine;
    PGRecoveryStateInterface *pg;
    RecoveryCtx *rctx;

  public:
    RecoveryState(PGRecoveryStateInterface *pg) : machine(this, pg), pg(pg), rctx(0) {
      machine.initiate();
    }

    void handle_event(const boost::statechart::event_base &evt,
		      RecoveryCtx *rctx) {
      start_handle(rctx);
      machine.process_event(evt);
      end_handle();
    }

    void handle_event(CephPeeringEvtRef evt,
		      RecoveryCtx *rctx);
  };
}

#endif // CEPH_PGRECOVERYSTATE_H
