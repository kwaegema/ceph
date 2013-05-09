// the content of this file must be moved where it makes sense. It is
// gathered here to introduce IPG with minimal changes

#include "IPG.h"
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
#include "messages/MRecoveryReserve.h"

#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"
#include "common/BackTrace.h"

#include <sstream>

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

epoch_t IPG::peek_map_epoch(ObjectStore *store, coll_t coll, hobject_t &infos_oid, bufferlist *bl)
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

/*------------ Recovery State Machine----------------*/
#undef dout_prefix
#define dout_prefix (*_dout << context< RecoveryMachine >().pg->gen_prefix() \
		     << "state<" << get_state_name() << ">: ")

/*------Crashed-------*/
IPG::RecoveryState::Crashed::Crashed(my_context ctx)
  : my_base(ctx)
{
  state_name = "Crashed";
  context< RecoveryMachine >().log_enter(state_name);
  assert(0 == "we got a bad state machine event");
}


/*------Initial-------*/
IPG::RecoveryState::Initial::Initial(my_context ctx)
  : my_base(ctx)
{
  state_name = "Initial";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result IPG::RecoveryState::Initial::react(const Load& l)
{
  IPG *pg = context< RecoveryMachine >().pg;

  // do we tell someone we're here?
  pg->set_send_notify((pg->get_role() != 0));

  return transit< Reset >();
}

boost::statechart::result IPG::RecoveryState::Initial::react(const MNotifyRec& notify)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->proc_replica_info(notify.from, notify.notify.info);
  pg->update_heartbeat_peers();
  pg->set_last_peering_reset();
  return transit< Primary >();
}

boost::statechart::result IPG::RecoveryState::Initial::react(const MInfoRec& i)
{
  IPG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_primary());
  post_event(i);
  return transit< Stray >();
}

boost::statechart::result IPG::RecoveryState::Initial::react(const MLogRec& i)
{
  IPG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_primary());
  post_event(i);
  return transit< Stray >();
}

void IPG::RecoveryState::Initial::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------Started-------*/
IPG::RecoveryState::Started::Started(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started";
  context< RecoveryMachine >().log_enter(state_name);
  IPG *pg = context< RecoveryMachine >().pg;
  pg->start_flush(
    context< RecoveryMachine >().get_cur_transaction(),
    context< RecoveryMachine >().get_on_applied_context_list(),
    context< RecoveryMachine >().get_on_safe_context_list());
}

boost::statechart::result
IPG::RecoveryState::Started::react(const FlushedEvt&)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->set_flushed(true);
  pg->requeue_ops(pg->get_waiting_for_active());
  return discard_event();
}


boost::statechart::result IPG::RecoveryState::Started::react(const AdvMap& advmap)
{
  dout(10) << "Started advmap" << dendl;
  IPG *pg = context< RecoveryMachine >().pg;
  if (pg->acting_up_affected(advmap.newup, advmap.newacting) ||
    pg->is_split(advmap.lastmap, advmap.osdmap)) {
    dout(10) << "up or acting affected, transitioning to Reset" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }
  pg->remove_down_peer_info(advmap.osdmap);
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::Started::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

void IPG::RecoveryState::Started::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*--------Reset---------*/
IPG::RecoveryState::Reset::Reset(my_context ctx)
  : my_base(ctx)
{
  state_name = "Reset";
  context< RecoveryMachine >().log_enter(state_name);
  IPG *pg = context< RecoveryMachine >().pg;
  pg->set_last_peering_reset();
}

boost::statechart::result
IPG::RecoveryState::Reset::react(const FlushedEvt&)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->flushed = true;
  pg->on_flushed();
  pg->requeue_ops(pg->get_waiting_for_active());
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::Reset::react(const AdvMap& advmap)
{
  IPG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Reset advmap" << dendl;

  // make sure we have past_intervals filled in.  hopefully this will happen
  // _before_ we are active.
  pg->generate_past_intervals();

  pg->remove_down_peer_info(advmap.osdmap);
  if (pg->acting_up_affected(advmap.newup, advmap.newacting) ||
    pg->is_split(advmap.lastmap, advmap.osdmap)) {
    dout(10) << "up or acting affected, calling start_peering_interval again"
	     << dendl;
    pg->start_peering_interval(advmap.lastmap, advmap.newup, advmap.newacting);
  }
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::Reset::react(const ActMap&)
{
  IPG *pg = context< RecoveryMachine >().pg;
  if (pg->should_send_notify() && pg->get_primary() >= 0) {
    context< RecoveryMachine >().send_notify(pg->get_primary(),
					     pg_notify_t(pg->get_osdmap()->get_epoch(),
							 pg->get_osdmap()->get_epoch(),
							 pg->get_info()),
					     pg->get_past_intervals());
  }

  pg->update_heartbeat_peers();
  pg->take_waiters();

  return transit< Started >();
}

boost::statechart::result IPG::RecoveryState::Reset::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

void IPG::RecoveryState::Reset::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*-------Start---------*/
IPG::RecoveryState::Start::Start(my_context ctx)
  : my_base(ctx)
{
  state_name = "Start";
  context< RecoveryMachine >().log_enter(state_name);

  IPG *pg = context< RecoveryMachine >().pg;
  if (pg->is_primary()) {
    dout(1) << "transitioning to Primary" << dendl;
    post_event(MakePrimary());
  } else { //is_stray
    dout(1) << "transitioning to Stray" << dendl; 
    post_event(MakeStray());
  }
}

void IPG::RecoveryState::Start::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---------Primary--------*/
IPG::RecoveryState::Primary::Primary(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary";
  context< RecoveryMachine >().log_enter(state_name);
  IPG *pg = context< RecoveryMachine >().pg;
  assert(pg->get_want_acting().empty());
}

boost::statechart::result IPG::RecoveryState::Primary::react(const AdvMap &advmap)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->remove_down_peer_info(advmap.osdmap);
  return forward_event();
}

boost::statechart::result IPG::RecoveryState::Primary::react(const MNotifyRec& notevt)
{
  dout(7) << "handle_pg_notify from osd." << notevt.from << dendl;
  IPG *pg = context< RecoveryMachine >().pg;
  if (pg->get_peer_info().count(notevt.from) &&
      pg->get_peer_info()[notevt.from].last_update == notevt.notify.info.last_update) {
    dout(10) << *pg << " got dup osd." << notevt.from << " info " << notevt.notify.info
	     << ", identical to ours" << dendl;
  } else {
    pg->proc_replica_info(notevt.from, notevt.notify.info);
  }
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::Primary::react(const ActMap&)
{
  dout(7) << "handle ActMap primary" << dendl;
  IPG *pg = context< RecoveryMachine >().pg;
  pg->publish_stats_to_osd();
  pg->take_waiters();
  return discard_event();
}

void IPG::RecoveryState::Primary::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  IPG *pg = context< RecoveryMachine >().pg;
  pg->get_want_acting().clear();
}

/*---------Peering--------*/
IPG::RecoveryState::Peering::Peering(my_context ctx)
  : my_base(ctx), flushed(false)
{
  state_name = "Started/Primary/Peering";
  context< RecoveryMachine >().log_enter(state_name);

  IPG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_active());
  assert(!pg->is_peering());
  assert(pg->is_primary());
  pg->state_set(PG_STATE_PEERING);
}

boost::statechart::result IPG::RecoveryState::Peering::react(const AdvMap& advmap) 
{
  IPG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Peering advmap" << dendl;
  if (prior_set.get()->affected_by_map(advmap.osdmap, pg)) {
    dout(1) << "Peering, affected_by_map, going to Reset" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }
  
  pg->adjust_need_up_thru(advmap.osdmap);
  
  return forward_event();
}

boost::statechart::result IPG::RecoveryState::Peering::react(const QueryState& q)
{
  IPG *pg = context< RecoveryMachine >().pg;

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

void IPG::RecoveryState::Peering::exit()
{
  dout(10) << "Leaving Peering" << dendl;
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  IPG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_PEERING);
  pg->clear_probe_targets();
}


/*------Backfilling-------*/
IPG::RecoveryState::Backfilling::Backfilling(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/Backfilling";
  context< RecoveryMachine >().log_enter(state_name);
  IPG *pg = context< RecoveryMachine >().pg;
  pg->set_backfill_reserved(true);
  pg->get_osd()->queue_for_recovery(pg);
  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_set(PG_STATE_BACKFILL);
}

boost::statechart::result
IPG::RecoveryState::Backfilling::react(const RemoteReservationRejected &)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->get_osd()->local_reserver.cancel_reservation(pg->get_info().pgid);
  pg->state_set(PG_STATE_BACKFILL_TOOFULL);

  pg->get_osd()->recovery_wq.dequeue(pg);

  pg->schedule_backfill_full_retry();
  return transit<NotBackfilling>();
}

void IPG::RecoveryState::Backfilling::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  IPG *pg = context< RecoveryMachine >().pg;
  pg->set_backfill_reserved(false);
  pg->set_backfill_reserving(false);
  pg->state_clear(PG_STATE_BACKFILL);
}

/*--WaitRemoteBackfillReserved--*/

IPG::RecoveryState::WaitRemoteBackfillReserved::WaitRemoteBackfillReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/WaitRemoteBackfillReserved";
  context< RecoveryMachine >().log_enter(state_name);
  IPG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  ConnectionRef con = pg->get_osd()->get_con_osd_cluster(
    pg->get_backfill_target(), pg->get_osdmap()->get_epoch());
  if (con) {
    if ((con->features & CEPH_FEATURE_BACKFILL_RESERVATION)) {
      pg->get_osd()->send_message_osd_cluster(
        new MBackfillReserve(
	  MBackfillReserve::REQUEST,
	  pg->get_info().pgid,
	  pg->get_osdmap()->get_epoch()),
	con.get());
    } else {
      post_event(RemoteBackfillReserved());
    }
  }
}

void IPG::RecoveryState::WaitRemoteBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

boost::statechart::result
IPG::RecoveryState::WaitRemoteBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_BACKFILL_TOOFULL);
  return transit<Backfilling>();
}

boost::statechart::result
IPG::RecoveryState::WaitRemoteBackfillReserved::react(const RemoteReservationRejected &evt)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->get_osd()->local_reserver.cancel_reservation(pg->get_info().pgid);
  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_set(PG_STATE_BACKFILL_TOOFULL);

  pg->schedule_backfill_full_retry();

  return transit<NotBackfilling>();
}

/*--WaitLocalBackfillReserved--*/
IPG::RecoveryState::WaitLocalBackfillReserved::WaitLocalBackfillReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/WaitLocalBackfillReserved";
  context< RecoveryMachine >().log_enter(state_name);
  IPG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  pg->get_osd()->local_reserver.request_reservation(
    pg->get_info().pgid,
    new QueuePeeringEvt<LocalBackfillReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      LocalBackfillReserved()));
}

void IPG::RecoveryState::WaitLocalBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*----NotBackfilling------*/
IPG::RecoveryState::NotBackfilling::NotBackfilling(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/NotBackfilling";
  context< RecoveryMachine >().log_enter(state_name);
}

void IPG::RecoveryState::NotBackfilling::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---RepNotRecovering----*/
IPG::RecoveryState::RepNotRecovering::RepNotRecovering(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive/RepNotRecovering";
  context< RecoveryMachine >().log_enter(state_name);
}

void IPG::RecoveryState::RepNotRecovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---RepWaitRecoveryReserved--*/
IPG::RecoveryState::RepWaitRecoveryReserved::RepWaitRecoveryReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive/RepWaitRecoveryReserved";
  context< RecoveryMachine >().log_enter(state_name);
  IPG *pg = context< RecoveryMachine >().pg;

  pg->get_osd()->remote_reserver.request_reservation(
    pg->get_info().pgid,
    new QueuePeeringEvt<RemoteRecoveryReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      RemoteRecoveryReserved()));
}

boost::statechart::result
IPG::RecoveryState::RepWaitRecoveryReserved::react(const RemoteRecoveryReserved &evt)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->get_osd()->send_message_osd_cluster(
    pg->get_acting()[0],
    new MRecoveryReserve(
      MRecoveryReserve::GRANT,
      pg->get_info().pgid,
      pg->get_osdmap()->get_epoch()),
    pg->get_osdmap()->get_epoch());
  return transit<RepRecovering>();
}

void IPG::RecoveryState::RepWaitRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*-RepWaitBackfillReserved*/
IPG::RecoveryState::RepWaitBackfillReserved::RepWaitBackfillReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive/RepWaitBackfillReserved";
  context< RecoveryMachine >().log_enter(state_name);
  IPG *pg = context< RecoveryMachine >().pg;

  double ratio, max_ratio;
  if (pg->get_osd()->too_full_for_backfill(&ratio, &max_ratio) &&
      !g_conf->osd_debug_skip_full_check_in_backfill_reservation) {
    dout(10) << "backfill reservation rejected: full ratio is "
	     << ratio << ", which is greater than max allowed ratio "
	     << max_ratio << dendl;
    post_event(RemoteReservationRejected());
  } else {
    pg->get_osd()->remote_reserver.request_reservation(
      pg->get_info().pgid,
      new QueuePeeringEvt<RemoteBackfillReserved>(
        pg, pg->get_osdmap()->get_epoch(),
        RemoteBackfillReserved()));
  }
}

void IPG::RecoveryState::RepWaitBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

boost::statechart::result
IPG::RecoveryState::RepWaitBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->get_osd()->send_message_osd_cluster(
    pg->get_acting()[0],
    new MBackfillReserve(
      MBackfillReserve::GRANT,
      pg->get_info().pgid,
      pg->get_osdmap()->get_epoch()),
    pg->get_osdmap()->get_epoch());
  return transit<RepRecovering>();
}

boost::statechart::result
IPG::RecoveryState::RepWaitBackfillReserved::react(const RemoteReservationRejected &evt)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->reject_reservation();
  return transit<RepNotRecovering>();
}

/*---RepRecovering-------*/
IPG::RecoveryState::RepRecovering::RepRecovering(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive/RepRecovering";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result
IPG::RecoveryState::RepRecovering::react(const BackfillTooFull &)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->reject_reservation();
  return transit<RepNotRecovering>();
}

void IPG::RecoveryState::RepRecovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  IPG *pg = context< RecoveryMachine >().pg;
  pg->get_osd()->remote_reserver.cancel_reservation(pg->get_info().pgid);
}

/*------Activating--------*/
IPG::RecoveryState::Activating::Activating(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/Activating";
  context< RecoveryMachine >().log_enter(state_name);
}

void IPG::RecoveryState::Activating::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

IPG::RecoveryState::WaitLocalRecoveryReserved::WaitLocalRecoveryReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/WaitLocalRecoveryReserved";
  context< RecoveryMachine >().log_enter(state_name);
  IPG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_RECOVERY_WAIT);
  pg->get_osd()->local_reserver.request_reservation(
    pg->get_info().pgid,
    new QueuePeeringEvt<LocalRecoveryReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      LocalRecoveryReserved()));
}

void IPG::RecoveryState::WaitLocalRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

IPG::RecoveryState::WaitRemoteRecoveryReserved::WaitRemoteRecoveryReserved(my_context ctx)
  : my_base(ctx),
    acting_osd_it(context< Active >().sorted_acting_set.begin())
{
  state_name = "Started/Primary/Active/WaitRemoteRecoveryReserved";
  context< RecoveryMachine >().log_enter(state_name);
  post_event(RemoteRecoveryReserved());
}

boost::statechart::result
IPG::RecoveryState::WaitRemoteRecoveryReserved::react(const RemoteRecoveryReserved &evt) {
  IPG *pg = context< RecoveryMachine >().pg;

  if (acting_osd_it != context< Active >().sorted_acting_set.end()) {
    // skip myself
    if (*acting_osd_it == pg->get_osd()->whoami)
      ++acting_osd_it;
  }

  if (acting_osd_it != context< Active >().sorted_acting_set.end()) {
    ConnectionRef con = pg->get_osd()->get_con_osd_cluster(*acting_osd_it, pg->get_osdmap()->get_epoch());
    if (con) {
      if ((con->features & CEPH_FEATURE_RECOVERY_RESERVATION)) {
	pg->get_osd()->send_message_osd_cluster(
          new MRecoveryReserve(MRecoveryReserve::REQUEST,
			       pg->get_info().pgid,
			       pg->get_osdmap()->get_epoch()),
	  con.get());
      } else {
	post_event(RemoteRecoveryReserved());
      }
    }
    ++acting_osd_it;
  } else {
    post_event(AllRemotesReserved());
  }
  return discard_event();
}

void IPG::RecoveryState::WaitRemoteRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

IPG::RecoveryState::Recovering::Recovering(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/Recovering";
  context< RecoveryMachine >().log_enter(state_name);

  IPG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERY_WAIT);
  pg->state_set(PG_STATE_RECOVERING);
  pg->get_osd()->queue_for_recovery(pg);
}

void IPG::RecoveryState::Recovering::release_reservations()
{
  IPG *pg = context< RecoveryMachine >().pg;
  assert(!pg->get_missing().have_missing());
  pg->state_clear(PG_STATE_RECOVERING);

  // release remote reservations
  for (set<int>::const_iterator i = context< Active >().sorted_acting_set.begin();
        i != context< Active >().sorted_acting_set.end();
        ++i) {
    if (*i == pg->get_osd()->whoami) // skip myself
      continue;
    ConnectionRef con = pg->get_osd()->get_con_osd_cluster(*i, pg->get_osdmap()->get_epoch());
    if (con) {
      if ((con->features & CEPH_FEATURE_RECOVERY_RESERVATION)) {
	pg->get_osd()->send_message_osd_cluster(
          new MRecoveryReserve(MRecoveryReserve::RELEASE,
			       pg->get_info().pgid,
			       pg->get_osdmap()->get_epoch()),
	  con.get());
      }
    }
  }
}

boost::statechart::result
IPG::RecoveryState::Recovering::react(const AllReplicasRecovered &evt)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERING);
  release_reservations();
  return transit<Recovered>();
}

boost::statechart::result
IPG::RecoveryState::Recovering::react(const RequestBackfill &evt)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERING);
  release_reservations();
  return transit<WaitRemoteBackfillReserved>();
}

void IPG::RecoveryState::Recovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

IPG::RecoveryState::Recovered::Recovered(my_context ctx)
  : my_base(ctx)
{
  int newest_update_osd;

  state_name = "Started/Primary/Active/Recovered";
  context< RecoveryMachine >().log_enter(state_name);

  IPG *pg = context< RecoveryMachine >().pg;
  pg->get_osd()->local_reserver.cancel_reservation(pg->get_info().pgid);

  // if we finished backfill, all acting are active; recheck if
  // DEGRADED is appropriate.
  if (pg->get_osdmap()->get_pg_size(pg->get_info().pgid) <= pg->get_acting().size())
    pg->state_clear(PG_STATE_DEGRADED);

  // adjust acting set?  (e.g. because backfill completed...)
  if (pg->get_acting() != pg->get_up() && !pg->choose_acting(newest_update_osd))
    assert(pg->get_want_acting().size());

  assert(!pg->needs_recovery());

  if (context< Active >().all_replicas_activated)
    post_event(GoClean());
}

void IPG::RecoveryState::Recovered::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

IPG::RecoveryState::Clean::Clean(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/Clean";
  context< RecoveryMachine >().log_enter(state_name);

  IPG *pg = context< RecoveryMachine >().pg;

  if (pg->get_info().last_complete != pg->get_info().last_update) {
    assert(0);
  }
  pg->finish_recovery(*context< RecoveryMachine >().get_on_safe_context_list());
  pg->mark_clean();

  pg->share_pg_info();
  pg->publish_stats_to_osd();

}

void IPG::RecoveryState::Clean::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  IPG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_CLEAN);
}

/*---------Active---------*/
IPG::RecoveryState::Active::Active(my_context ctx)
  : my_base(ctx),
    sorted_acting_set(context< RecoveryMachine >().pg->get_acting().begin(),
                      context< RecoveryMachine >().pg->get_acting().end()),
    all_replicas_activated(false)
{
  state_name = "Started/Primary/Active";
  context< RecoveryMachine >().log_enter(state_name);

  IPG *pg = context< RecoveryMachine >().pg;

  assert(!pg->get_backfill_reserving());
  assert(!pg->get_backfill_reserved());
  assert(pg->is_primary());
  dout(10) << "In Active, about to call activate" << dendl;
  pg->activate(*context< RecoveryMachine >().get_cur_transaction(),
	       pg->get_osdmap()->get_epoch(),
	       *context< RecoveryMachine >().get_on_safe_context_list(),
	       *context< RecoveryMachine >().get_query_map(),
	       context< RecoveryMachine >().get_info_map());
  assert(pg->is_active());
  dout(10) << "Activate Finished" << dendl;
}

boost::statechart::result IPG::RecoveryState::Active::react(const AdvMap& advmap)
{
  IPG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Active advmap" << dendl;
  if (!pg->get_pool().newly_removed_snaps.empty()) {
    pg->get_snap_trimq().union_of(pg->get_pool().get_newly_removed_snaps());
    dout(10) << *pg << " snap_trimq now " << pg->get_snap_trimq() << dendl;
    pg->set_dirty_info(true);
    pg->set_dirty_big_info(true);
  }

  for (vector<int>::const_iterator p = pg->get_want_acting().begin();
       p != pg->get_want_acting().end(); ++p) {
    if (!advmap.osdmap->is_up(*p)) {
      assert((std::find(pg->get_acting().begin(), pg->get_acting().end(), *p) !=
	      pg->get_acting().end()) ||
	     (std::find(pg->get_up().begin(), pg->get_up().end(), *p) !=
	      pg->get_up().end()));
    }
  }

  /* Check for changes in pool size (if the acting set changed as a result,
   * this does not matter) */
  if (advmap.lastmap->get_pg_size(pg->get_info().pgid) !=
      pg->get_osdmap()->get_pg_size(pg->get_info().pgid)) {
    unsigned active = pg->get_acting().size();
    if (pg->get_backfill_target() != -1)
      --active;
    if (pg->get_osdmap()->get_pg_size(pg->get_info().pgid) <= active)
      pg->state_clear(PG_STATE_DEGRADED);
    else
      pg->state_set(PG_STATE_DEGRADED);
    pg->publish_stats_to_osd(); // degraded may have changed
  }
  return forward_event();
}
    
boost::statechart::result IPG::RecoveryState::Active::react(const ActMap&)
{
  IPG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Active: handling ActMap" << dendl;
  assert(pg->is_active());
  assert(pg->is_primary());

  if (pg->have_unfound()) {
    // object may have become unfound
    pg->discover_all_missing(*context< RecoveryMachine >().get_query_map());
  }

  if (g_conf->osd_check_for_log_corruption)
    pg->check_log_for_corruption(pg->get_osd()->store);

  int unfound = pg->get_missing().num_missing() - pg->get_missing_loc().size();
  if (unfound > 0 &&
      pg->all_unfound_are_queried_or_lost(pg->get_osdmap())) {
    if (g_conf->osd_auto_mark_unfound_lost) {
      pg->get_osd()->clog.error() << pg->get_info().pgid << " has " << unfound
			    << " objects unfound and apparently lost, would automatically marking lost but NOT IMPLEMENTED\n";
      //pg->mark_all_unfound_lost(*context< RecoveryMachine >().get_cur_transaction());
    } else
      pg->get_osd()->clog.error() << pg->get_info().pgid << " has " << unfound << " objects unfound and apparently lost\n";
  }

  if (!pg->get_snap_trimq().empty() &&
      pg->is_clean()) {
    dout(10) << "Active: queuing snap trim" << dendl;
    pg->queue_snap_trim();
  }

  if (!pg->is_clean() &&
      !pg->get_osdmap()->test_flag(CEPH_OSDMAP_NOBACKFILL)) {
    pg->get_osd()->queue_for_recovery(pg);
  }
  return forward_event();
}

boost::statechart::result IPG::RecoveryState::Active::react(const MNotifyRec& notevt)
{
  IPG *pg = context< RecoveryMachine >().pg;
  assert(pg->is_active());
  assert(pg->is_primary());
  if (pg->get_peer_info().count(notevt.from)) {
    dout(10) << "Active: got notify from " << notevt.from 
	     << ", already have info from that osd, ignoring" 
	     << dendl;
  } else if (pg->get_peer_purged().count(notevt.from)) {
    dout(10) << "Active: got notify from " << notevt.from
	     << ", already purged that peer, ignoring"
	     << dendl;
  } else {
    dout(10) << "Active: got notify from " << notevt.from 
	     << ", calling proc_replica_info and discover_all_missing"
	     << dendl;
    pg->proc_replica_info(notevt.from, notevt.notify.info);
    if (pg->have_unfound()) {
      pg->discover_all_missing(*context< RecoveryMachine >().get_query_map());
    }
  }
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::Active::react(const MInfoRec& infoevt)
{
  IPG *pg = context< RecoveryMachine >().pg;
  assert(pg->is_active());
  assert(pg->is_primary());

  // don't update history (yet) if we are active and primary; the replica
  // may be telling us they have activated (and committed) but we can't
  // share that until _everyone_ does the same.
  if (pg->is_acting(infoevt.from)) {
    assert(pg->get_info().history.last_epoch_started < 
	   pg->get_info().history.same_interval_since);
    assert(infoevt.info.history.last_epoch_started >= 
	   pg->get_info().history.same_interval_since);
    dout(10) << " peer osd." << infoevt.from << " activated and committed" 
	     << dendl;
    pg->get_peer_activated().insert(infoevt.from);
  }

  if (pg->get_peer_activated().size() == pg->get_acting().size()) {
    pg->all_activated_and_committed();
  }
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::Active::react(const MLogRec& logevt)
{
  dout(10) << "searching osd." << logevt.from
           << " log for unfound items" << dendl;
  IPG *pg = context< RecoveryMachine >().pg;
  bool got_missing = pg->search_for_missing(logevt.msg->info,
                                            &logevt.msg->missing, logevt.from);
  if (got_missing)
    pg->get_osd()->queue_for_recovery(pg);
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::Active::react(const QueryState& q)
{
  IPG *pg = context< RecoveryMachine >().pg;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  {
    q.f->open_array_section("might_have_unfound");
    for (set<int>::iterator p = pg->get_might_have_unfound().begin();
	 p != pg->get_might_have_unfound().end();
	 ++p) {
      q.f->open_object_section("osd");
      q.f->dump_int("osd", *p);
      if (pg->get_peer_missing().count(*p)) {
	q.f->dump_string("status", "already probed");
      } else if (pg->get_peer_missing_requested().count(*p)) {
	q.f->dump_string("status", "querying");
      } else if (!pg->get_osdmap()->is_up(*p)) {
	q.f->dump_string("status", "osd is down");
      } else {
	q.f->dump_string("status", "not queried");
      }
      q.f->close_section();
    }
    q.f->close_section();
  }
  {
    q.f->open_object_section("recovery_progress");
    pg->dump_recovery_info(q.f);
    q.f->close_section();
  }

  {
    q.f->open_object_section("scrub");
    q.f->dump_stream("scrubber.epoch_start") << pg->get_scrubber().epoch_start;
    q.f->dump_int("scrubber.active", pg->get_scrubber().active);
    q.f->dump_int("scrubber.block_writes", pg->get_scrubber().block_writes);
    q.f->dump_int("scrubber.finalizing", pg->get_scrubber().finalizing);
    q.f->dump_int("scrubber.waiting_on", pg->get_scrubber().waiting_on);
    {
      q.f->open_array_section("scrubber.waiting_on_whom");
      for (set<int>::iterator p = pg->get_scrubber().waiting_on_whom.begin();
	   p != pg->get_scrubber().waiting_on_whom.end();
	   ++p) {
	q.f->dump_int("osd", *p);
      }
      q.f->close_section();
    }
    q.f->close_section();
  }

  q.f->close_section();
  return forward_event();
}

boost::statechart::result IPG::RecoveryState::Active::react(const AllReplicasActivated &evt)
{
  all_replicas_activated = true;
  return discard_event();
}

void IPG::RecoveryState::Active::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  IPG *pg = context< RecoveryMachine >().pg;
  pg->get_osd()->local_reserver.cancel_reservation(pg->get_info().pgid);

  pg->set_backfill_reserved(false);
  pg->set_backfill_reserving(false);
  pg->state_clear(PG_STATE_DEGRADED);
  pg->state_clear(PG_STATE_BACKFILL_TOOFULL);
  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_clear(PG_STATE_RECOVERY_WAIT);
  pg->state_clear(PG_STATE_REPLAY);
}

/*------ReplicaActive-----*/
IPG::RecoveryState::ReplicaActive::ReplicaActive(my_context ctx) 
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive";

  context< RecoveryMachine >().log_enter(state_name);
}


boost::statechart::result IPG::RecoveryState::ReplicaActive::react(
  const Activate& actevt) {
  dout(10) << "In ReplicaActive, about to call activate" << dendl;
  IPG *pg = context< RecoveryMachine >().pg;
  map< int, map< pg_t, pg_query_t> > query_map;
  pg->activate(*context< RecoveryMachine >().get_cur_transaction(),
	       actevt.query_epoch,
	       *context< RecoveryMachine >().get_on_safe_context_list(),
	       query_map, NULL);
  dout(10) << "Activate Finished" << dendl;
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::ReplicaActive::react(const MInfoRec& infoevt)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->proc_primary_info(*context<RecoveryMachine>().get_cur_transaction(),
			infoevt.info);
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::ReplicaActive::react(const MLogRec& logevt)
{
  IPG *pg = context< RecoveryMachine >().pg;
  dout(10) << "received log from " << logevt.from << dendl;
  pg->merge_log(*context<RecoveryMachine>().get_cur_transaction(),
		logevt.msg->info, logevt.msg->log, logevt.from);

  assert(pg->get_log().head == pg->get_info().last_update);

  return discard_event();
}

boost::statechart::result IPG::RecoveryState::ReplicaActive::react(const ActMap&)
{
  IPG *pg = context< RecoveryMachine >().pg;
  if (pg->should_send_notify() && pg->get_primary() >= 0) {
    context< RecoveryMachine >().send_notify(pg->get_primary(),
					     pg_notify_t(pg->get_osdmap()->get_epoch(),
							 pg->get_osdmap()->get_epoch(),
							 pg->get_info()),
					     pg->get_past_intervals());
  }
  pg->take_waiters();
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::ReplicaActive::react(const MQuery& query)
{
  IPG *pg = context< RecoveryMachine >().pg;
  if (query.query.type == pg_query_t::MISSING) {
    pg->update_history_from_master(query.query.history);
    pg->fulfill_log(query.from, query.query, query.query_epoch);
  } // else: from prior to activation, safe to ignore
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::ReplicaActive::react(const QueryState& q)
{
  IPG *pg = context< RecoveryMachine >().pg;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_int("scrubber.finalizing", pg->get_scrubber().finalizing);
  q.f->close_section();
  return forward_event();
}

void IPG::RecoveryState::ReplicaActive::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  IPG *pg = context< RecoveryMachine >().pg;
  pg->get_osd()->remote_reserver.cancel_reservation(pg->get_info().pgid);
}

/*-------Stray---*/
IPG::RecoveryState::Stray::Stray(my_context ctx) 
  : my_base(ctx) {
  state_name = "Started/Stray";
  context< RecoveryMachine >().log_enter(state_name);

  IPG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_active());
  assert(!pg->is_peering());
  assert(!pg->is_primary());
}

boost::statechart::result IPG::RecoveryState::Stray::react(const MLogRec& logevt)
{
  IPG *pg = context< RecoveryMachine >().pg;
  MOSDPGLog *msg = logevt.msg.get();
  dout(10) << "got info+log from osd." << logevt.from << " " << msg->info << " " << msg->log << dendl;

  if (msg->info.last_backfill == hobject_t()) {
    // restart backfill
    pg->unreg_next_scrub();
    pg->get_info() = msg->info;
    pg->reg_next_scrub();
    pg->set_dirty_info(true);
    pg->set_dirty_big_info(true);  // maybe.
    pg->set_dirty_log(true);
    pg->get_log().claim_log(msg->log);
    pg->get_missing().clear();
  } else {
    pg->merge_log(*context<RecoveryMachine>().get_cur_transaction(),
		  msg->info, msg->log, logevt.from);
  }

  assert(pg->get_log().head == pg->get_info().last_update);

  post_event(Activate(logevt.msg->get_epoch()));
  return transit<ReplicaActive>();
}

boost::statechart::result IPG::RecoveryState::Stray::react(const MInfoRec& infoevt)
{
  IPG *pg = context< RecoveryMachine >().pg;
  dout(10) << "got info from osd." << infoevt.from << " " << infoevt.info << dendl;

  if (pg->get_info().last_update > infoevt.info.last_update) {
    // rewind divergent log entries
    pg->rewind_divergent_log(*context< RecoveryMachine >().get_cur_transaction(),
			     infoevt.info.last_update);
    pg->get_info().stats = infoevt.info.stats;
  }
  
  assert(infoevt.info.last_update == pg->get_info().last_update);
  assert(pg->get_log().head == pg->get_info().last_update);

  post_event(Activate(infoevt.msg_epoch));
  return transit<ReplicaActive>();
}

boost::statechart::result IPG::RecoveryState::Stray::react(const MQuery& query)
{
  IPG *pg = context< RecoveryMachine >().pg;
  if (query.query.type == pg_query_t::INFO) {
    pair<int, pg_info_t> notify_info;
    pg->update_history_from_master(query.query.history);
    pg->fulfill_info(query.from, query.query, notify_info);
    context< RecoveryMachine >().send_notify(notify_info.first,
					     pg_notify_t(query.query_epoch,
							 pg->get_osdmap()->get_epoch(),
							 notify_info.second),
					     pg->get_past_intervals());
  } else {
    pg->fulfill_log(query.from, query.query, query.query_epoch);
  }
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::Stray::react(const ActMap&)
{
  IPG *pg = context< RecoveryMachine >().pg;
  if (pg->should_send_notify() && pg->get_primary() >= 0) {
    context< RecoveryMachine >().send_notify(pg->get_primary(),
					     pg_notify_t(pg->get_osdmap()->get_epoch(),
							 pg->get_osdmap()->get_epoch(),
							 pg->get_info()),
					     pg->get_past_intervals());
  }
  pg->take_waiters();
  return discard_event();
}

void IPG::RecoveryState::Stray::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*--------GetInfo---------*/
IPG::RecoveryState::GetInfo::GetInfo(my_context ctx)
  : my_base(ctx) 
{
  state_name = "Started/Primary/Peering/GetInfo";
  context< RecoveryMachine >().log_enter(state_name);

  IPG *pg = context< RecoveryMachine >().pg;
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

void IPG::RecoveryState::GetInfo::get_infos()
{
  IPG *pg = context< RecoveryMachine >().pg;
  auto_ptr<PriorSet> &prior_set = context< Peering >().prior_set;

  for (set<int>::const_iterator it = prior_set->probe.begin();
       it != prior_set->probe.end();
       ++it) {
    int peer = *it;
    if (peer == pg->get_osd()->whoami) {
      continue;
    }
    if (pg->get_peer_info().count(peer)) {
      dout(10) << " have osd." << peer << " info " << pg->get_peer_info()[peer] << dendl;
      continue;
    }
    if (peer_info_requested.count(peer)) {
      dout(10) << " already requested info from osd." << peer << dendl;
    } else if (!pg->get_osdmap()->is_up(peer)) {
      dout(10) << " not querying info from down osd." << peer << dendl;
    } else {
      dout(10) << " querying info from osd." << peer << dendl;
      context< RecoveryMachine >().send_query(
	peer, pg_query_t(pg_query_t::INFO,
			 pg->get_info().history,
			 pg->get_osdmap()->get_epoch()));
      peer_info_requested.insert(peer);
    }
  }
}

boost::statechart::result IPG::RecoveryState::GetInfo::react(const MNotifyRec& infoevt) 
{
  set<int>::iterator p = peer_info_requested.find(infoevt.from);
  if (p != peer_info_requested.end())
    peer_info_requested.erase(p);

  IPG *pg = context< RecoveryMachine >().pg;
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
	      pg_info_t *pinfo;
	      if (o == pg->get_osd()->whoami) {
		pinfo = &pg->get_info();
	      } else {
		assert(pg->get_peer_info().count(o));
		pinfo = &pg->get_peer_info()[o];
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

boost::statechart::result IPG::RecoveryState::GetInfo::react(const QueryState& q)
{
  IPG *pg = context< RecoveryMachine >().pg;
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("requested_info_from");
  for (set<int>::iterator p = peer_info_requested.begin(); p != peer_info_requested.end(); ++p) {
    q.f->open_object_section("osd");
    q.f->dump_int("osd", *p);
    if (pg->get_peer_info().count(*p)) {
      q.f->open_object_section("got_info");
      pg->get_peer_info()[*p].dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void IPG::RecoveryState::GetInfo::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------GetLog------------*/
IPG::RecoveryState::GetLog::GetLog(my_context ctx) : 
  my_base(ctx), newest_update_osd(-1), msg(0)
{
  state_name = "Started/Primary/Peering/GetLog";
  context< RecoveryMachine >().log_enter(state_name);

  IPG *pg = context< RecoveryMachine >().pg;

  // adjust acting?
  if (!pg->choose_acting(newest_update_osd)) {
    if (!pg->get_want_acting().empty()) {
      post_event(NeedActingChange());
    } else {
      post_event(IsIncomplete());
    }
    return;
  }

  // am i the best?
  if (newest_update_osd == pg->get_osd()->whoami) {
    post_event(GotLog());
    return;
  }

  const pg_info_t& best = pg->get_peer_info()[newest_update_osd];

  // am i broken?
  if (pg->get_info().last_update < best.log_tail) {
    dout(10) << " not contiguous with osd." << newest_update_osd << ", down" << dendl;
    post_event(IsIncomplete());
    return;
  }

  // how much log to request?
  eversion_t request_log_from = pg->get_info().last_update;
  for (vector<int>::const_iterator p = pg->get_acting().begin() + 1; p != pg->get_acting().end(); ++p) {
    pg_info_t& ri = pg->get_peer_info()[*p];
    if (ri.last_update >= best.log_tail && ri.last_update < request_log_from)
      request_log_from = ri.last_update;
  }

  // how much?
  dout(10) << " requesting log from osd." << newest_update_osd << dendl;
  context<RecoveryMachine>().send_query(
    newest_update_osd,
    pg_query_t(pg_query_t::LOG, request_log_from, pg->get_info().history,
	       pg->get_osdmap()->get_epoch()));
}

boost::statechart::result IPG::RecoveryState::GetLog::react(const AdvMap& advmap)
{
  // make sure our log source didn't go down.  we need to check
  // explicitly because it may not be part of the prior set, which
  // means the Peering state check won't catch it going down.
  if (!advmap.osdmap->is_up(newest_update_osd)) {
    dout(10) << "GetLog: newest_update_osd osd." << newest_update_osd << " went down" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }

  // let the Peering state do its checks.
  return forward_event();
}

boost::statechart::result IPG::RecoveryState::GetLog::react(const MLogRec& logevt)
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

boost::statechart::result IPG::RecoveryState::GetLog::react(const GotLog&)
{
  dout(10) << "leaving GetLog" << dendl;
  IPG *pg = context< RecoveryMachine >().pg;
  if (msg) {
    dout(10) << "processing master log" << dendl;
    pg->proc_master_log(*context<RecoveryMachine>().get_cur_transaction(),
			msg->info, msg->log, msg->missing, 
			newest_update_osd);
  }
  return transit< GetMissing >();
}

boost::statechart::result IPG::RecoveryState::GetLog::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_int("newest_update_osd", newest_update_osd);
  q.f->close_section();
  return forward_event();
}

void IPG::RecoveryState::GetLog::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------WaitActingChange--------*/
IPG::RecoveryState::WaitActingChange::WaitActingChange(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/WaitActingChange";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result IPG::RecoveryState::WaitActingChange::react(const AdvMap& advmap)
{
  IPG *pg = context< RecoveryMachine >().pg;
  OSDMapRef osdmap = advmap.osdmap;

  dout(10) << "verifying no want_acting " << pg->get_want_acting() << " targets didn't go down" << dendl;
  for (vector<int>::iterator p = pg->get_want_acting().begin(); p != pg->get_want_acting().end(); ++p) {
    if (!osdmap->is_up(*p)) {
      dout(10) << " want_acting target osd." << *p << " went down, resetting" << dendl;
      post_event(advmap);
      return transit< Reset >();
    }
  }
  return forward_event();
}

boost::statechart::result IPG::RecoveryState::WaitActingChange::react(const MLogRec& logevt)
{
  dout(10) << "In WaitActingChange, ignoring MLocRec" << dendl;
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::WaitActingChange::react(const MInfoRec& evt)
{
  dout(10) << "In WaitActingChange, ignoring MInfoRec" << dendl;
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::WaitActingChange::react(const MNotifyRec& evt)
{
  dout(10) << "In WaitActingChange, ignoring MNotifyRec" << dendl;
  return discard_event();
}

boost::statechart::result IPG::RecoveryState::WaitActingChange::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for pg acting set to change");
  q.f->close_section();
  return forward_event();
}

void IPG::RecoveryState::WaitActingChange::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------Incomplete--------*/
IPG::RecoveryState::Incomplete::Incomplete(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/Incomplete";
  context< RecoveryMachine >().log_enter(state_name);
  IPG *pg = context< RecoveryMachine >().pg;

  pg->state_clear(PG_STATE_PEERING);
  pg->state_set(PG_STATE_INCOMPLETE);
  pg->publish_stats_to_osd();
}

boost::statechart::result IPG::RecoveryState::Incomplete::react(const AdvMap &advmap) {
  IPG *pg = context< RecoveryMachine >().pg;
  int64_t poolnum = pg->get_info().pgid.pool();

  // Reset if min_size changed, pg might now be able to go active
  if (advmap.lastmap->get_pools().find(poolnum)->second.min_size !=
      advmap.osdmap->get_pools().find(poolnum)->second.min_size) {
    post_event(advmap);
    return transit< Reset >();
  }

  return forward_event();
}

void IPG::RecoveryState::Incomplete::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  IPG *pg = context< RecoveryMachine >().pg;

  pg->state_clear(PG_STATE_INCOMPLETE);
}

/*------GetMissing--------*/
IPG::RecoveryState::GetMissing::GetMissing(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/GetMissing";
  context< RecoveryMachine >().log_enter(state_name);

  IPG *pg = context< RecoveryMachine >().pg;
  for (vector<int>::const_iterator i = pg->get_acting().begin() + 1;
       i != pg->get_acting().end();
       ++i) {
    const pg_info_t& pi = pg->get_peer_info()[*i];

    if (pi.is_empty())
      continue;                                // no pg data, nothing divergent

    if (pi.last_update < pg->get_log().tail) {
      dout(10) << " osd." << *i << " is not contiguous, will restart backfill" << dendl;
      pg->get_peer_missing()[*i];
      continue;
    }
    if (pi.last_backfill == hobject_t()) {
      dout(10) << " osd." << *i << " will fully backfill; can infer empty missing set" << dendl;
      pg->get_peer_missing()[*i];
      continue;
    }

    if (pi.last_update == pi.last_complete &&  // peer has no missing
	pi.last_update == pg->get_info().last_update) {  // peer is up to date
      // replica has no missing and identical log as us.  no need to
      // pull anything.
      // FIXME: we can do better here.  if last_update==last_complete we
      //        can infer the rest!
      dout(10) << " osd." << *i << " has no missing, identical log" << dendl;
      pg->get_peer_missing()[*i];
      pg->search_for_missing(pi, &pg->get_peer_missing()[*i], *i);
      continue;
    }

    // We pull the log from the peer's last_epoch_started to ensure we
    // get enough log to detect divergent updates.
    eversion_t since(pi.last_epoch_started, 0);
    assert(pi.last_update >= pg->get_info().log_tail);  // or else choose_acting() did a bad thing
    if (pi.log_tail <= since) {
      dout(10) << " requesting log+missing since " << since << " from osd." << *i << dendl;
      context< RecoveryMachine >().send_query(
	*i,
	pg_query_t(pg_query_t::LOG, since, pg->get_info().history,
		   pg->get_osdmap()->get_epoch()));
    } else {
      dout(10) << " requesting fulllog+missing from osd." << *i
	       << " (want since " << since << " < log.tail " << pi.log_tail << ")"
	       << dendl;
      context< RecoveryMachine >().send_query(
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

boost::statechart::result IPG::RecoveryState::GetMissing::react(const MLogRec& logevt)
{
  IPG *pg = context< RecoveryMachine >().pg;

  peer_missing_requested.erase(logevt.from);
  pg->proc_replica_log(*context<RecoveryMachine>().get_cur_transaction(),
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

boost::statechart::result IPG::RecoveryState::GetMissing::react(const QueryState& q)
{
  IPG *pg = context< RecoveryMachine >().pg;
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("peer_missing_requested");
  for (set<int>::iterator p = peer_missing_requested.begin(); p != peer_missing_requested.end(); ++p) {
    q.f->open_object_section("osd");
    q.f->dump_int("osd", *p);
    if (pg->get_peer_missing().count(*p)) {
      q.f->open_object_section("got_missing");
      pg->get_peer_missing()[*p].dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void IPG::RecoveryState::GetMissing::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---WaitFlushedPeering---*/
IPG::RecoveryState::WaitFlushedPeering::WaitFlushedPeering(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/WaitFlushedPeering";
  IPG *pg = context< RecoveryMachine >().pg;
  context< RecoveryMachine >().log_enter(state_name);
  if (context< RecoveryMachine >().pg->flushed)
    post_event(Activate(pg->get_osdmap()->get_epoch()));
}

boost::statechart::result
IPG::RecoveryState::WaitFlushedPeering::react(const FlushedEvt &evt)
{
  IPG *pg = context< RecoveryMachine >().pg;
  pg->flushed = true;
  pg->requeue_ops(pg->get_waiting_for_active());
  return transit< WaitFlushedPeering >();
}

boost::statechart::result
IPG::RecoveryState::WaitFlushedPeering::react(const QueryState &q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for flush");
  return forward_event();
}

/*------WaitUpThru--------*/
IPG::RecoveryState::WaitUpThru::WaitUpThru(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/WaitUpThru";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result IPG::RecoveryState::WaitUpThru::react(const ActMap& am)
{
  IPG *pg = context< RecoveryMachine >().pg;
  if (!pg->get_need_up_thru()) {
    post_event(CheckRepops());
  }
  return forward_event();
}

boost::statechart::result IPG::RecoveryState::WaitUpThru::react(const MLogRec& logevt)
{
  dout(10) << "searching osd." << logevt.from
           << " log for unfound items" << dendl;
  IPG *pg = context< RecoveryMachine >().pg;
  bool got_missing = pg->search_for_missing(logevt.msg->info,
                                            &logevt.msg->missing, logevt.from);

  // hmm.. should we?
  (void)got_missing;
  //if (got_missing)
  //pg->get_osd()->queue_for_recovery(pg);

  return discard_event();
}

boost::statechart::result IPG::RecoveryState::WaitUpThru::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for osdmap to reflect a new up_thru for this osd");
  q.f->close_section();
  return forward_event();
}

void IPG::RecoveryState::WaitUpThru::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*----RecoveryState::RecoveryMachine Methods-----*/
#undef dout_prefix
#define dout_prefix *_dout << pg->gen_prefix() 

void IPG::RecoveryState::RecoveryMachine::log_enter(const char *state_name)
{
  dout(20) << "enter " << state_name << dendl;
  pg->get_osd()->pg_recovery_stats.log_enter(state_name);
}

void IPG::RecoveryState::RecoveryMachine::log_exit(const char *state_name, utime_t enter_time)
{
  utime_t dur = ceph_clock_now(g_ceph_context) - enter_time;
  dout(20) << "exit " << state_name << " " << dur << " " << event_count << " " << event_time << dendl;
  pg->get_osd()->pg_recovery_stats.log_exit(state_name, ceph_clock_now(g_ceph_context) - enter_time,
				      event_count, event_time);
  event_count = 0;
  event_time = utime_t();
}
