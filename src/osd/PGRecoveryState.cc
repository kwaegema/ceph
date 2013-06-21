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

#include "PGRecoveryState.h"
#include "PGPeering.h"
#include "OSD.h"

#include "messages/MBackfillReserve.h"
#include "messages/MRecoveryReserve.h"
#include "include/types.h"

using namespace PGRecoveryState;

#define dout_subsys ceph_subsys_osd

/*------------ Recovery State Machine----------------*/
#undef dout_prefix
#define dout_prefix (*_dout << context< RecoveryMachine >().pg->gen_prefix() \
		     << "state<" << get_state_name() << ">: ")

/*------Crashed-------*/
RecoveryState::Crashed::Crashed(my_context ctx)
  : my_base(ctx)
{
  state_name = "Crashed";
  context< RecoveryMachine >().log_enter(state_name);
  assert(0 == "we got a bad state machine event");
}


/*------Initial-------*/
RecoveryState::Initial::Initial(my_context ctx)
  : my_base(ctx)
{
  state_name = "Initial";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result RecoveryState::Initial::react(const Load& l)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;

  // do we tell someone we're here?
  pg->set_send_notify();

  return transit< Reset >();
}

boost::statechart::result RecoveryState::Initial::react(const MNotifyRec& notify)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->proc_replica_info(notify.from, notify.notify.info);
  pg->update_heartbeat_peers();
  pg->set_last_peering_reset();
  return transit< Primary >();
}

boost::statechart::result RecoveryState::Initial::react(const MInfoRec& i)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_primary());
  post_event(i);
  return transit< Stray >();
}

boost::statechart::result RecoveryState::Initial::react(const MLogRec& i)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_primary());
  post_event(i);
  return transit< Stray >();
}

void RecoveryState::Initial::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------Started-------*/
RecoveryState::Started::Started(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started";
  context< RecoveryMachine >().log_enter(state_name);
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->start_flush(
    context< RecoveryMachine >().get_cur_transaction(),
    context< RecoveryMachine >().get_on_applied_context_list(),
    context< RecoveryMachine >().get_on_safe_context_list());
}

boost::statechart::result
RecoveryState::Started::react(const FlushedEvt&)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->set_flushed(true);
  pg->requeue_waiting_for_active();
  return discard_event();
}


boost::statechart::result RecoveryState::Started::react(const AdvMap& advmap)
{
  dout(10) << "Started advmap" << dendl;
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  if (pg->acting_up_affected(advmap.newup, advmap.newacting) ||
    pg->is_split(advmap.lastmap, advmap.osdmap)) {
    dout(10) << "up or acting affected, transitioning to Reset" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }
  pg->remove_down_peer_info(advmap.osdmap);
  return discard_event();
}

boost::statechart::result RecoveryState::Started::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

void RecoveryState::Started::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*--------Reset---------*/
RecoveryState::Reset::Reset(my_context ctx)
  : my_base(ctx)
{
  state_name = "Reset";
  context< RecoveryMachine >().log_enter(state_name);
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->set_last_peering_reset();
}

boost::statechart::result
RecoveryState::Reset::react(const FlushedEvt&)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->set_flushed(true);
  pg->on_flushed();
  pg->requeue_waiting_for_active();
  return discard_event();
}

boost::statechart::result RecoveryState::Reset::react(const AdvMap& advmap)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
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

boost::statechart::result RecoveryState::Reset::react(const ActMap&)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
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

boost::statechart::result RecoveryState::Reset::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

void RecoveryState::Reset::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*-------Start---------*/
RecoveryState::Start::Start(my_context ctx)
  : my_base(ctx)
{
  state_name = "Start";
  context< RecoveryMachine >().log_enter(state_name);

  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  if (pg->is_primary()) {
    dout(1) << "transitioning to Primary" << dendl;
    post_event(MakePrimary());
  } else { //is_stray
    dout(1) << "transitioning to Stray" << dendl; 
    post_event(MakeStray());
  }
}

void RecoveryState::Start::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---------Primary--------*/
RecoveryState::Primary::Primary(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary";
  context< RecoveryMachine >().log_enter(state_name);
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  assert(pg->get_want_acting().empty());
}

boost::statechart::result RecoveryState::Primary::react(const AdvMap &advmap)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->remove_down_peer_info(advmap.osdmap);
  return forward_event();
}

boost::statechart::result RecoveryState::Primary::react(const MNotifyRec& notevt)
{
  dout(7) << "handle_pg_notify from osd." << notevt.from << dendl;
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  const map<int,pg_info_t>  &peer_info = pg->get_peer_info();
  if (peer_info.count(notevt.from) &&
      peer_info.find(notevt.from)->second.last_update == notevt.notify.info.last_update) {
    dout(10) << *pg << " got dup osd." << notevt.from << " info " << notevt.notify.info
	     << ", identical to ours" << dendl;
  } else {
    pg->proc_replica_info(notevt.from, notevt.notify.info);
  }
  return discard_event();
}

boost::statechart::result RecoveryState::Primary::react(const ActMap&)
{
  dout(7) << "handle ActMap primary" << dendl;
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->publish_stats_to_osd();
  pg->take_waiters();
  return discard_event();
}

void RecoveryState::Primary::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->clear_want_acting();
}

/*------Backfilling-------*/
RecoveryState::Backfilling::Backfilling(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/Backfilling";
  context< RecoveryMachine >().log_enter(state_name);
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->set_backfill_reserved(true);
  pg->get_osd()->queue_for_recovery(pg);
  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_set(PG_STATE_BACKFILL);
}

boost::statechart::result
RecoveryState::Backfilling::react(const RemoteReservationRejected &)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->get_osd()->local_reserver.cancel_reservation(pg->get_info().pgid);
  pg->state_set(PG_STATE_BACKFILL_TOOFULL);

  pg->get_osd()->dequeue_from_recovery(pg);

  pg->schedule_backfill_full_retry();
  return transit<NotBackfilling>();
}

void RecoveryState::Backfilling::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->set_backfill_reserved(false);
  pg->set_backfill_reserving(false);
  pg->state_clear(PG_STATE_BACKFILL);
}

/*--WaitRemoteBackfillReserved--*/

RecoveryState::WaitRemoteBackfillReserved::WaitRemoteBackfillReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/WaitRemoteBackfillReserved";
  context< RecoveryMachine >().log_enter(state_name);
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  ConnectionRef con = pg->get_osd()->get_con_osd_cluster(
    pg->get_backfill_target(), pg->get_osdmap()->get_epoch());
  if (con) {
    if ((con->features & CEPH_FEATURE_BACKFILL_RESERVATION)) {
      unsigned priority = pg->is_degraded() ? OSDService::BACKFILL_HIGH
	  : OSDService::BACKFILL_LOW;
      pg->get_osd()->send_message_osd_cluster(
       new MBackfillReserve(
	  MBackfillReserve::REQUEST,
	  pg->get_info().pgid,
	  pg->get_osdmap()->get_epoch(), priority),
	con.get());
    } else {
      post_event(RemoteBackfillReserved());
    }
  }
}

void RecoveryState::WaitRemoteBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

boost::statechart::result
RecoveryState::WaitRemoteBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_BACKFILL_TOOFULL);
  return transit<Backfilling>();
}

boost::statechart::result
RecoveryState::WaitRemoteBackfillReserved::react(const RemoteReservationRejected &evt)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->get_osd()->local_reserver.cancel_reservation(pg->get_info().pgid);
  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_set(PG_STATE_BACKFILL_TOOFULL);

  pg->schedule_backfill_full_retry();

  return transit<NotBackfilling>();
}

/*--WaitLocalBackfillReserved--*/
RecoveryState::WaitLocalBackfillReserved::WaitLocalBackfillReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/WaitLocalBackfillReserved";
  context< RecoveryMachine >().log_enter(state_name);
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  pg->get_osd()->local_reserver.request_reservation(
    pg->get_info().pgid,
    new QueuePeeringEvt<LocalBackfillReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      LocalBackfillReserved()), pg->is_degraded() ? OSDService::BACKFILL_HIGH
	 : OSDService::BACKFILL_LOW);
}

void RecoveryState::WaitLocalBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*----NotBackfilling------*/
RecoveryState::NotBackfilling::NotBackfilling(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/NotBackfilling";
  context< RecoveryMachine >().log_enter(state_name);
}

void RecoveryState::NotBackfilling::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---RepNotRecovering----*/
RecoveryState::RepNotRecovering::RepNotRecovering(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive/RepNotRecovering";
  context< RecoveryMachine >().log_enter(state_name);
}

void RecoveryState::RepNotRecovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*---RepWaitRecoveryReserved--*/
RecoveryState::RepWaitRecoveryReserved::RepWaitRecoveryReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive/RepWaitRecoveryReserved";
  context< RecoveryMachine >().log_enter(state_name);
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;

  pg->get_osd()->remote_reserver.request_reservation(
    pg->get_info().pgid,
    new QueuePeeringEvt<RemoteRecoveryReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      RemoteRecoveryReserved()), OSDService::RECOVERY);
}

boost::statechart::result
RecoveryState::RepWaitRecoveryReserved::react(const RemoteRecoveryReserved &evt)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->get_osd()->send_message_osd_cluster(
    pg->get_acting()[0],
    new MRecoveryReserve(
      MRecoveryReserve::GRANT,
      pg->get_info().pgid,
      pg->get_osdmap()->get_epoch()),
    pg->get_osdmap()->get_epoch());
  return transit<RepRecovering>();
}

void RecoveryState::RepWaitRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*-RepWaitBackfillReserved*/
RecoveryState::RepWaitBackfillReserved::RepWaitBackfillReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive/RepWaitBackfillReserved";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result 
RecoveryState::RepNotRecovering::react(const RequestBackfillPrio &evt)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;

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
        RemoteBackfillReserved()), evt.priority);
  }
  return transit<RepWaitBackfillReserved>();
}

void RecoveryState::RepWaitBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

boost::statechart::result
RecoveryState::RepWaitBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
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
RecoveryState::RepWaitBackfillReserved::react(const RemoteReservationRejected &evt)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->reject_reservation();
  return transit<RepNotRecovering>();
}

/*---RepRecovering-------*/
RecoveryState::RepRecovering::RepRecovering(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive/RepRecovering";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result
RecoveryState::RepRecovering::react(const BackfillTooFull &)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->reject_reservation();
  return transit<RepNotRecovering>();
}

void RecoveryState::RepRecovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->get_osd()->remote_reserver.cancel_reservation(pg->get_info().pgid);
}

/*------Activating--------*/
RecoveryState::Activating::Activating(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/Activating";
  context< RecoveryMachine >().log_enter(state_name);
}

void RecoveryState::Activating::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

RecoveryState::WaitLocalRecoveryReserved::WaitLocalRecoveryReserved(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/WaitLocalRecoveryReserved";
  context< RecoveryMachine >().log_enter(state_name);
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_RECOVERY_WAIT);
  pg->get_osd()->local_reserver.request_reservation(
    pg->get_info().pgid,
    new QueuePeeringEvt<LocalRecoveryReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      LocalRecoveryReserved()), OSDService::RECOVERY);
}

void RecoveryState::WaitLocalRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

RecoveryState::WaitRemoteRecoveryReserved::WaitRemoteRecoveryReserved(my_context ctx)
  : my_base(ctx),
    acting_osd_it(context< Active >().sorted_acting_set.begin())
{
  state_name = "Started/Primary/Active/WaitRemoteRecoveryReserved";
  context< RecoveryMachine >().log_enter(state_name);
  post_event(RemoteRecoveryReserved());
}

boost::statechart::result
RecoveryState::WaitRemoteRecoveryReserved::react(const RemoteRecoveryReserved &evt) {
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;

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

void RecoveryState::WaitRemoteRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

RecoveryState::Recovering::Recovering(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/Recovering";
  context< RecoveryMachine >().log_enter(state_name);

  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERY_WAIT);
  pg->state_set(PG_STATE_RECOVERING);
  pg->get_osd()->queue_for_recovery(pg);
}

void RecoveryState::Recovering::release_reservations()
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  assert(!pg->get_pg_log().get_missing().have_missing());
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
RecoveryState::Recovering::react(const AllReplicasRecovered &evt)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERING);
  release_reservations();
  return transit<Recovered>();
}

boost::statechart::result
RecoveryState::Recovering::react(const RequestBackfill &evt)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERING);
  release_reservations();
  return transit<WaitRemoteBackfillReserved>();
}

void RecoveryState::Recovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

RecoveryState::Recovered::Recovered(my_context ctx)
  : my_base(ctx)
{
  int newest_update_osd;

  state_name = "Started/Primary/Active/Recovered";
  context< RecoveryMachine >().log_enter(state_name);

  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
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

void RecoveryState::Recovered::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

RecoveryState::Clean::Clean(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Active/Clean";
  context< RecoveryMachine >().log_enter(state_name);

  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;

  if (pg->get_info().last_complete != pg->get_info().last_update) {
    assert(0);
  }
  pg->finish_recovery(*context< RecoveryMachine >().get_on_safe_context_list());
  pg->mark_clean();

  pg->share_pg_info();
  pg->publish_stats_to_osd();

}

void RecoveryState::Clean::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_CLEAN);
}

/*---------Active---------*/
RecoveryState::Active::Active(my_context ctx)
  : my_base(ctx),
    sorted_acting_set(context< RecoveryMachine >().pg->get_acting().begin(),
                      context< RecoveryMachine >().pg->get_acting().end()),
    all_replicas_activated(false)
{
  state_name = "Started/Primary/Active";
  context< RecoveryMachine >().log_enter(state_name);

  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;

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

boost::statechart::result RecoveryState::Active::react(const AdvMap& advmap)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  dout(10) << "Active advmap" << dendl;
  if (!pg->get_pool().newly_removed_snaps.empty()) {
    pg->get_snap_trimq().union_of(pg->get_pool().newly_removed_snaps);
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
    
boost::statechart::result RecoveryState::Active::react(const ActMap&)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  dout(10) << "Active: handling ActMap" << dendl;
  assert(pg->is_active());
  assert(pg->is_primary());

  if (pg->have_unfound()) {
    // object may have become unfound
    pg->discover_all_missing(*context< RecoveryMachine >().get_query_map());
  }

  if (g_conf->osd_check_for_log_corruption)
    pg->check_log_for_corruption(pg->get_osd()->store);

  int unfound = pg->get_pg_log().get_missing().num_missing() - pg->get_missing_loc().size();
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

boost::statechart::result RecoveryState::Active::react(const MNotifyRec& notevt)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
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

boost::statechart::result RecoveryState::Active::react(const MInfoRec& infoevt)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
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

boost::statechart::result RecoveryState::Active::react(const MLogRec& logevt)
{
  dout(10) << "searching osd." << logevt.from
           << " log for unfound items" << dendl;
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  bool got_missing = pg->search_for_missing(logevt.msg->info,
                                            &logevt.msg->missing, logevt.from);
  if (got_missing)
    pg->get_osd()->queue_for_recovery(pg);
  return discard_event();
}

boost::statechart::result RecoveryState::Active::react(const QueryState& q)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  {
    q.f->open_array_section("might_have_unfound");
    for (set<int>::const_iterator p = pg->get_might_have_unfound().begin();
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

boost::statechart::result RecoveryState::Active::react(const AllReplicasActivated &evt)
{
  all_replicas_activated = true;
  return discard_event();
}

void RecoveryState::Active::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
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
RecoveryState::ReplicaActive::ReplicaActive(my_context ctx) 
  : my_base(ctx)
{
  state_name = "Started/ReplicaActive";

  context< RecoveryMachine >().log_enter(state_name);
}


boost::statechart::result RecoveryState::ReplicaActive::react(
  const Activate& actevt) {
  dout(10) << "In ReplicaActive, about to call activate" << dendl;
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  map< int, map< pg_t, pg_query_t> > query_map;
  pg->activate(*context< RecoveryMachine >().get_cur_transaction(),
	       actevt.query_epoch,
	       *context< RecoveryMachine >().get_on_safe_context_list(),
	       query_map, NULL);
  dout(10) << "Activate Finished" << dendl;
  return discard_event();
}

boost::statechart::result RecoveryState::ReplicaActive::react(const MInfoRec& infoevt)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->proc_primary_info(*context<RecoveryMachine>().get_cur_transaction(),
			infoevt.info);
  return discard_event();
}

boost::statechart::result RecoveryState::ReplicaActive::react(const MLogRec& logevt)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  dout(10) << "received log from " << logevt.from << dendl;
  ObjectStore::Transaction* t = context<RecoveryMachine>().get_cur_transaction();
  pg->merge_log(*t,logevt.msg->info, logevt.msg->log, logevt.from);
  assert(pg->get_pg_log().get_head() == pg->get_info().last_update);

  return discard_event();
}

boost::statechart::result RecoveryState::ReplicaActive::react(const ActMap&)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
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

boost::statechart::result RecoveryState::ReplicaActive::react(const MQuery& query)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  if (query.query.type == pg_query_t::MISSING) {
    pg->update_history_from_master(query.query.history);
    pg->fulfill_log(query.from, query.query, query.query_epoch);
  } // else: from prior to activation, safe to ignore
  return discard_event();
}

boost::statechart::result RecoveryState::ReplicaActive::react(const QueryState& q)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_int("scrubber.finalizing", pg->get_scrubber().finalizing);
  q.f->close_section();
  return forward_event();
}

void RecoveryState::ReplicaActive::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  pg->get_osd()->remote_reserver.cancel_reservation(pg->get_info().pgid);
}

/*-------Stray---*/
RecoveryState::Stray::Stray(my_context ctx) 
  : my_base(ctx) {
  state_name = "Started/Stray";
  context< RecoveryMachine >().log_enter(state_name);

  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_active());
  assert(!pg->is_peering());
  assert(!pg->is_primary());
}

boost::statechart::result RecoveryState::Stray::react(const MLogRec& logevt)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  MOSDPGLog *msg = logevt.msg.get();
  dout(10) << "got info+log from osd." << logevt.from << " " << msg->info << " " << msg->log << dendl;

  if (msg->info.last_backfill == hobject_t()) {
    // restart backfill
    pg->unreg_next_scrub();
    pg->set_info(msg->info);
    pg->reg_next_scrub();
    pg->set_dirty_info(true);
    pg->set_dirty_big_info(true);  // maybe.
    pg->claim_log(msg->log);
    pg->reset_backfill();
  } else {
    ObjectStore::Transaction* t = context<RecoveryMachine>().get_cur_transaction();
    pg->merge_log(*t, msg->info, msg->log, logevt.from);
  }

  assert(pg->get_pg_log().get_head() == pg->get_info().last_update);

  post_event(Activate(logevt.msg->get_epoch()));
  return transit<ReplicaActive>();
}

boost::statechart::result RecoveryState::Stray::react(const MInfoRec& infoevt)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  dout(10) << "got info from osd." << infoevt.from << " " << infoevt.info << dendl;

  if (pg->get_info().last_update > infoevt.info.last_update) {
    // rewind divergent log entries
    ObjectStore::Transaction* t = context<RecoveryMachine>().get_cur_transaction();
    pg->rewind_divergent_log(*t, infoevt.info.last_update);
    pg->set_info_stats(infoevt.info.stats);
  }
  
  assert(infoevt.info.last_update == pg->get_info().last_update);
  assert(pg->get_pg_log().get_head() == pg->get_info().last_update);

  post_event(Activate(infoevt.msg_epoch));
  return transit<ReplicaActive>();
}

boost::statechart::result RecoveryState::Stray::react(const MQuery& query)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
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

boost::statechart::result RecoveryState::Stray::react(const ActMap&)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
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

void RecoveryState::Stray::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*------WaitActingChange--------*/
RecoveryState::WaitActingChange::WaitActingChange(my_context ctx)
  : my_base(ctx)
{
  state_name = "Started/Primary/Peering/WaitActingChange";
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result RecoveryState::WaitActingChange::react(const AdvMap& advmap)
{
  PGRecoveryStateInterface *pg = context< RecoveryMachine >().pg;
  OSDMapRef osdmap = advmap.osdmap;

  dout(10) << "verifying no want_acting targets went down" << dendl;
  for (vector<int>::const_iterator p = pg->get_want_acting().begin(); p != pg->get_want_acting().end(); ++p) {
    if (!osdmap->is_up(*p)) {
      dout(10) << " want_acting target osd." << *p << " went down, resetting" << dendl;
      post_event(advmap);
      return transit< Reset >();
    }
  }
  return forward_event();
}

boost::statechart::result RecoveryState::WaitActingChange::react(const MLogRec& logevt)
{
  dout(10) << "In WaitActingChange, ignoring MLocRec" << dendl;
  return discard_event();
}

boost::statechart::result RecoveryState::WaitActingChange::react(const MInfoRec& evt)
{
  dout(10) << "In WaitActingChange, ignoring MInfoRec" << dendl;
  return discard_event();
}

boost::statechart::result RecoveryState::WaitActingChange::react(const MNotifyRec& evt)
{
  dout(10) << "In WaitActingChange, ignoring MNotifyRec" << dendl;
  return discard_event();
}

boost::statechart::result RecoveryState::WaitActingChange::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for pg acting set to change");
  q.f->close_section();
  return forward_event();
}

void RecoveryState::WaitActingChange::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*----RecoveryState::RecoveryMachine Methods-----*/
#undef dout_prefix
#define dout_prefix *_dout << pg->gen_prefix() 

void RecoveryState::RecoveryMachine::log_enter(const char *state_name)
{
  dout(20) << "enter " << state_name << dendl;
  pg->get_osd()->pg_recovery_stats.log_enter(state_name);
}

void RecoveryState::RecoveryMachine::log_exit(const char *state_name, utime_t enter_time)
{
  utime_t dur = ceph_clock_now(g_ceph_context) - enter_time;
  dout(20) << "exit " << state_name << " " << dur << " " << event_count << " " << event_time << dendl;
  pg->get_osd()->pg_recovery_stats.log_exit(state_name, ceph_clock_now(g_ceph_context) - enter_time,
				      event_count, event_time);
  event_count = 0;
  event_time = utime_t();
}


void RecoveryState::handle_event(CephPeeringEvtRef evt,
				 RecoveryCtx *rctx) {
  start_handle(rctx);
  machine.process_event(evt->get_event());
  end_handle();
}
