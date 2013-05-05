// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#include <stdio.h>
#include <signal.h>
#include "common/Mutex.h"
#include "common/Thread.h"
#include "common/ceph_argparse.h"
#include "osd/OSD.h"
#include "osd/ReplicatedPG.h"
#include "mon/MonClient.h"
#include "global/global_init.h"
#include <gtest/gtest.h>

class OSDTest : public OSDAbstract {
public:
  virtual void need_heartbeat_peer_update() {}
  virtual void dequeue_op(PGRef pg, OpRequestRef op) {}
  virtual void process_peering_events(
				      const list<PG*> &pg,
				      ThreadPool::TPHandle &handle) {}
  virtual void _share_map_outgoing(int peer, Connection *con,
				   OSDMapRef map = OSDMapRef()) {}
  virtual bool  _have_pg(pg_t pgid) { return false; }
  virtual PG   *_lookup_lock_pg(pg_t pgid) { return NULL; }
  virtual void pg_stat_queue_enqueue(PG *pg) {}
  virtual void pg_stat_queue_dequeue(PG *pg) {}
  virtual void clear_pg_stat_queue() {}

  virtual void start_recovery_op(PG *pg, const hobject_t& soid) {}
  virtual void finish_recovery_op(PG *pg, const hobject_t& soid, bool dequeue) {}
  virtual void defer_recovery(PG *pg) {}
  virtual void do_recovery(PG *pg) {}
  virtual bool _recover_now() { return false; }


  OSDTest(int id, Messenger *internal_messenger, Messenger *external_messenger, Messenger *hbclientm, MonClient *mc) : OSDAbstract(id, internal_messenger, external_messenger, hbclientm, mc) {}
  virtual ~OSDTest() {}

  virtual bool ms_dispatch(Message *m) { return true; }

  virtual bool ms_handle_reset(Connection *con) { return true; }
  virtual void ms_handle_remote_reset(Connection *con) {}
};

class ReplicatedPGTest : public ::testing::Test {

};

PG::RecoveryCtx create_context()
{
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  C_Contexts *on_applied = new C_Contexts(g_ceph_context);
  C_Contexts *on_safe = new C_Contexts(g_ceph_context);
  map< int, map<pg_t,pg_query_t> > *query_map =
    new map<int, map<pg_t, pg_query_t> >;
  map<int,vector<pair<pg_notify_t, pg_interval_map_t> > > *notify_list =
    new map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >;
  map<int,vector<pair<pg_notify_t, pg_interval_map_t> > > *info_map =
    new map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >;
  PG::RecoveryCtx rctx(query_map, info_map, notify_list,
		       on_applied, on_safe, t);
  return rctx;
}

TEST_F(ReplicatedPGTest, ReplicatedPG) {
  int whoami = 123;
  MonClient mc(g_ceph_context);

  Messenger *client_messenger = Messenger::create(g_ceph_context,
						  entity_name_t::OSD(whoami), "client",
						  getpid());
  Messenger *cluster_messenger = Messenger::create(g_ceph_context,
						   entity_name_t::OSD(whoami), "cluster",
						   getpid());
  Messenger *messenger_hbclient = Messenger::create(g_ceph_context,
						    entity_name_t::OSD(whoami), "hbclient",
						    getpid());
  OSDTest *osd = new OSDTest(whoami, cluster_messenger, client_messenger,
			     messenger_hbclient,
			     &mc);

  // inspired by make_pg
  pg_t pgid;
  OSDMap *osdmap = new OSDMap();
  OSDMapRef osdmap_ref(osdmap);
  epoch_t e;
  uuid_d fsid;
  int num_osd = 1;
  int pg_bits = 0;
  int pgp_bits = 0;
  osdmap->build_simple(g_ceph_context, e, fsid, num_osd, pg_bits, pgp_bits);
  PGPool pool(0, "", 0);
  pool.update(osdmap_ref);
  ReplicatedPG *pg = new ReplicatedPG(&osd->service, osdmap_ref, pool, pgid, OSD::make_pg_log_oid(pgid), OSD::make_pg_biginfo_oid(pgid));
  pg->lock();

  {
    pg_history_t history;
    epoch_t created = 1;
    history.epoch_created = created;
    history.last_epoch_clean = created;
    history.same_interval_since = created;
    history.same_up_since = created;
    history.same_primary_since = created;
    
    PG::RecoveryCtx rctx = create_context();

    int role = 0; // primary
    pg_interval_map_t pi;
    vector<int> up(1);
    up[0] = 1;
    vector<int> acting = up;
    pg->init(role, up, acting, history, pi, rctx.transaction);
    {
      const PG::RecoveryState::Initial* state = pg->recovery_state.machine.state_cast<const PG::RecoveryState::Initial *>();
      EXPECT_EQ("Initial", std::string(state->get_state_name()));
    }
    PG::Initialize evt;
    pg->recovery_state.handle_event(evt, &rctx);
    {
      const PG::RecoveryState::Reset* state = pg->recovery_state.machine.state_cast<const PG::RecoveryState::Reset *>();
      EXPECT_EQ("Reset", std::string(state->get_state_name()));
    }
  }

  pg->unlock();
  delete pg;
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// Local Variables:
// compile-command: "cd ../.. ; make unittest_replicated_pg ; ./unittest_replicated_pg # --gtest_filter=ReplicatedPGTest.* --log-to-stderr=true  --debug-osd=20"
// End:
