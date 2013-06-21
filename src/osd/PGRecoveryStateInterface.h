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

#ifndef CEPH_PGRECOVERYSTATEINTERFACE_H
#define CEPH_PGRECOVERYSTATEINTERFACE_H

#include "osd_types.h"
#include "OSDMap.h"
#include "os/ObjectStore.h"

class OSDService;
class PGLog;
class PGPool;
class PGScrubber;

namespace PGRecoveryState {

  class CephPeeringEvt;
  class PriorSet;

  typedef std::tr1::shared_ptr<CephPeeringEvt> CephPeeringEvtRef;

  class PGRecoveryStateInterface {
  protected:
    PGRecoveryStateInterface() {}
    ~PGRecoveryStateInterface() {}
  public:

    virtual const pg_info_t &get_info() const = 0;
    virtual bool should_send_notify() const = 0;
    virtual void set_send_notify() = 0;
    virtual bool proc_replica_info(int from, const pg_info_t &info) = 0;
    virtual void update_heartbeat_peers() = 0;
    virtual void set_last_peering_reset() = 0;
    virtual bool is_primary() const = 0;
  virtual void start_flush(ObjectStore::Transaction *t,
		   list<Context *> *on_applied,
		   list<Context *> *on_safe) = 0;
    virtual bool get_flushed() const = 0;
    virtual void set_flushed(bool _flushed) = 0;
    virtual void requeue_waiting_for_active() = 0;
  virtual std::string gen_prefix() const = 0;
  virtual bool acting_up_affected(const vector<int>& newup, const vector<int>& newacting) = 0;
  virtual bool is_split(OSDMapRef lastmap, OSDMapRef nextmap) = 0;
  virtual void remove_down_peer_info(const OSDMapRef osdmap) = 0;
  virtual void on_flushed() = 0;
  virtual void generate_past_intervals() = 0;
  virtual void start_peering_interval(const OSDMapRef lastmap,
			      const vector<int>& newup,
			      const vector<int>& newacting) = 0;
    virtual int        get_primary() = 0;
    virtual OSDMapRef get_osdmap() const = 0;
    virtual const map<epoch_t,pg_interval_t> get_past_intervals() const = 0;
  virtual void take_waiters() = 0;
    virtual const vector<int> &get_want_acting() const = 0;
    virtual const vector<int> &get_acting() const = 0;
    virtual const vector<int> &get_up() const = 0;
    virtual void clear_want_acting() = 0;
  virtual bool choose_acting(int& newest_update_osd) = 0;

    virtual const map<int,pg_info_t>  &get_peer_info() = 0;
    virtual void publish_stats_to_osd() = 0;
    virtual OSDService *get_osd() = 0;
    virtual bool get_backfill_reserved() const = 0;
    virtual void set_backfill_reserved(bool _backfill_reserved) = 0;
    virtual bool get_backfill_reserving() const = 0;
    virtual void set_backfill_reserving(bool _backfill_reserving) = 0;
    virtual void state_set(int m) = 0;
    virtual void state_clear(int m) = 0;
  virtual void get(const string &tag) = 0;
  virtual void put(const string &tag) = 0;
  virtual void lock(bool no_lockdep = false) = 0;
    virtual void unlock() = 0;
  virtual void queue_peering_event(CephPeeringEvtRef evt) = 0;
  virtual void schedule_backfill_full_retry() = 0;
    virtual int get_backfill_target() const = 0;
    virtual bool       is_degraded() const = 0;
    virtual bool       is_clean() const = 0;
    virtual bool       is_active() const = 0;
    virtual bool       is_peering() const = 0;

  virtual void reject_reservation() = 0;
    virtual const PGLog &get_pg_log() const = 0;
    virtual bool needs_recovery() const = 0;
    virtual ostream& operator<<(ostream& out) const = 0;
    virtual void finish_recovery(list<Context*>& tfin) = 0;
  virtual void mark_clean() = 0;
  virtual void share_pg_info() = 0;
  virtual void activate(ObjectStore::Transaction& t,
		epoch_t query_epoch,
		list<Context*>& tfin,
		map< int, map<pg_t,pg_query_t> >& query_map,
		map<int, vector<pair<pg_notify_t, pg_interval_map_t> > > *activator_map=0) = 0;
    virtual const PGPool &get_pool() const = 0;
    virtual interval_set<snapid_t> get_snap_trimq() = 0;
    virtual void set_dirty_info(bool _dirty_info) = 0;
    virtual void set_dirty_big_info(bool _dirty_big_info) = 0;
    virtual bool have_unfound() const = 0; 
  virtual void discover_all_missing(std::map< int, map<pg_t,pg_query_t> > &query_map) = 0;
  virtual bool check_log_for_corruption(ObjectStore *store) = 0;
    virtual const map<hobject_t, set<int> > &get_missing_loc() const = 0;
  virtual bool all_unfound_are_queried_or_lost(const OSDMapRef osdmap) const = 0;

  virtual void queue_snap_trim() = 0;
    virtual const set<int> &get_peer_purged() const = 0;
    virtual bool is_acting(int osd) const  = 0;
    virtual set<int> &get_peer_activated() = 0;
  virtual void all_activated_and_committed() = 0;
  virtual bool search_for_missing(const pg_info_t &oinfo, const pg_missing_t *omissing,
			  int fromosd) = 0;
    virtual const set<int> &get_might_have_unfound() const = 0;
    virtual const map<int,pg_missing_t> &get_peer_missing() const = 0;
    virtual const set<int> &get_peer_log_requested() const = 0;
  virtual void dump_recovery_info(Formatter *f) const = 0;
    virtual const PGScrubber &get_scrubber() const = 0;
  virtual void proc_primary_info(ObjectStore::Transaction &t, const pg_info_t &info) = 0;
  virtual void merge_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog, int from) = 0;
  virtual void update_history_from_master(pg_history_t new_history) = 0;
  virtual void fulfill_log(int from, const pg_query_t &query, epoch_t query_epoch) = 0;

  virtual void unreg_next_scrub() = 0;
    virtual void set_info(const pg_info_t& _info) = 0;
  virtual void reg_next_scrub() = 0;
    virtual void claim_log(const pg_log_t& o) = 0;
    virtual void reset_backfill() = 0;
  virtual void rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead) = 0;
  virtual void fulfill_info(int from, const pg_query_t &query, 
		    pair<int, pg_info_t> &notify_info) = 0;
    virtual const set<int> &get_peer_missing_requested() const = 0;
    virtual void set_peer_missing(int peer) = 0;
    virtual pg_missing_t &get_peer_missing(int peer) = 0;

    virtual void set_info_stats(const pg_stat_t &stats) = 0;
  virtual bool adjust_need_up_thru(const OSDMapRef osdmap) = 0;
  virtual void build_prior(std::auto_ptr<PriorSet> &prior_set) = 0;
  virtual void clear_probe_targets() = 0;
  virtual void proc_master_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog,
		       pg_missing_t& omissing, int from) = 0;
    virtual bool get_need_up_thru() const = 0;
  virtual void proc_replica_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog,
			pg_missing_t& omissing, int from) = 0;

  };

  typedef boost::intrusive_ptr<PGRecoveryStateInterface> PGRecoveryStateInterfaceRef;

  inline void intrusive_ptr_add_ref(PGRecoveryStateInterface *pg) {
    pg->get("intptr");
  }

  inline void intrusive_ptr_release(PGRecoveryStateInterface *pg) {
    pg->put("intptr");
  }

  inline ostream& operator<<(ostream& out, const PGRecoveryStateInterface& pg) {
    return pg.operator<<(out);
  }
  
}

#endif // CEPH_PGRECOVERYSTATEINTERFACE_H
