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
#ifndef CEPH_PG_LOG_H
#define CEPH_PG_LOG_H

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

#include "OpRequest.h"
#include "OSDMap.h"
#include "os/ObjectStore.h"
#include "msg/Messenger.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDPGLog.h"
#include "common/tracked_int_ptr.hpp"

#include <list>
#include <memory>
#include <string>
using namespace std;

#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;

class PG;
class OSD;
class OSDService;
class MOSDOp;
class MOSDSubOp;
class MOSDSubOpReply;
class MOSDPGScan;
class MOSDPGBackfill;
class MOSDPGInfo;

struct PGLog {
  /* Exceptions */
  class read_log_error : public buffer::error {
  public:
    explicit read_log_error(const char *what) {
      snprintf(buf, sizeof(buf), "read_log_error: %s", what);
    }
    const char *what() const throw () {
      return buf;
    }
  private:
    char buf[512];
  };

  /**
   * IndexLog - adds in-memory index of the log, by oid.
   * plus some methods to manipulate it all.
   */
  struct IndexedLog : public pg_log_t {
    hash_map<hobject_t,pg_log_entry_t*> objects;  // ptrs into log.  be careful!
    hash_map<osd_reqid_t,pg_log_entry_t*> caller_ops;

    // recovery pointers
    list<pg_log_entry_t>::iterator complete_to;  // not inclusive of referenced item
    version_t last_requested;           // last object requested by primary

    /****/
    IndexedLog() : last_requested(0) {}

    void claim_log(const pg_log_t& o) {
      log = o.log;
      head = o.head;
      tail = o.tail;
      index();
    }

    void split_into(
      pg_t child_pgid,
      unsigned split_bits,
      IndexedLog *olog);

    void zero() {
      unindex();
      pg_log_t::clear();
      reset_recovery_pointers();
    }
    void reset_recovery_pointers() {
      complete_to = log.end();
      last_requested = 0;
    }

    bool logged_object(const hobject_t& oid) const {
      return objects.count(oid);
    }
    bool logged_req(const osd_reqid_t &r) const {
      return caller_ops.count(r);
    }
    eversion_t get_request_version(const osd_reqid_t &r) const {
      hash_map<osd_reqid_t,pg_log_entry_t*>::const_iterator p = caller_ops.find(r);
      if (p == caller_ops.end())
	return eversion_t();
      return p->second->version;    
    }

    void index() {
      objects.clear();
      caller_ops.clear();
      for (list<pg_log_entry_t>::iterator i = log.begin();
           i != log.end();
           ++i) {
        objects[i->soid] = &(*i);
	if (i->reqid_is_indexed()) {
	  //assert(caller_ops.count(i->reqid) == 0);  // divergent merge_log indexes new before unindexing old
	  caller_ops[i->reqid] = &(*i);
	}
      }
    }

    void index(pg_log_entry_t& e) {
      if (objects.count(e.soid) == 0 || 
          objects[e.soid]->version < e.version)
        objects[e.soid] = &e;
      if (e.reqid_is_indexed()) {
	//assert(caller_ops.count(i->reqid) == 0);  // divergent merge_log indexes new before unindexing old
	caller_ops[e.reqid] = &e;
      }
    }
    void unindex() {
      objects.clear();
      caller_ops.clear();
    }
    void unindex(pg_log_entry_t& e) {
      // NOTE: this only works if we remove from the _tail_ of the log!
      if (objects.count(e.soid) && objects[e.soid]->version == e.version)
        objects.erase(e.soid);
      if (e.reqid_is_indexed() &&
	  caller_ops.count(e.reqid) &&  // divergent merge_log indexes new before unindexing old
	  caller_ops[e.reqid] == &e)
	caller_ops.erase(e.reqid);
    }


    // accessors
    pg_log_entry_t *is_updated(const hobject_t& oid) {
      if (objects.count(oid) && objects[oid]->is_update()) return objects[oid];
      return 0;
    }
    pg_log_entry_t *is_deleted(const hobject_t& oid) {
      if (objects.count(oid) && objects[oid]->is_delete()) return objects[oid];
      return 0;
    }
    
    // actors
    void add(pg_log_entry_t& e) {
      // add to log
      log.push_back(e);
      assert(e.version > head);
      assert(head.version == 0 || e.version.version > head.version);
      head = e.version;

      // to our index
      objects[e.soid] = &(log.back());
      caller_ops[e.reqid] = &(log.back());
    }

    void trim(ObjectStore::Transaction &t, hobject_t& oid, eversion_t s);

    ostream& print(ostream& out) const;
  };
  
  /**
   * OndiskLog - some info about how we store the log on disk.
   */
  class OndiskLog {
  public:
    // ok
    uint64_t tail;                     // first byte of log. 
    uint64_t head;                     // byte following end of log.
    uint64_t zero_to;                // first non-zeroed byte of log.
    bool has_checksums;

    /**
     * We reconstruct the missing set by comparing the recorded log against
     * the objects in the pg collection.  Unfortunately, it's possible to
     * have an object in the missing set which is not in the log due to
     * a divergent operation with a prior_version pointing before the
     * pg log tail.  To deal with this, we store alongside the log a mapping
     * of divergent priors to be checked along with the log during read_state.
     */
    map<eversion_t, hobject_t> divergent_priors;
    void add_divergent_prior(eversion_t version, hobject_t obj) {
      divergent_priors.insert(make_pair(version, obj));
    }

    OndiskLog() : tail(0), head(0), zero_to(0),
		  has_checksums(true) {}

    uint64_t length() const { return head - tail; }
    bool trim_to(eversion_t v, ObjectStore::Transaction& t);

    void zero() {
      tail = 0;
      head = 0;
      zero_to = 0;
    }

    void encode(bufferlist& bl) const {
      ENCODE_START(5, 3, bl);
      ::encode(tail, bl);
      ::encode(head, bl);
      ::encode(zero_to, bl);
      ::encode(divergent_priors, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& bl) {
      DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
      has_checksums = (struct_v >= 2);
      ::decode(tail, bl);
      ::decode(head, bl);
      if (struct_v >= 4)
	::decode(zero_to, bl);
      else
	zero_to = 0;
      if (struct_v >= 5)
	::decode(divergent_priors, bl);
      DECODE_FINISH(bl);
    }
    void dump(Formatter *f) const {
      f->dump_unsigned("head", head);
      f->dump_unsigned("tail", tail);
      f->dump_unsigned("zero_to", zero_to);
      f->open_array_section("divergent_priors");
      for (map<eversion_t, hobject_t>::const_iterator p = divergent_priors.begin();
	   p != divergent_priors.end();
	   ++p) {
	f->open_object_section("prior");
	f->dump_stream("version") << p->first;
	f->dump_stream("object") << p->second;
	f->close_section();
      }
      f->close_section();
    }
    static void generate_test_instances(list<OndiskLog*>& o) {
      o.push_back(new OndiskLog);
      o.push_back(new OndiskLog);
      o.back()->tail = 2;
      o.back()->head = 3;
      o.back()->zero_to = 1;
    }
  };
  WRITE_CLASS_ENCODER(OndiskLog)

protected:
  OndiskLog   ondisklog;
  pg_missing_t     missing;
  IndexedLog  log;

public:
  const pg_missing_t& get_missing() const { return missing; }

  void reset_recovery_pointers() { log.reset_recovery_pointers(); }

  const eversion_t &get_tail() const { return log.tail; }
  const eversion_t &get_head() const { return log.head; }
  const IndexedLog &get_log() const { return log; }
  const OndiskLog &get_ondisklog() const { return ondisklog; }

  void set_last_requested(version_t last_requested) {
    log.last_requested = last_requested;
  }
  void claim_log(const pg_log_t& o) {
    log.claim_log(o);
    missing.clear();
  }

  void missing_add(const hobject_t& oid, eversion_t need, eversion_t have) {
    missing.add(oid, need, have);
  }

  void add(pg_log_entry_t& e) { log.add(e); }

  void split_into(
      pg_t child_pgid,
      unsigned split_bits,
      PGLog *opg_log) { 
    log.split_into(child_pgid, split_bits, &(opg_log->log));
    missing.split_into(child_pgid, split_bits, &(opg_log->missing));
  }

  void activate_not_complete(pg_info_t &info) {
    log.complete_to = log.log.begin();
    while (log.complete_to->version <
	   missing.missing[missing.rmissing.begin()->second].need)
      log.complete_to++;
    assert(log.complete_to != log.log.end());
    if (log.complete_to == log.log.begin()) {
      info.last_complete = eversion_t();
    } else {
      log.complete_to--;
      info.last_complete = log.complete_to->version;
      log.complete_to++;
    }
    log.last_requested = 0;
  }

  void proc_replica_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog,
			pg_missing_t& omissing, int from);
  bool merge_old_entry(ObjectStore::Transaction& t, pg_log_entry_t& oe, pg_info_t& info, list<hobject_t> &remove_snap_mapped_objects, bool &dirty_log);
  void merge_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog, int from,
                      pg_info_t &info, list<hobject_t> &remove_snap_mapped_objects,
                      bool &dirty_log, bool &dirty_info, bool &dirty_big_info);
  void rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead,
                            pg_info_t &info, list<hobject_t> &remove_snap_mapped_objects,
                            bool &dirty_log, bool &dirty_info, bool &dirty_big_info);

  static void clear_info_log(
    pg_t pgid,
    const hobject_t &infos_oid,
    const hobject_t &log_oid,
    ObjectStore::Transaction *t);

  void write_log(ObjectStore::Transaction& t, const hobject_t &log_oid) {
    _write_log(t, log, log_oid, ondisklog.divergent_priors);
  }

protected:
  static void _write_log(ObjectStore::Transaction& t, pg_log_t &log,
    const hobject_t &log_oid, map<eversion_t, hobject_t> &divergent_priors);
public:

  bool read_log(ObjectStore *store, coll_t coll, hobject_t log_oid,
		const pg_info_t &info, ostringstream &oss) {
    return _read_log(store, coll, log_oid, info, ondisklog, log, missing, oss);
  }

protected:
  /// return true if the log should be rewritten
  static bool _read_log(ObjectStore *store, coll_t coll, hobject_t log_oid,
    const pg_info_t &info, OndiskLog &ondisklog, IndexedLog &log,
    pg_missing_t &missing, ostringstream &oss);
  static void read_log_old(ObjectStore *store, coll_t coll, hobject_t log_oid,
    const pg_info_t &info, OndiskLog &ondisklog, IndexedLog &log,
    pg_missing_t &missing, ostringstream &oss);
public:
  void trim(ObjectStore::Transaction& t, eversion_t trim_to, pg_info_t &info, hobject_t &log_oid);
};
  
WRITE_CLASS_ENCODER(PGLog::OndiskLog)

#endif // CEPH_PG_LOG_H
