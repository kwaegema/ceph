#include "PGRegistry.h"
#include "ReplicatedPG.h"

#define dout_subsys ceph_subsys_osd

void ObjectContext::set_snaps(set<snapid_t> &new_snaps)
{
  dout(10) << obs.oi.soid << " snaps " << obs.oi.snaps
	   << " -> " << new_snaps << dendl;
  obs.oi.snaps = vector<snapid_t>(new_snaps.rbegin(), new_snaps.rend());
}

void ObjectContext::save_object_info(coll_t coll, ObjectStore::Transaction *t)
{
  bufferlist bl;
  ::encode(obs.oi, bl);
  t->setattr(coll, obs.oi.soid, OI_ATTR, bl);
}

void ObjectContext::save_snapset(coll_t coll, ObjectStore::Transaction *t)
{
  bufferlist bl;
  ::encode(obs.oi, bl);
  t->setattr(coll, obs.oi.soid, SS_ATTR, bl);
}

void ObjectContext::set_version(eversion_t version)
{
  obs.oi.prior_version = obs.oi.version;
  obs.oi.version = version;
}

void ObjectContext::watchers_erase(WatchRef watch)
{
  watchers.erase(make_pair(watch->get_cookie(), watch->get_entity()));
  obs.oi.watchers.erase(make_pair(watch->get_cookie(), watch->get_entity()));
}

void ObjectContext::no_longer_blocking()
{
  for (set<ObjectContextRef>::iterator j = blocking.begin();
       j != blocking.end();
       blocking.erase(j++)) {
    dout(10) << " no longer blocking writes for " << (*j)->obs.oi.soid << dendl;
    (*j)->blocked_by = ObjectContextRef();
  }
}

void ObjectContext::notify_ack_watchers(uint64_t notify_id, boost::optional<uint64_t> &watch_cookie, entity_name_t &entity)
{
  for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
         watchers.begin();
       i != watchers.end();
       ++i) {
    if (i->first.second != entity) continue;
    if (watch_cookie &&
        watch_cookie.get() != i->first.first) continue;
    dout(10) << "acking notify on watch " << i->first << dendl;
    i->second->notify_ack(notify_id);
  }
}

void ObjectContext::notify_watchers(NotifyRef notif)
{
  for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
         watchers.begin();
       i != watchers.end();
       ++i) {
    dout(10) << "starting notify on watch " << i->first << dendl;
    i->second->start_notify(notif);
  }
  notif->init();
}

void ObjectContext::remove_watcher(watch_info_t &watch_info, entity_name_t &entity)
{
  pair<uint64_t, entity_name_t> watcher(watch_info.cookie, entity);
  if (watchers.count(watcher)) {
    WatchRef watch = watchers[watcher];
    dout(10) << "remove_watcher applying disconnect found watcher "
             << watcher << dendl;
    watchers.erase(watcher);
    watch->remove();
  } else {
    dout(10) << "remove_watcher failed to find watcher "
             << watcher << dendl;
  }
}

WatchRef ObjectContext::get_or_create_watcher(watch_info_t &watch_info, entity_name_t &entity, const entity_addr_t &peer_addr, ReplicatedPG *pg, OSDService *osd)
{
  pair<uint64_t, entity_name_t> watcher(watch_info.cookie, entity);

  WatchRef watch;
  if (watchers.count(watcher)) {
    dout(15) << "get_or_create_watcher found existing watch watcher " << watcher
             << dendl;
    watch = watchers[watcher];
  } else {
    dout(15) << "get_or_create_watcher new watcher " << watcher
             << dendl;
    ObjectContextRef self(this);
    watch = Watch::makeWatchRef(
                                pg, osd, self, watch_info.timeout_seconds,
                                watch_info.cookie, entity, peer_addr);
    watchers.insert(
                         make_pair(
                                   watcher,
                                   watch));
  }

  return watch;
}

static void add_interval_usage(interval_set<uint64_t>& s, object_stat_sum_t& delta_stats) {
  for (interval_set<uint64_t>::const_iterator p = s.begin(); p != s.end(); ++p) {
    delta_stats.num_bytes += p.get_len();
  }
}

object_stat_sum_t ObjectContext::remove_clone_from_snapset(snapid_t last)
{
  SnapSet &snapset = ssc->get_snapset();

  vector<snapid_t>::iterator p;
  for (p = snapset.clones.begin(); p != snapset.clones.end(); ++p)
    if (*p == last)
      break;
  assert(p != snapset.clones.end());
  object_stat_sum_t delta;
  if (p != snapset.clones.begin()) {
    // not the oldest... merge overlap into next older clone
    vector<snapid_t>::iterator n = p - 1;
    interval_set<uint64_t> keep;
    keep.union_of(
                  snapset.clone_overlap[*n],
                  snapset.clone_overlap[*p]);
    add_interval_usage(keep, delta);  // not deallocated
    snapset.clone_overlap[*n].intersection_of(
                                              snapset.clone_overlap[*p]);
  } else {
    add_interval_usage(
                       snapset.clone_overlap[last],
                       delta);  // not deallocated
  }
  delta.num_objects--;
  delta.num_object_clones--;
  delta.num_bytes -= snapset.clone_size[last];

  snapset.clones.erase(p);
  snapset.clone_overlap.erase(last);
  snapset.clone_size.erase(last);

  return delta;
}

void PGRegistry::save_head(ReplicatedPG_OpContext *ctx, const hobject_t &soid, eversion_t old_version)
{
  // on the head object
  ctx->new_obs.oi.version = ctx->at_version;
  ctx->new_obs.oi.prior_version = old_version;
  ctx->new_obs.oi.last_reqid = ctx->reqid;
  if (ctx->mtime != utime_t()) {
    ctx->new_obs.oi.mtime = ctx->mtime;
    dout(10) << " set mtime to " << ctx->new_obs.oi.mtime << dendl;
  } else {
    dout(10) << " mtime unchanged at " << ctx->new_obs.oi.mtime << dendl;
  }

  bufferlist bv(sizeof(ctx->new_obs.oi));
  ::encode(ctx->new_obs.oi, bv);
  ctx->op_t.setattr(pg->coll, soid, OI_ATTR, bv);

  bufferlist bss;
  ::encode(ctx->new_snapset, bss);
  dout(10) << " final snapset " << ctx->new_snapset
           << " in " << soid << dendl;
  ctx->op_t.setattr(pg->coll, soid, SS_ATTR, bss);   
}

void PGRegistry::save_snapset(ReplicatedPG_OpContext *ctx, const hobject_t &soid)
{
  hobject_t snapoid(soid.oid, soid.get_key(), CEPH_SNAPDIR, soid.hash,
                    pg->info.pgid.pool(), soid.get_namespace());
  ctx->snapset_obc = get_object_context(snapoid, true);
  ctx->snapset_obc->obs.set_exists(true);
  ctx->snapset_obc->set_version(ctx->at_version);
  ctx->snapset_obc->obs.oi.last_reqid = ctx->reqid;
  ctx->snapset_obc->obs.oi.mtime = ctx->mtime;

  ctx->op_t.touch(pg->coll, snapoid);

  bufferlist bv(sizeof(ctx->snapset_obc->obs.oi));
  ::encode(ctx->snapset_obc->obs.oi, bv);
  ctx->op_t.setattr(pg->coll, snapoid, OI_ATTR, bv);

  bufferlist bss;
  ::encode(ctx->new_snapset, bss);
  ctx->op_t.setattr(pg->coll, snapoid, SS_ATTR, bss);

  ctx->at_version.version++;
}

int PGRegistry::find_object_context(const hobject_t& oid,
                                        ObjectContextRef &pobc,
                                        snapid_t *psnapid,
                                        bool with_ssc)
{
  int r = find_or_create_object_context(oid, pobc, false, psnapid);
  if (r == 0 && oid.snap != pobc->obs.oi.soid.snap)
    r = -ENOENT;
  if (r == 0 && with_ssc && !pobc->ssc)
    pobc->ssc = get_or_create_snapset_context(
      oid.oid,
      oid.get_key(),
      oid.hash,
      false,
      oid.get_namespace());
  return r;
}

int PGRegistry::find_or_create_object_context(const hobject_t& oid,
				      ObjectContextRef &pobc,
				      bool can_create,
				      snapid_t *psnapid)
{
  hobject_t head(oid.oid, oid.get_key(), CEPH_NOSNAP, oid.hash,
		 pg->info.pgid.pool(), oid.get_namespace());
  hobject_t snapdir(oid.oid, oid.get_key(), CEPH_SNAPDIR, oid.hash,
		    pg->info.pgid.pool(), oid.get_namespace());

  // want the snapdir?
  if (oid.snap == CEPH_SNAPDIR) {
    // return head or snapdir, whichever exists.
    ObjectContextRef obc = get_object_context(head, can_create);
    if (obc && !obc->obs.exists) {
      // ignore it if the obc exists but the object doesn't
      obc = ObjectContextRef();
    }
    if (!obc) {
      obc = get_object_context(snapdir, can_create);
    }
    if (!obc)
      return -ENOENT;
    dout(10) << "find_object_context " << oid << " @" << oid.snap << dendl;
    pobc = obc;

    // always populate ssc for SNAPDIR...
    if (!obc->ssc)
      obc->ssc = get_or_create_snapset_context(oid.oid, oid.get_key(), oid.hash, true, oid.get_namespace());
    return 0;
  }

  // want the head?
  if (oid.snap == CEPH_NOSNAP) {
    ObjectContextRef obc = get_object_context(head, can_create);
    if (!obc)
      return -ENOENT;
    dout(10) << "find_object_context " << oid << " @" << oid.snap << dendl;
    pobc = obc;

    if (can_create && !obc->ssc)
      obc->ssc = get_or_create_snapset_context(oid.oid, oid.get_key(), oid.hash, true, oid.get_namespace());

    return 0;
  }

  // we want a snap
  SnapSetContextRef ssc = get_or_create_snapset_context(oid.oid, oid.get_key(), oid.hash, can_create, oid.get_namespace());
  if (!ssc)
    return -ENOENT;

  dout(10) << "find_object_context " << oid << " @" << oid.snap
	   << " snapset " << ssc->snapset << dendl;
 
  // head?
  if (oid.snap > ssc->snapset.seq) {
    if (ssc->snapset.head_exists) {
      ObjectContextRef obc = get_object_context(head, false);
      dout(10) << "find_object_context  " << head
	       << " want " << oid.snap << " > snapset seq " << ssc->snapset.seq
	       << " -- HIT " << obc->obs
	       << dendl;
      if (!obc->ssc)
	obc->ssc = ssc;
      else
	assert(ssc == obc->ssc);
      pobc = obc;
      return 0;
    }
    dout(10) << "find_object_context  " << head
	     << " want " << oid.snap << " > snapset seq " << ssc->snapset.seq
	     << " but head dne -- DNE"
	     << dendl;
    return -ENOENT;
  }

  // which clone would it be?
  unsigned k = 0;
  while (k < ssc->snapset.clones.size() &&
	 ssc->snapset.clones[k] < oid.snap)
    k++;
  if (k == ssc->snapset.clones.size()) {
    dout(10) << "find_object_context  no clones with last >= oid.snap " << oid.snap << " -- DNE" << dendl;
    return -ENOENT;
  }
  hobject_t soid(oid.oid, oid.get_key(), ssc->snapset.clones[k], oid.hash,
		 pg->info.pgid.pool(), oid.get_namespace());

  ssc = SnapSetContextRef();

  if (pg->pg_log.get_missing().is_missing(soid)) {
    dout(20) << "find_object_context  " << soid << " missing, try again later" << dendl;
    if (psnapid)
      *psnapid = soid.snap;
    return -EAGAIN;
  }

  ObjectContextRef obc = get_object_context(soid, false);
  assert(obc);

  // clone
  dout(20) << "find_object_context  " << soid << " snaps " << obc->obs.oi.snaps << dendl;
  snapid_t first = obc->obs.oi.snaps[obc->obs.oi.snaps.size()-1];
  snapid_t last = obc->obs.oi.snaps[0];
  if (first <= oid.snap) {
    dout(20) << "find_object_context  " << soid << " [" << first << "," << last
	     << "] contains " << oid.snap << " -- HIT " << obc->obs << dendl;
    pobc = obc;
    return 0;
  } else {
    dout(20) << "find_object_context  " << soid << " [" << first << "," << last
	     << "] does not contain " << oid.snap << " -- DNE" << dendl;
    return -ENOENT;
  }
}

ObjectContextRef PGRegistry::get_object_context(const hobject_t& soid,
							      bool can_create)
{
  ObjectContextRef obc = objects.lookup(soid);
  if (obc) {
    dout(10) << "get_object_context " << (void*)obc.get() << " " << soid << dendl;
  } else {
    // check disk
    bufferlist bv;
    int r = osd->store->getattr(pg->coll, soid, OI_ATTR, bv);
    if (r < 0) {
      if (!can_create)
	return ObjectContextRef();   // -ENOENT!

      // new object.
      object_info_t oi(soid);
      SnapSetContextRef ssc = get_or_create_snapset_context(soid.oid, soid.get_key(), soid.hash, true, soid.get_namespace());
      return new_object_context(oi, ssc);
    }

    object_info_t oi(bv);

    assert(oi.soid.pool == (int64_t)pg->info.pgid.pool());

    SnapSetContextRef ssc;
    if (can_create)
      ssc = get_or_create_snapset_context(soid.oid, soid.get_key(), soid.hash, true, soid.get_namespace());
    obc = objects.lookup_or_create(soid);
    obc->ssc = ssc;
    obc->obs.oi = oi;
    obc->obs.exists = true;

    populate_obc_watchers(obc);
    dout(10) << "get_object_context " << obc << " " << soid << " 0 -> 1 read " << obc->obs.oi << dendl;
  }
  return obc;
}

ObjectContextRef PGRegistry::new_object_context(const object_info_t& oi,
                                                    SnapSetContextRef ssc)
{
  ObjectContextRef obc(new ObjectContext());
  obc->obs.oi = oi;
  obc->obs.exists = false;
  obc->ssc = ssc;
  dout(10) << "new_object_context " << obc << " " << oi.soid << " " << dendl;
  populate_obc_watchers(obc);
  return obc;
}

ObjectContextRef PGRegistry::lookup_or_create_object_context(const object_info_t& oi)
{
  ObjectContextRef obc = objects.lookup_or_create(oi.soid);
  obc->obs.oi = oi;
  return obc;
}

void PGRegistry::populate_obc_watchers(ObjectContextRef obc)
{
  assert(obc);
  assert(pg->is_active());
  assert(!pg->is_missing_object(obc->obs.oi.soid) ||
	 (pg->pg_log.get_log().objects.count(obc->obs.oi.soid) && // or this is a revert... see recover_primary()
	  pg->pg_log.get_log().objects.find(obc->obs.oi.soid)->second->op ==
	    pg_log_entry_t::LOST_REVERT &&
	  pg->pg_log.get_log().objects.find(obc->obs.oi.soid)->second->reverting_to ==
	    obc->obs.oi.version));

  dout(10) << "populate_obc_watchers " << obc->obs.oi.soid << dendl;
  assert(obc->watchers.empty());
  // populate unconnected_watchers
  for (map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator p =
	obc->obs.oi.watchers.begin();
       p != obc->obs.oi.watchers.end();
       ++p) {
    utime_t expire = pg->info.stats.last_became_active;
    expire += p->second.timeout_seconds;
    dout(10) << "  unconnected watcher " << p->first << " will expire " << expire << dendl;
    WatchRef watch(
      Watch::makeWatchRef(
        pg, osd, obc, p->second.timeout_seconds, p->first.first,
	p->first.second, p->second.addr));
    watch->disconnect();
    obc->watchers.insert(
      make_pair(
	make_pair(p->first.first, p->first.second),
	watch));
  }
  // Look for watchers from blacklisted clients and drop
  check_blacklisted_obc_watchers(obc);
}

void PGRegistry::check_blacklisted_obc_watchers(ObjectContextRef obc)
{
  dout(20) << "PGRegistry::check_blacklisted_obc_watchers for obc " << obc->obs.oi.soid << dendl;
  for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator k =
	 obc->watchers.begin();
	k != obc->watchers.end();
	) {
    //Advance iterator now so handle_watch_timeout() can erase element
    map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j = k++;
    dout(30) << "watch: Found " << j->second->get_entity() << " cookie " << j->second->get_cookie() << dendl;
    entity_addr_t ea = j->second->get_peer_addr();
    dout(30) << "watch: Check entity_addr_t " << ea << dendl;
    if (pg->get_osdmap()->is_blacklisted(ea)) {
      dout(10) << "watch: Found blacklisted watcher for " << ea << dendl;
      assert(j->second->get_pg() == pg);
      pg->handle_watch_timeout(j->second);
    }
  }
}

void PGRegistry::check_blacklisted_watchers()
{
  dout(20) << "PGRegistry::check_blacklisted_watchers for pg " << pg->get_pgid() << dendl;
  pair<hobject_t, ObjectContextRef> i;
  while (objects.get_next(i.first, &i)) {
    check_blacklisted_obc_watchers(i.second);
  }
  pg->check_wake();
}

void PGRegistry::get_watchers(list<obj_watch_item_t> &pg_watchers) const
{
  pair<hobject_t, ObjectContextRef> i;
  while (objects.get_next(i.first, &i)) {
    i.second->get_watchers(pg_watchers);
  }
  pg->check_wake();
}

void ObjectContext::get_watchers(list<obj_watch_item_t> &pg_watchers) const
{
  for (map<pair<uint64_t, entity_name_t>, WatchRef>::const_iterator j =
	 watchers.begin();
	j != watchers.end();
	++j) {
    obj_watch_item_t owi;

    owi.obj = obs.oi.soid;
    owi.wi.addr = j->second->get_peer_addr();
    owi.wi.name = j->second->get_entity();
    owi.wi.cookie = j->second->get_cookie();
    owi.wi.timeout_seconds = j->second->get_timeout();

    dout(30) << "watch: Found oid=" << owi.obj << " addr=" << owi.wi.addr
      << " name=" << owi.wi.name << " cookie=" << owi.wi.cookie << dendl;

    pg_watchers.push_back(owi);
  }
}

void PGRegistry::discard_watchers()
{
  pair<hobject_t, ObjectContextRef> i;
  while (objects.get_next(i.first, &i)) {
    for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j =
	   i.second->watchers.begin();
	 j != i.second->watchers.end();
	 i.second->watchers.erase(j++)) {
      j->second->discard();
    }
  }
}

void PGRegistry::add_object_context_to_pg_stat(ObjectContextRef obc, pg_stat_t *pgstat)
{
  object_info_t& oi = obc->obs.oi;

  dout(10) << "add_object_context_to_pg_stat " << oi.soid << dendl;
  object_stat_sum_t stat;

  stat.num_bytes += oi.size;

  if (oi.soid.snap != CEPH_SNAPDIR)
    stat.num_objects++;

  if (oi.soid.snap && oi.soid.snap != CEPH_NOSNAP && oi.soid.snap != CEPH_SNAPDIR) {
    stat.num_object_clones++;

    if (!obc->ssc)
      obc->ssc = get_or_create_snapset_context(oi.soid.oid,
				     oi.soid.get_key(),
				     oi.soid.hash,
				     false,
				     oi.soid.get_namespace());
    assert(obc->ssc);

    // subtract off clone overlap
    if (obc->ssc->snapset.clone_overlap.count(oi.soid.snap)) {
      interval_set<uint64_t>& o = obc->ssc->snapset.clone_overlap[oi.soid.snap];
      for (interval_set<uint64_t>::const_iterator r = o.begin();
	   r != o.end();
	   ++r) {
	stat.num_bytes -= r.get_len();
      }	  
    }
  }

  // add it in
  pgstat->stats.sum.add(stat);
  if (oi.category.length())
    pgstat->stats.cat_sum[oi.category].add(stat);
}

ObjectContextRef PGRegistry::mark_object_lost(ObjectStore::Transaction *t, const hobject_t &oid, eversion_t version)
{
  ObjectContextRef obc = get_object_context(oid, true);

  obc->ondisk_write_lock();

  obc->obs.oi.lost = true;
  obc->obs.oi.version = pg->info.last_update;
  obc->obs.oi.prior_version = version;

  bufferlist b2;
  obc->obs.oi.encode(b2);
  t->setattr(pg->coll, oid, OI_ATTR, b2);

  return obc;
}

SnapSetContextRef PGRegistry::create_snapset_context(const object_t &_oid, SnapSet &_snapset)
{
  SnapSetContextRef ssc = snapsets.lookup_or_create(_oid);
  ssc->oid = _oid;
  ssc->snapset = _snapset;
  dout(10) << "create_snapset_context " << ssc << " " << ssc->oid << dendl;
  return ssc;
}

SnapSetContextRef PGRegistry::create_snapset_context(const object_t& oid)
{
  SnapSetContextRef ssc = snapsets.lookup_or_create(oid);
  ssc->oid = oid;
  dout(10) << "create_snapset_context " << ssc << " " << ssc->oid << dendl;
  return ssc;
}

SnapSetContextRef PGRegistry::get_or_create_snapset_context(const object_t& oid,
						  const string& key,
						  ps_t seed,
						  bool can_create,
						  const string& nspace)
{
  SnapSetContextRef ssc = snapsets.lookup(oid);
  if (!ssc) {
    bufferlist bv;
    hobject_t head(oid, key, CEPH_NOSNAP, seed,
		   pg->info.pgid.pool(), nspace);
    int r = osd->store->getattr(pg->coll, head, SS_ATTR, bv);
    if (r < 0) {
      hobject_t snapdir(oid, key, CEPH_SNAPDIR, seed,
			pg->info.pgid.pool(), nspace);
      r = osd->store->getattr(pg->coll, snapdir, SS_ATTR, bv);
      if (r < 0 && !can_create)
	return SnapSetContextRef();
    }
    ssc = create_snapset_context(oid);
    if (r >= 0) {
      bufferlist::iterator bvp = bv.begin();
      ssc->snapset.decode(bvp);
    }
  }
  assert(ssc);
  dout(10) << "get_snapset_context " << ssc->oid << " "
	   << dendl;
  return ssc;
}

int PGRegistry::list_snaps(const hobject_t& soid, obj_list_snap_response_t& resp)
{
  int result = 0;

  hobject_t clone_oid = soid;
  ObjectContextRef clone_obc = objects.lookup(clone_oid);
  assert(clone_obc);
  SnapSetContextRef ssc = get_or_create_snapset_context(clone_oid.oid,
                                                        clone_oid.get_key(),
                                                        clone_oid.hash,
                                                        false,
                                                        clone_oid.get_namespace());
  assert(ssc);

  int clonecount = ssc->snapset.clones.size();
  if (ssc->snapset.head_exists)
    clonecount++;
  resp.clones.reserve(clonecount);
  for (vector<snapid_t>::const_iterator clone_iter = ssc->snapset.clones.begin();
       clone_iter != ssc->snapset.clones.end(); ++clone_iter) {
    clone_info ci;
    ci.cloneid = *clone_iter;
    clone_oid.snap = *clone_iter;
    for (vector<snapid_t>::reverse_iterator p = clone_obc->obs.oi.snaps.rbegin();
         p != clone_obc->obs.oi.snaps.rend();
         ++p) {
      ci.snaps.push_back(*p);
    }

    dout(20) << " clone " << *clone_iter << " snaps " << ci.snaps << dendl;

    map<snapid_t, interval_set<uint64_t> >::const_iterator coi;
    coi = ssc->snapset.clone_overlap.find(ci.cloneid);
    if (coi == ssc->snapset.clone_overlap.end()) {
      osd->clog.error() << "osd." << osd->whoami << ": inconsistent clone_overlap found for oid "
                        << clone_oid << " clone " << *clone_iter;
      result = -EINVAL;
      break;
    }
    const interval_set<uint64_t> &o = coi->second;
    ci.overlap.reserve(o.num_intervals());
    for (interval_set<uint64_t>::const_iterator r = o.begin();
         r != o.end(); ++r) {
      ci.overlap.push_back(pair<uint64_t,uint64_t>(r.get_start(), r.get_len()));
    }

    map<snapid_t, uint64_t>::const_iterator si;
    si = ssc->snapset.clone_size.find(ci.cloneid);
    if (si == ssc->snapset.clone_size.end()) {
      osd->clog.error() << "osd." << osd->whoami << ": inconsistent clone_size found for oid "
                        << clone_oid << " clone " << *clone_iter;
      result = -EINVAL;
      break;
    }
    ci.size = si->second;

    resp.clones.push_back(ci);
  }
  if (ssc->snapset.head_exists) {
    assert(clone_obc->obs.exists);
    clone_info ci;
    ci.cloneid = CEPH_NOSNAP;

    //Size for HEAD is oi.size
    ci.size = clone_obc->obs.oi.size;

    resp.clones.push_back(ci);
  }
  resp.seq = ssc->snapset.seq;

  return result;
}
