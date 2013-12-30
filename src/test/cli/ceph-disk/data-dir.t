  $ uuid=$(uuidgen)
  $ export CEPH_CONF=$TESTDIR/ceph.conf
  $ echo "[global]\nfsid = $uuid\nosd_data = $TESTDIR/osd-data\n" > $CEPH_CONF
  $ osd_data=$(ceph-conf osd_data)
  $ mkdir $osd_data
  $ ceph-disk --verbose prepare --data-dir $osd_data
  DEBUG:ceph-disk:Preparing osd data dir .* (re)
  $ ceph-disk --verbose prepare --data-dir $osd_data
  DEBUG:ceph-disk:Data dir .* already exists (re)
  $ rm -fr $osd_data
