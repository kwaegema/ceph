#!/bin/sh -e

BASE=/tmp/cephtest
TLIB=binary/usr/local/lib

echo "starting hadoop-internal-tests tests"

export LD_LIBRARY_PATH=$BASE/$TLIB 
#command="java -DCEPH_CONF_FILE=$BASE/ceph.conf -Djava.library.path=$LD_LIBRARY_PATH -cp /usr/share/java/junit4.jar:$BASE/$TLIB/libcephfs.jar:$BASE/$TLIB/libcephfs-test.jar org.junit.runner.JUnitCore com.ceph.fs.CephAllTests"
command1="cd $BASE/hadoop" 
command2="ant -Dextra.library.path=$BASE/$TLIB -Dceph.conf.file=$BASE/ceph.conf test -Dtestcase=TestCephFileSystem"

echo "----------------------"
echo $command1
echo "----------------------"
echo $command2
echo "----------------------"

$command1
$command2

echo "completed hadoop-internal-tests tests"

exit 0
