#!/bin/bash
set -ex

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 <branch> [branch ...]" >&2
    echo "Example: $0 ceph-bench-tentacle-base ceph-bench-tentacle-osdc" >&2
    exit 1
fi

if [ ! -d ~/git/cephfs-mdsbench-src ]; then
    print "~/git/cephfs-mdsbench-src" not found
    exit 1
fi
if [ ! -e ~/git/cephfs-mdsbench-src/cephfs-mdsbench.cc ]; then
    print "~/git/cephfs-mdsbench-src/cephfs-mdsbench.cc" not found
    exit 1
fi

for i in "$@"; do

    pushd ~/git/cephfs-mdsbench-src
      g++ --std=c++20 -D_FILE_OFFSET_BITS=64 -O3 \
        -o /usr/local/$i/bin/cephfs-mdsbench \
        cephfs-mdsbench.cc \
        -I/root/git/$i/src \
        -I/usr/local/$i/include \
        -L/usr/local/$i/lib64 \
        -L/usr/local/$i/lib64/ceph \
        -Wl,-rpath,/usr/local/$i/lib64 \
        -Wl,-rpath,/usr/local/$i/lib64/ceph \
        -lcephfs -l:libceph-common.so.2 -lpthread -lboost_program_options
    popd

done
