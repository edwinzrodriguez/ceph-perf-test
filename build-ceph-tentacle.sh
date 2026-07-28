#!/bin/bash
set -e

for i in ceph-bench-tentacle-base ceph-bench-tentacle-osdc ceph-bench-tentacle-client-lock; do
    if [ ! -e ~/git/$i ]; then
        pushd ~/git/ceph-tentacle
        git worktree add ~/git/$i
        popd
    fi
    pushd ~/git/$i
        git switch $i
        git submodule update --init --recursive
        if [ ! -e ~/git/$i/build ]; then
            ./do_cmake.sh -DUSE_TRACEFLOW=OFF \
            -DCMAKE_BUILD_TYPE=RelWithDebInfo \
            -DCMAKE_INSTALL_PREFIX=/usr/local/$i
        else
          pushd ~/git/$i/build
              cmake .. -DUSE_TRACEFLOW=OFF \
                -DCMAKE_BUILD_TYPE=RelWithDebInfo \
                -DCMAKE_INSTALL_PREFIX=/usr/local/$i
          popd
        fi
        pushd ~/git/$i/build
            ninja install
        popd
    popd
    
    pushd ~/git/nfs-ganesha
    git pull
    popd
    mkdir -pv ~/git/ganesha-build-$i
    pushd ~/git/ganesha-build-$i
    cmake ../nfs-ganesha/src/ -GNinja \
      -DUSE_TRACEFLOW=OFF \
      -DUSE_FSAL_CEPH=true \
      -DCEPHFS_INCLUDE_DIR=/usr/local/$i/include \
      -DCEPHFS_LIBRARY_DIR=/usr/local/$i/lib64 \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DCMAKE_INSTALL_PREFIX=/usr/local/$i
    ninja install
    popd
    mkdir -pv /usr/local/$i/var/lib/nfs/ganesha

    pushd ~/git
      git clone https://github.com/axboe/fio.git fio-$i
    popd
    pushd ~/git/fio-$i
      git checkout fio-3.36
      git submodule update --init --recursive
      ./configure --prefix=/usr/local/$i
      make install
    popd

done

