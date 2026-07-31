#!/bin/bash
set -e

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 <branch> [branch ...]" >&2
    echo "Example: $0 ceph-bench-tentacle-base ceph-bench-tentacle-osdc" >&2
    exit 1
fi

for i in "$@"; do
    if [ ! -e ~/git/$i ]; then
        pushd ~/git/ceph-tentacle
        git worktree add ~/git/$i -b $i origin/$i
        popd
    fi
    pushd ~/git/$i
        git checkout origin/$i
        git submodule update --init --recursive
        if [ ! -e ~/git/$i/build ]; then
            ./do_cmake.sh -DUSE_TRACEFLOW=OFF \
            -DCMAKE_BUILD_TYPE=RelWithDebInfo \
            -DCMAKE_INSTALL_PREFIX=/usr/local \
            -DENABLE_GIT_VERSION=OFF
        else
          pushd ~/git/$i/build
              cmake .. -DUSE_TRACEFLOW=OFF \
                -DCMAKE_BUILD_TYPE=RelWithDebInfo \
                -DCMAKE_INSTALL_PREFIX=/usr/local \
                -DENABLE_GIT_VERSION=OFF
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
      -DCEPHFS_INCLUDE_DIR=/usr/local/include \
      -DCEPHFS_LIBRARY_DIR=/usr/local/lib64 \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DCMAKE_INSTALL_PREFIX=/usr/local
    ninja install
    popd
    mkdir -pv /usr/local/var/lib/nfs/ganesha

    if [ ! -e ~/git/fio-$i ]; then
      pushd ~/git
        git clone https://github.com/axboe/fio.git fio-$i
      popd
    fi
    pushd ~/git/fio-$i
      git checkout fio-3.36
      git submodule update --init --recursive
      ./configure --prefix=/usr/local --extra-cflags="-I/usr/local/include -L/usr/local/lib64"
      make install
    popd

done
