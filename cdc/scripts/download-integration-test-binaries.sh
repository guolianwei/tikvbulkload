#!/usr/bin/env bash
# Copyright 20201 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

# download-integration-test-binaries.sh will
# * download all the binaries you need for integration testing

# Notice:
# Please don't try the script locally,
# it downloads files for linux platform. We only use it in docker-compose.

set -o errexit
set -o nounset
set -o pipefail

# See https://misc.flogisoft.com/bash/tip_colors_and_formatting.
color-green() { # Green
	echo -e "\x1B[1;32m${*}\x1B[0m"
}

# Specify the download branch.
branch=$1

# PingCAP file server URL.
file_server_url="http://fileserver.pingcap.net"

# Get sha1 based on branch name.
tidb_sha1=$(curl "${file_server_url}/download/refs/pingcap/tidb/${branch}/sha1")
tikv_sha1=$(curl "${file_server_url}/download/refs/pingcap/tikv/${branch}/sha1")
pd_sha1=$(curl "${file_server_url}/download/refs/pingcap/pd/${branch}/sha1")

# All download links.
tidb_download_url="${file_server_url}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz"
tikv_download_url="${file_server_url}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz"
pd_download_url="${file_server_url}/download/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz"
go_ycsb_download_url="${file_server_url}/download/builds/pingcap/kvass/go-ycsb"
etcd_download_url="${file_server_url}/download/builds/pingcap/cdc/etcd-v3.4.7-linux-amd64.tar.gz"

# Some temporary dir.
rm -rf tmp
rm -rf third_bin
rm -rf bin

mkdir -p third_bin
mkdir -p tmp
mkdir -p bin

color-green "Download binaries..."
curl "${tidb_download_url}" | tar xz -C tmp bin/tidb-server
curl "${tikv_download_url}" | tar xz -C tmp bin/tikv-server
curl "${pd_download_url}" | tar xz --wildcards -C tmp bin/*
mv tmp/bin/* third_bin

curl "${go_ycsb_download_url}" -o third_bin/go-ycsb
curl -L "${etcd_download_url}" | tar xz -C tmp
mv tmp/etcd-v3.4.7-linux-amd64/etcdctl third_bin
chmod a+x third_bin/*

# Copy it to the bin directory in the root directory.
rm -rf tmp
mv third_bin/* ./bin
rm -rf third_bin

color-green "Download SUCCESS"
