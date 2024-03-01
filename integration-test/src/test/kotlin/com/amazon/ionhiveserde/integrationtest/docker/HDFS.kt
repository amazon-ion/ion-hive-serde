// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.integrationtest.docker

/**
 * WARNING: This is a workaround!!!
 *
 * There is something wrong with the hadoop docker config that's blocking interacting with HDFS from outside the docker
 * container. To work around that we are using shell to send commands to docker to manipulate HDFS.
 *
 * See https://github.com/amazon-ion/ion-hive-serde/issues/37 for more info
 */
object HDFS {

    /**
     * Puts all data in path to HDFS /data directory
     *
     * @param path path relative to the shared directory
     */
    @JvmStatic
    fun put(path: String) {
        runInDocker("hadoop fs -mkdir -p /data")
        runInDocker("hadoop fs -put -f /$SHARED_DIR/$path /data")
    }

    /**
     * Removes data from HDFS
     *
     * @param path HDFS absolute path
     */
    @JvmStatic
    fun rm(path: String) = runInDocker("hadoop fs -rm -r -f $path")
}
