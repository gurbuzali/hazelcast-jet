/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kinesis;

/**
 * JAVADOC
 */
public class ToDo {

    //todo: merges and splits can reorder in-flight messages, document this
    //todo: offer option for single instance sources to fix reordering; test this

    //todo: Use ranges only for initial shard distribution, handle splits and merges internally;
    // splits: processor handles all children, merge: processor that handled parent handles it,
    // decide based on two parents

    //todo: test source when stream doesn't exist
    //todo: test stream when shard count is more than 100

    //todo: saved shard offsets for recovery, maybe as shard id -- offset last seen map?
    // what happens after split/merge, should we delete the offset of CLOSED shards?

    //todo: clarify what license to use
    //todo: handle Kinesis quotas: https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
    //todo: delete this class

    //todo: test that does random shard splits & merges mixed with data updates and checks that all data is processed

    //todo: @since tags
    //todo: @Nonnull/@Nullable annotations
    //todo: javadoc

    //todo: make sure all integration tests run on real backend too

    //todo: sink could rely not only on throughput exceptions, but actively track shards

    //todo: is one AmazonKinesisAsync per Jet member enough? how does performance look like?

    //todo: update source/sink docs
    //todo: tutorial

    //todo: preserve order during merges and splits via a generic Jet mechanism which we could use to detect when items
    // from closed shards have finished traversing the pipeline; this same mechanism could be used to replace the current
    // what snapshotting is being done too
}
