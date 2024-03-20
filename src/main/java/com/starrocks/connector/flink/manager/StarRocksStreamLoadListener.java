/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.manager;

import com.starrocks.connector.flink.plugins.RedisPlugin;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.v2.StreamLoadListener;

public class StarRocksStreamLoadListener implements StreamLoadListener {
    private RedisPlugin redisPlugin;
    @Override
    public void onResponse(StreamLoadResponse response, String tableId) {
        if(response.getException()==null&&this.redisPlugin!=null){
            this.redisPlugin.collectMetrics(tableId,response.getFlushRows(),response.getFlushBytes());
        }
    }
    public StarRocksStreamLoadListener(RedisPlugin redisPlugin) {
        this.redisPlugin = redisPlugin;
    }
}
