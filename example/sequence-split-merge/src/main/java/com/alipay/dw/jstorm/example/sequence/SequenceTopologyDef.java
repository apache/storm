/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.dw.jstorm.example.sequence;


public class SequenceTopologyDef {
    
    public static final long   MAX_MESSAGE_COUNT   = 100000;
    
    public static final String SEQUENCE_SPOUT_NAME = "SequenceSpout";
    
    public static final String SPLIT_BOLT_NAME     = "Split";
    
    public static final String TRADE_BOLT_NAME     = "Trade";
    
    public static final String CUSTOMER_BOLT_NAME  = "Customer";
    
    public static final String MERGE_BOLT_NAME     = "Merge";
    
    public static final String TOTAL_BOLT_NAME     = "Total";
    
    public static final String TRADE_STREAM_ID     = "trade_stream";
    
    public static final String CUSTOMER_STREAM_ID  = "customer_stream";
    
}
