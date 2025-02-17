/*
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

package accord.coordinate;

import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.messages.Accept;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

public class ProposeOnly extends Propose<Deps>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ProposeOnly.class);

    ProposeOnly(Node node, Topologies topologies, Route<?> sendTo, FullRoute<?> route, Accept.Kind kind, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Deps, Throwable> callback)
    {
        super(node, topologies, kind, ballot, txnId, txn, sendTo, route, executeAt, deps, callback);
    }

    @Override
    void onAccepted()
    {
        Deps deps = mergeDeps();
        callback.accept(deps, null);
    }

    @Override
    CoordinationAdapter<Deps> adapter()
    {
        throw new UnsupportedOperationException();
    }
}
