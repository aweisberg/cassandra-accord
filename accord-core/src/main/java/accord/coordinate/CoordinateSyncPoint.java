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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.messages.PreAccept.PreAcceptOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncResult;
import javax.annotation.Nullable;

import static accord.coordinate.Propose.Invalidate.proposeAndCommitInvalidate;
import static accord.primitives.Timestamp.mergeMax;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.utils.Functions.foldl;
import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.checkState;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class CoordinateSyncPoint extends CoordinatePreAccept<SyncPoint>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CoordinateSyncPoint.class);

    // Whether to wait on the dependencies applying globally before returning a result
    private final boolean async;

    private CoordinateSyncPoint(Node node, TxnId txnId, Txn txn, FullRoute<?> route, boolean async)
    {
        super(node, txnId, txn, route);
        checkArgument(txnId.rw() == Kind.SyncPoint || async, "Exclusive sync points only support async application");
        this.async = async;
    }

    public static CoordinateSyncPoint exclusive(Node node, Seekables<?, ?> keysOrRanges)
    {
        return coordinate(node, null, ExclusiveSyncPoint, keysOrRanges, true);
    }

    public static CoordinateSyncPoint inclusive(Node node, Seekables<?, ?> keysOrRanges, boolean async)
    {
        return coordinate(node, null,  Kind.SyncPoint, keysOrRanges, async);
    }

    private static CoordinateSyncPoint coordinate(Node node, @Nullable TxnId txnId, Kind kind, Seekables<?, ?> keysOrRanges, boolean async)
    {
        if (txnId == null)
            txnId = node.nextTxnId(kind, keysOrRanges.domain());
        checkState(txnId.rw() == Kind.SyncPoint || txnId.rw() == ExclusiveSyncPoint);
        FullRoute<?> route = node.computeRoute(txnId, keysOrRanges);
        CoordinateSyncPoint coordinate = new CoordinateSyncPoint(node, txnId, node.agent().emptyTxn(kind, keysOrRanges), route, async);
        coordinate.start();
        return coordinate;
    }

    public static AsyncResult<SyncPoint> coordinate(Node node, TxnId txnId, Seekables<?, ?> keysOrRanges)
    {
        return coordinate(node, txnId, txnId.rw(), keysOrRanges, true);
    }

    void onNewEpoch(Topologies topologies, Timestamp executeAt, List<PreAcceptOk> successes)
    {
        // SyncPoint transactions always propose their own txnId as their executeAt, as they are not really executed.
        // They only create happens-after relationships wrt their dependencies, which represent all transactions
        // that *may* execute before their txnId, so once these dependencies apply we can say that any action that
        // awaits these dependencies applies after them. In the case of ExclusiveSyncPoint, we additionally guarantee
        // that no lower TxnId can later apply.
        onPreAccepted(topologies, executeAt, successes);
    }

    void onPreAccepted(Topologies topologies, Timestamp executeAt, List<PreAcceptOk> successes)
    {
        Deps deps = Deps.merge(successes, ok -> ok.deps);
        Timestamp checkRejected = foldl(successes, (ok, prev) -> mergeMax(ok.witnessedAt, prev), Timestamp.NONE);
        if (checkRejected.isRejected())
        {
            proposeAndCommitInvalidate(node, Ballot.ZERO, txnId, route.homeKey(), route, checkRejected, this);
        }
        else
        {
            executeAt = txnId;
            // we don't need to fetch deps from Accept replies, so we don't need to contact unsynced epochs
            topologies = node.topology().forEpoch(route, txnId.epoch());
            // SyncPoint transactions always propose their own txnId as their executeAt, as they are not really executed.
            // They only create happens-after relationships wrt their dependencies, which represent all transactions
            // that *may* execute before their txnId, so once these dependencies apply we can say that any action that
            // awaits these dependencies applies after them. In the case of ExclusiveSyncPoint, we additionally guarantee
            // that no lower TxnId can later apply.
            ProposeSyncPoint.proposeSyncPoint(node, topologies, Ballot.ZERO, txnId, txn, route, deps, executeAt, this, async, tracker.nodes());
            // TODO make sure any changes here are properly integrated with ProposeSyncPoint
            // It seems like sending the commit is not there yet
//            new Propose<SyncPoint>(node, topologies, Ballot.ZERO, txnId, txn, route, executeAt, deps, this)
//            {
//                @Override
//                void onAccepted()
//                {
//                    Topologies commit = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
//                    Topologies latest = commit.size() == 1 ? commit : node.topology().forEpoch(route, executeAt.epoch());
//                    if (latest != commit)
//                        node.send(commit.nodes(), id -> new Commit(Commit.Kind.Maximal, id, commit.forEpoch(txnId.epoch()), commit, txnId, txn, route, null, executeAt, deps, false));
//                    node.send(latest.nodes(), id -> new Apply(id, latest, latest, executeAt.epoch(), txnId, route, txn, executeAt, deps, txn.execute(txnId, executeAt, null), txn.result(txnId, executeAt, null)));
//                    accept(new SyncPoint(txnId, deps, route), null);
//                }
//            }.start();
        }
    }
}
