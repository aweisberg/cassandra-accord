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
import javax.annotation.Nullable;

import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.messages.Commit;
import accord.messages.Propagate;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Known;
import accord.primitives.LatestDeps;
import accord.primitives.Participants;
import accord.primitives.ProgressToken;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Status;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.WrappableException;

import static accord.coordinate.CoordinationAdapter.Factory.Kind.Recovery;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownExecuteAt.ApplyAtKnown;
import static accord.primitives.Known.Outcome.Apply;
import static accord.primitives.ProgressToken.APPLIED;
import static accord.primitives.ProgressToken.INVALIDATED;
import static accord.primitives.ProgressToken.TRUNCATED_DURABLE_OR_INVALIDATED;
import static accord.primitives.Route.castToFullRoute;
import static accord.primitives.Status.Durability.Majority;
import static accord.topology.Topologies.SelectNodeOwnership.SLICE;
import static accord.topology.Topologies.SelectNodeOwnership.SHARE;
import static accord.utils.Invariants.illegalState;

public class RecoverWithRoute extends CheckShards<FullRoute<?>>
{
    final @Nullable Ballot promisedBallot; // if non-null, has already been promised by some shard
    final BiConsumer<Outcome, Throwable> callback;
    final Status witnessedByInvalidation;
    // TODO (required): we don't use this on all paths, and should at least make sure we don't trigger any Invariant failures e.g. "Fetch was successful for all keys, but the WaitingState has not been cleared"
    final long reportLowEpoch, reportHighEpoch;

    private RecoverWithRoute(Node node, Topologies topologies, @Nullable Ballot promisedBallot, TxnId txnId, Infer.InvalidIf invalidIf, FullRoute<?> route, Status witnessedByInvalidation, long reportLowEpoch, long reportHighEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        super(node, txnId, route, IncludeInfo.All, invalidIf);
        this.reportLowEpoch = reportLowEpoch;
        this.reportHighEpoch = reportHighEpoch;
        // if witnessedByInvalidation == AcceptedInvalidate then we cannot assume its definition was known, and our comparison with the status is invalid
        Invariants.require(witnessedByInvalidation != Status.AcceptedInvalidate);
        // if witnessedByInvalidation == Invalidated we should anyway not be recovering
        Invariants.require(witnessedByInvalidation != Status.Invalidated);
        this.promisedBallot = promisedBallot;
        this.callback = callback;
        this.witnessedByInvalidation = witnessedByInvalidation;
        assert topologies.oldestEpoch() == topologies.currentEpoch() && topologies.currentEpoch() == txnId.epoch();
    }

    public static RecoverWithRoute recover(Node node, TxnId txnId, Infer.InvalidIf invalidIf, FullRoute<?> route, @Nullable Status witnessedByInvalidation, long reportLowEpoch, long reportHighEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, node.topology().forEpoch(route, txnId.epoch(), SHARE), txnId, invalidIf, route, witnessedByInvalidation, reportLowEpoch, reportHighEpoch, callback);
    }

    private static RecoverWithRoute recover(Node node, Topologies topologies, TxnId txnId, Infer.InvalidIf invalidIf, FullRoute<?> route, @Nullable Status witnessedByInvalidation, long reportLowEpoch, long reportHighEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, topologies, null, txnId, invalidIf, route, witnessedByInvalidation, reportLowEpoch, reportHighEpoch, callback);
    }

    public static RecoverWithRoute recover(Node node, @Nullable Ballot promisedBallot, TxnId txnId, Infer.InvalidIf invalidIf, FullRoute<?> route, @Nullable Status witnessedByInvalidation, long reportLowEpoch, long reportHighEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, node.topology().forEpoch(route, txnId.epoch(), SHARE), promisedBallot, txnId, invalidIf, route, witnessedByInvalidation, reportLowEpoch, reportHighEpoch, callback);
    }

    private static RecoverWithRoute recover(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Infer.InvalidIf invalidIf, FullRoute<?> route, @Nullable Status witnessedByInvalidation, long reportLowEpoch, long reportHighEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        RecoverWithRoute recover = new RecoverWithRoute(node, topologies, ballot, txnId, invalidIf, route, witnessedByInvalidation, reportLowEpoch, reportHighEpoch, callback);
        recover.start();
        return recover;
    }

    private FullRoute<?> route()
    {
        return castToFullRoute(this.route);
    }

    @Override
    public void contact(Id to)
    {
        node.send(to, new CheckStatus(to, topologies(), txnId, route, sourceEpoch, IncludeInfo.All), this);
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        Ranges rangesForNode = topologies().getEpoch(txnId.epoch()).rangesForNode(from);
        Route<?> route = this.route.slice(rangesForNode);
        return isSufficient(route, ok);
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        return isSufficient(route, merged);
    }

    protected boolean isSufficient(Route<?> route, CheckStatusOk ok)
    {
        CheckStatusOkFull full = (CheckStatusOkFull)ok;
        Known sufficientTo = full.knownFor(txnId, route, route);
        if (!sufficientTo.isDefinitionKnown())
            return false;

        if (sufficientTo.outcome().isInvalidated())
            return true;

        Invariants.require(full.partialTxn.covers(route));
        return true;
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure != null)
        {
            callback.accept(null, failure);
            return;
        }

        CheckStatusOkFull full = ((CheckStatusOkFull) this.merged).finish(route, route, route, success.withQuorum, previouslyKnownToBeInvalidIf);
        Known known = full.knownFor(txnId, route, route);

        // TODO (required): audit this logic, and centralise with e.g. FetchData inferences
        // TODO (expected): skip straight to ExecuteTxn if we have a Stable reply from each shard
        switch (known.outcome())
        {
            default: throw new AssertionError();
            case Unknown:
                if (known.definition().isKnown())
                {
                    Txn txn = full.partialTxn.reconstitute(route);
                    Recover.recover(node, txnId, txn, route, reportLowEpoch, reportHighEpoch, callback);
                }
                else if (!known.definition().isOrWasKnown())
                {
                    if (witnessedByInvalidation != null && witnessedByInvalidation.compareTo(Status.PreAccepted) > 0)
                        throw illegalState("We previously invalidated, finding a status that should be recoverable");
                    Invalidate.invalidate(node, txnId, route, witnessedByInvalidation != null, reportLowEpoch, reportHighEpoch, callback);
                }
                else
                {
                    callback.accept(full.toProgressToken(), null);
                }
                break;

            case WasApply:
            case Apply:
                if (!known.isDefinitionKnown())
                {
                    if (!known.isTruncated() && !known.isInvalidated())
                    {
                        // we must have raced with a successful apply, so should simply abort
                        callback.accept(ProgressToken.NONE, null);
                        return;
                    }

                    // TODO (expected): if we determine new durability, propagate it
                    CheckStatusOkFull propagate;
                    if (!full.map.hasFullyTruncated(route))
                    {
                        // we might have only part of the full transaction, and a shard may have truncated;
                        // in this case we want to skip straight to apply, but only for the shards that haven't truncated
                        Route<?> trySendTo = route.without(full.map.matchingRanges(minMax -> minMax.min.isTruncated()));
                        if (!trySendTo.isEmpty())
                        {
                            if (known.isInvalidated())
                            {
                                Commit.Invalidate.commitInvalidate(node, txnId, trySendTo, txnId);
                            }
                            else
                            {
                                known = full.knownFor(txnId, trySendTo, trySendTo);
                                if (known.isDefinitionKnown() && known.is(ApplyAtKnown) && known.outcome() == Apply)
                                {
                                    if (!known.is(DepsKnown))
                                    {
                                        Invariants.require(txnId.isSystemTxn() || full.partialTxn.covers(trySendTo));
                                        Participants<?> haveStable = full.map.knownFor(Known.DepsOnly, route);
                                        Route<?> haveUnstable = route.without(haveStable);
                                        Deps stable = full.stableDeps.reconstitutePartial(haveStable).asFullUnsafe();

                                        LatestDeps.withStable(node.coordinationAdapter(txnId, Recovery), node, txnId, full.executeAt, full.partialTxn, stable, haveUnstable, trySendTo, SLICE, route, callback, deps -> {
                                            Deps stableDeps = deps.intersecting(trySendTo);
                                            node.coordinationAdapter(txnId, Recovery).persist(node, null, trySendTo, trySendTo, SLICE, route, txnId, full.partialTxn, full.executeAt, stableDeps, full.writes, full.result, null);
                                        });
                                    }
                                    else
                                    {
                                        Invariants.require(full.stableDeps.covers(trySendTo));
                                        Invariants.require(txnId.isSystemTxn() || full.partialTxn.covers(trySendTo));
                                        node.coordinationAdapter(txnId, Recovery).persist(node, null, trySendTo, trySendTo, SLICE, route, txnId, full.partialTxn, full.executeAt, full.stableDeps, full.writes, full.result, null);
                                    }
                                }
                            }
                            propagate = full;
                        }
                        else
                        {
                            propagate = full.merge(Majority);
                        }
                    }
                    else
                    {
                        propagate = full;
                    }

                    Propagate.propagate(node, txnId, previouslyKnownToBeInvalidIf, sourceEpoch, reportLowEpoch, reportHighEpoch, success.withQuorum, route, route, null, propagate, (s, f) -> callback.accept(f == null ? propagate.toProgressToken() : null, f));
                    break;
                }

                Txn txn = full.partialTxn.reconstitute(route);
                if (known.is(ApplyAtKnown) && known.outcome() == Apply)
                {
                    Deps deps;
                    Route<?> missingDeps;
                    if (known.is(DepsKnown))
                    {
                        deps = full.stableDeps.reconstitute(route);
                        missingDeps = route.slice(0, 0);
                    }
                    else
                    {
                        Participants<?> hasDeps = full.map.knownFor(Known.DepsOnly, route);
                        missingDeps = route.without(hasDeps);
                        // convert to plain Deps as when we merge with latest deps we may erroneously keep the
                        // PartialDeps if e.g. an empty range of deps is found
                        deps = new Deps(full.stableDeps.reconstitutePartial(hasDeps));
                    }
                    LatestDeps.withStable(node.coordinationAdapter(txnId, Recovery), node, txnId, full.executeAt, full.partialTxn, deps, missingDeps, route, SHARE, route, callback, mergedDeps -> {
                        node.withEpoch(full.executeAt.epoch(), node.agent(), t -> WrappableException.wrap(t), () -> {
                            node.coordinationAdapter(txnId, Recovery).persist(node, topologies, route, txnId, txn, full.executeAt, mergedDeps, full.writes, full.result, (s, f) -> {
                                callback.accept(f == null ? APPLIED : null, f);
                            });
                        });
                    });
                }
                else
                {
                    Recover.recover(node, txnId, txn, route, callback);
                }
                break;

            case Abort:
                if (witnessedByInvalidation != null && witnessedByInvalidation.hasBeen(Status.PreCommitted))
                    throw illegalState("We previously invalidated, finding a status that should be recoverable");

                Propagate.propagate(node, txnId, previouslyKnownToBeInvalidIf, sourceEpoch, reportLowEpoch, reportHighEpoch, success.withQuorum, route, route, null, full, (s, f) -> callback.accept(f == null ? INVALIDATED : null, f));
                break;

            case Erased:
                // we should only be able to hit the Erased case if every participating shard has advanced past this TxnId, so we don't need to recover it
                Propagate.propagate(node, txnId, previouslyKnownToBeInvalidIf, sourceEpoch, reportLowEpoch, reportHighEpoch, success.withQuorum, route, route, null, full, (s, f) -> callback.accept(f == null ? TRUNCATED_DURABLE_OR_INVALIDATED : null, f));
                break;
        }
    }
}
