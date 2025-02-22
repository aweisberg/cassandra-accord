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

package accord.local;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.api.VisibleForImplementation;
import accord.local.RedundantStatus.Coverage;
import accord.local.RedundantStatus.Property;
import accord.primitives.AbstractRanges;
import accord.primitives.Deps;
import accord.primitives.EpochSupplier;
import accord.primitives.KeyDeps;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;
import org.agrona.collections.Int2ObjectHashMap;

import static accord.api.ProtocolModifiers.Toggles.requiresUniqueHlcs;
import static accord.local.RedundantStatus.Coverage.ALL;
import static accord.local.RedundantStatus.Coverage.SOME;
import static accord.local.RedundantStatus.ONLY_LE_MASK;
import static accord.local.RedundantStatus.NOT_OWNED_ONLY;
import static accord.local.RedundantStatus.PRE_BOOTSTRAP_OR_STALE_ONLY;
import static accord.local.RedundantStatus.Property.GC_BEFORE;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.LOCALLY_SYNCED;
import static accord.local.RedundantStatus.Property.LOCALLY_WITNESSED;
import static accord.local.RedundantStatus.Property.PRE_BOOTSTRAP;
import static accord.local.RedundantStatus.Property.PRE_BOOTSTRAP_OR_STALE;
import static accord.local.RedundantStatus.Property.SHARD_AND_LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED_AND_LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED_AND_LOCALLY_SYNCED;
import static accord.local.RedundantStatus.Property.SHARD_ONLY_APPLIED;
import static accord.local.RedundantStatus.WAS_OWNED_LOCALLY_RETIRED;
import static accord.local.RedundantStatus.WAS_OWNED_ONLY;
import static accord.local.RedundantStatus.WAS_OWNED_SHARD_RETIRED;
import static accord.local.RedundantStatus.addHistory;
import static accord.local.RedundantStatus.any;
import static accord.local.RedundantStatus.mask;
import static accord.local.RedundantStatus.matchesMask;
import static accord.primitives.Timestamp.Flag.SHARD_BOUND;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.utils.ArrayBuffers.cachedAny;
import static accord.utils.ArrayBuffers.cachedInts;
import static accord.utils.Invariants.illegalState;
import static accord.utils.Invariants.require;
import static accord.utils.Invariants.requireStrictlyOrdered;

public class RedundantBefore extends ReducingRangeMap<RedundantBefore.Bounds>
{
    public interface RedundantBeforeSupplier
    {
        RedundantBefore redundantBefore();
    }

    public static class SerializerSupport
    {
        public static RedundantBefore create(boolean inclusiveEnds, RoutingKey[] ends, Bounds[] values)
        {
            return new RedundantBefore(inclusiveEnds, ends, values);
        }
    }

    public static class QuickBounds
    {
        // start inclusive, end exclusive
        public final long startEpoch, endEpoch;
        public final TxnId bootstrappedAt;
        public final TxnId gcBefore;

        public QuickBounds(long startEpoch, long endEpoch, TxnId bootstrappedAt, TxnId gcBefore)
        {
            this.startEpoch = startEpoch;
            this.endEpoch = endEpoch;
            this.bootstrappedAt = bootstrappedAt;
            this.gcBefore = gcBefore;
        }

        public QuickBounds withEpochs(long startEpoch, long endEpoch)
        {
            return new QuickBounds(startEpoch, endEpoch, bootstrappedAt, gcBefore);
        }

        public QuickBounds withGcBeforeBeforeAtLeast(TxnId newGcBefore)
        {
            return new QuickBounds(startEpoch, endEpoch, bootstrappedAt, newGcBefore);
        }

        public QuickBounds withBootstrappedAtLeast(TxnId newBootstrappedAt)
        {
            return new QuickBounds(startEpoch, endEpoch, newBootstrappedAt, gcBefore);
        }
    }

    public static class Bounds extends QuickBounds
    {
        // TODO (desired): we don't need to maintain this now, can migrate to ReducingRangeMap.foldWithBounds
        public final Range range;

        // TODO (expected): we need to eventually support GCing PRE_BOOTSTRAP bounds
        //  once we know storage layer has fully expunged earlier TxnId
        //  OR we may be able to safely overwrite them with some better invariants and adequate testing
        public final TxnId[] bounds;
        // two entries per bound, first for equality (LE) matches, second for inequality (LT) matches
        public final int[] statuses;

        private transient final long maxBoundEpoch, maxBoundHlc;
        public transient final TxnId depBound;

        /**
         * staleUntilAtLeast provides a minimum TxnId until which we know we will be unable to completely execute
         * transactions locally for the impacted range.
         *
         * See also {@link SafeCommandStore#safeToReadAt()}.
         */
        public final @Nullable Timestamp staleUntilAtLeast;
        private transient final RedundantStatus noMatch;
        private transient RedundantStatus last = RedundantStatus.NONE;

        public Bounds(Range range, long startEpoch, long endEpoch, TxnId[] bounds, int[] statuses, @Nullable Timestamp staleUntilAtLeast)
        {
            super(startEpoch, endEpoch, maxBound(bounds, statuses, PRE_BOOTSTRAP), maxBound(bounds, statuses, GC_BEFORE));
            this.range = range;
            this.bounds = bounds;
            this.statuses = statuses;
            this.staleUntilAtLeast = staleUntilAtLeast;
            this.noMatch = staleUntilAtLeast == null ? RedundantStatus.NONE : PRE_BOOTSTRAP_OR_STALE_ONLY;
            this.maxBoundEpoch = bounds.length == 0 ? 0 : bounds[0].epoch();
            this.maxBoundHlc = bounds.length == 0 ? 0 : bounds[0].hlc();
            this.depBound = depBound(bounds, statuses);
            checkMinBoundOrRX(bounds);
            requireStrictlyOrdered(Comparator.reverseOrder(), bounds);
            require(isShardBound(gcBefore) || gcBefore.equals(TxnId.NONE));
        }

        public static Bounds create(Range range, TxnId bound, RedundantStatus status, @Nullable Timestamp staleUntilAtLeast)
        {
            return create(range, Long.MIN_VALUE, Long.MAX_VALUE, bound, status, staleUntilAtLeast);
        }

        public static Bounds create(Range range, long startEpoch, long endEpoch, TxnId bound, RedundantStatus status, @Nullable Timestamp staleUntilAtLeast)
        {
            return new Bounds(range, startEpoch, endEpoch, new TxnId[] { bound }, new int[] { status.encoded & ONLY_LE_MASK, status.encoded }, staleUntilAtLeast);
        }

        private static TxnId depBound(TxnId[] bounds, int[] statuses)
        {
            TxnId depBound = maxBound(bounds, statuses, SHARD_AND_LOCALLY_APPLIED);
            if (depBound.equals(TxnId.NONE))
                return null;
            return depBound.addFlag(SHARD_BOUND);
        }

        private static void checkMinBoundOrRX(TxnId ... txnIds)
        {
            for (TxnId txnId : txnIds)
                checkMinBoundOrRX(txnId);
        }

        private static void checkMinBoundOrRX(TxnId txnId)
        {
            Invariants.requireArgument(txnId.domain().isRange() && txnId.is(ExclusiveSyncPoint) || isMinBound(txnId));
        }

        private static boolean isMinBound(TxnId txnId)
        {
            return txnId.hlc() == 0 && txnId.flags() == 0 && txnId.node.id == 0;
        }

        private static boolean isShardBound(TxnId txnId)
        {
            return txnId.domain().isRange() && txnId.is(ExclusiveSyncPoint) && txnId.is(SHARD_BOUND);
        }

        public static Bounds reduce(Bounds a, Bounds b)
        {
            return merge(a.range.slice(b.range), a, b);
        }

        private static Bounds merge(Range range, Bounds cur, Bounds add)
        {
            // TODO (required): we shouldn't be trying to merge non-intersecting epochs
            if (cur.startEpoch > add.endEpoch)
                return cur;

            if (add.startEpoch > cur.endEpoch)
                return add;

            int csu = compareStaleUntilAtLeast(cur.staleUntilAtLeast, add.staleUntilAtLeast);
            Timestamp staleUntilAtLeast = csu >= 0 ? cur.staleUntilAtLeast : add.staleUntilAtLeast;
            staleUntilAtLeast = maybeClearStaleUntilAtLeast(staleUntilAtLeast, cur.bounds, cur.statuses);
            staleUntilAtLeast = maybeClearStaleUntilAtLeast(staleUntilAtLeast, add.bounds, add.statuses);

            long startEpoch = Long.max(cur.startEpoch, add.startEpoch);
            long endEpoch = Long.min(cur.endEpoch, add.endEpoch);
            TxnId[] mergedBounds;
            int[] mergedStatuses;
            {
                Object[] boundBuf = null;
                int[] statusBuf = null;
                int mergedCount = 0;
                int prevLtStatus = staleUntilAtLeast == null ? 0 : PRE_BOOTSTRAP_OR_STALE_ONLY.encoded;
                int prevExistingLtStatus = prevLtStatus; // we don't apply PRE_BOOTSTRAP_MERGE_MASK as this already applied
                int i = 0, j = 0;
                while (i < cur.bounds.length || j < add.bounds.length)
                {
                    int c = i == cur.bounds.length ? -1 : j == add.bounds.length ? 1 : cur.bounds[i].compareTo(add.bounds[j]);
                    TxnId nextBound;
                    int leStatus, ltStatus;
                    if (c > 0)
                    {
                        nextBound = cur.bounds[i];
                        leStatus = addHistory(prevLtStatus, cur.statuses[i*2]);
                        ltStatus = addHistory(prevLtStatus, prevExistingLtStatus = cur.statuses[i*2+1]);
                        ++i;
                    }
                    else if (c < 0)
                    {
                        nextBound = add.bounds[j];
                        leStatus = addHistory(prevLtStatus | add.statuses[j*2],   prevExistingLtStatus);
                        ltStatus = addHistory(prevLtStatus | add.statuses[j*2+1], prevExistingLtStatus);
                        ++j;
                    }
                    else
                    {
                        nextBound = cur.bounds[i].addFlags(add.bounds[j]);
                        leStatus = addHistory(prevLtStatus | add.statuses[j*2],   cur.statuses[i*2]);
                        ltStatus = addHistory(prevLtStatus | add.statuses[j*2+1], prevExistingLtStatus = cur.statuses[i*2+1]);
                        ++i;
                        ++j;
                    }

                    // we keep the start/end bound of an equal pre-bootstrap run, so that we correctly apply Property.mergeWithPreBootstrap
                    if (leStatus == ltStatus && ltStatus == prevLtStatus)
                    {
                        if (!any(prevLtStatus, PRE_BOOTSTRAP_OR_STALE))
                            continue;

                        if (mergedCount >= 2)
                        {
                            int[] prev = statusBuf != null ? statusBuf : cur.statuses;
                            int prev2LtStatus = prev[mergedCount*2 - 3];
                            int prevLeStatus = prev[mergedCount*2 - 2];
                            Invariants.require(prevLtStatus == prev[mergedCount*2 - 1]);
                            if (prevLtStatus == prev2LtStatus && prevLeStatus == prev2LtStatus)
                                --mergedCount;
                        }
                    }

                    if (boundBuf == null)
                    {
                        if (mergedCount < cur.bounds.length && cur.bounds[mergedCount].equalsStrict(nextBound)
                            && cur.statuses[mergedCount*2] == leStatus && cur.statuses[mergedCount*2+1] == ltStatus)
                        {
                            prevLtStatus = ltStatus;
                            ++mergedCount;
                            continue;
                        }

                        boundBuf = cachedAny().get(cur.bounds.length + add.bounds.length);
                        statusBuf = cachedInts().getInts((cur.bounds.length + add.bounds.length)*2);
                        System.arraycopy(cur.bounds, 0, boundBuf, 0, mergedCount);
                        System.arraycopy(cur.statuses, 0, statusBuf, 0, mergedCount*2);
                    }
                    boundBuf[mergedCount] = nextBound;
                    statusBuf[mergedCount*2] = leStatus;
                    statusBuf[mergedCount*2 + 1] = ltStatus;
                    ++mergedCount;
                    prevLtStatus = ltStatus;
                }

                if (boundBuf == null)
                {
                    mergedBounds = mergedCount == cur.bounds.length ? cur.bounds : Arrays.copyOf(cur.bounds, mergedCount);
                    mergedStatuses = mergedCount == cur.statuses.length ? cur.statuses : Arrays.copyOf(cur.statuses, mergedCount * 2);
                }
                else
                {
                    mergedBounds = new TxnId[mergedCount];
                    mergedStatuses = new int[mergedCount*2];
                    System.arraycopy(boundBuf, 0, mergedBounds, 0, mergedCount);
                    System.arraycopy(statusBuf, 0, mergedStatuses, 0, mergedCount*2);
                    cachedAny().forceDiscard(boundBuf, mergedCount);
                    cachedInts().forceDiscard(statusBuf);
                }
            }

            return new Bounds(range, startEpoch, endEpoch, mergedBounds, mergedStatuses, staleUntilAtLeast);
        }

        private static Timestamp maybeClearStaleUntilAtLeast(Timestamp staleUntilAtLeast, TxnId[] bounds, int[] statuses)
        {
            for (int i = 0 ; staleUntilAtLeast != null && i < bounds.length && bounds[i].compareTo(staleUntilAtLeast) > 0 ; ++i)
            {
                if (any(statuses[i], PRE_BOOTSTRAP))
                    staleUntilAtLeast = null;
            }
            return staleUntilAtLeast;
        }

        public Bounds with(TxnId newBound, RedundantStatus addStatus)
        {
            // TODO (desired): introduce special-cased faster merge for adding a single value
            return merge(range, this, new Bounds(range, Long.MIN_VALUE, Long.MAX_VALUE, new TxnId[] { newBound }, new int[] { addStatus.encoded }, null));
        }

        @VisibleForImplementation
        public Bounds withEpochs(long start, long end)
        {
            return new Bounds(range, start, end, bounds, statuses, staleUntilAtLeast);
        }

        static @Nonnull Boolean isShardOnlyApplied(Bounds bounds, @Nonnull Boolean prev, TxnId txnId)
        {
            return is(bounds, prev, txnId, SHARD_ONLY_APPLIED);
        }

        static @Nonnull Boolean is(Bounds bounds, @Nonnull Boolean prev, TxnId txnId, Property property)
        {
            return bounds == null ? prev : bounds.is(txnId, property);
        }

        boolean is(TxnId txnId, Property a, Property b)
        {
            Invariants.require(a != GC_BEFORE && b != GC_BEFORE);
            if (a.compareLessEqual != b.compareLessEqual)
                return is(txnId, a) && is(txnId, b);

            int propertyMask = mask(a, SOME) | mask(b, SOME);
            if (matchesMask(noMatch.encoded, propertyMask))
                return true;

            if (noBoundMatches(txnId))
                return false;

            int i = findStatusIndexInternal(txnId);
            return i >= 0 && matchesMask(statuses[i], propertyMask);
        }

        boolean is(TxnId txnId, Property test)
        {
            Invariants.require(test != GC_BEFORE);
            if (noBoundMatches(txnId))
                return noMatch.any(test);

            if (test.mergeWithPreBootstrapOrStale)
                return txnId.compareTo(bound(test)) <= test.compareLessEqual;

            int i = findStatusIndexInternal(txnId);
            return i >= 0 && any(statuses[i], test);
        }

        TxnId bound(Property property)
        {
            Invariants.require(property != GC_BEFORE);
            Invariants.require(property.mergeWithPreBootstrapOrStale);
            return maxBound(property);
        }

        boolean noBoundMatches(TxnId txnId)
        {
            long epoch = txnId.epoch();
            return epoch > maxBoundEpoch || (epoch == maxBoundEpoch && txnId.hlc() > maxBoundHlc);
        }

        int findStatusIndex(TxnId txnId)
        {
            if (noBoundMatches(txnId))
                return -1;
            return findStatusIndexInternal(txnId);
        }

        int findStatusIndexInternal(TxnId txnId)
        {
            int i = 0;
            while (i < bounds.length)
            {
                int c = txnId.compareTo(bounds[i]);
                if (c >= 0)
                    return i * 2 - (c == 0 ? 0 : 1);
                ++i;
            }
            return statuses.length - 1;
        }

        public TxnId maxBound(Property property)
        {
            return maxBound(bounds, statuses, property);
        }

        static TxnId maxBound(TxnId[] bounds, int[] statuses, Property property)
        {
            return maxBound(bounds, statuses, mask(property, ALL));
        }

        static TxnId maxBound(TxnId[] bounds, int[] statuses, int propertyMask)
        {
            // we ignore LT/LE, as this should be applied by the caller as part of comparing maxBound
            for (int i = 1; i < statuses.length ; i += 2)
            {
                if ((statuses[i] & propertyMask) == propertyMask)
                    return bounds[i/2];
            }
            return TxnId.NONE;
        }

        static @Nullable RedundantStatus getAndMerge(Bounds bounds, @Nullable RedundantStatus prev, TxnId txnId, @Nullable Timestamp executeAtIfKnown)
        {
            if (bounds == null)
                return prev;
            RedundantStatus next = bounds.get(txnId, executeAtIfKnown);
            return prev == null ? next : prev.mergeShards(next);
        }

        static RangeDeps.BuilderByRange collectDep(Bounds bounds, @Nonnull RangeDeps.BuilderByRange prev, @Nonnull EpochSupplier minEpoch, @Nonnull EpochSupplier executeAt)
        {
            // we report an RX that represents a point on or after our GC bound, so that we never report an incomplete
            // transitive dependency history. If we consistently only GC'd at gcBefore we could report this bound,
            // but since it is likely safe to use this bound in cases that don't have lagged durability,
            // we conservatively report this bound since it is expected to be applied already at all non-stale shards
            if (bounds != null && bounds.depBound != null)
                prev.add(bounds.range, bounds.depBound);

            return prev;
        }

        static Ranges validateSafeToRead(Bounds bounds, @Nonnull Ranges safeToRead, Timestamp bootstrapAt, Object ignore)
        {
            if (bounds == null)
                return safeToRead;

            if (bootstrapAt.compareTo(bounds.maxBootstrappedAt()) < 0 || (bounds.staleUntilAtLeast != null && bootstrapAt.compareTo(bounds.staleUntilAtLeast) < 0))
                return safeToRead.without(Ranges.of(bounds.range));

            return safeToRead;
        }

        static TxnId min(Bounds bounds, @Nullable TxnId min, Function<Bounds, TxnId> get)
        {
            if (bounds == null)
                return min;

            return TxnId.nonNullOrMin(min, get.apply(bounds));
        }

        static TxnId max(Bounds bounds, @Nullable TxnId max, Function<Bounds, TxnId> get)
        {
            if (bounds == null)
                return max;

            return TxnId.nonNullOrMax(max, get.apply(bounds));
        }

        static Participants<?> participantsWithoutStaleOrPreBootstrap(Bounds bounds, @Nonnull Participants<?> execute, TxnId txnId, @Nullable EpochSupplier executeAt)
        {
            return withoutStaleOrPreBootstrap(bounds, execute, txnId, executeAt, Participants::without);
        }

        static Ranges rangesWithoutStaleOrPreBootstrap(Bounds bounds, @Nonnull Ranges execute, TxnId txnId, @Nullable EpochSupplier executeAt)
        {
            return withoutStaleOrPreBootstrap(bounds, execute, txnId, executeAt, Ranges::without);
        }

        static <P extends Participants<?>> P withoutStaleOrPreBootstrap(Bounds bounds, @Nonnull P execute, TxnId txnId, @Nullable EpochSupplier executeAt, BiFunction<P, Ranges, P> without)
        {
            if (bounds == null)
                return execute;

            Invariants.require(executeAt == null ? !bounds.outOfBounds(txnId) : !bounds.outOfBounds(txnId, executeAt));
            if (bounds.is(txnId, PRE_BOOTSTRAP_OR_STALE))
                return without.apply(execute, Ranges.of(bounds.range));

            return execute;
        }

        static Participants<?> withoutStaleOrPreBootstrapOrLocallyRetired(Bounds bounds, @Nonnull Participants<?> execute, TxnId txnId)
        {
            if (bounds == null)
                return execute;

            if (bounds.is(txnId, PRE_BOOTSTRAP_OR_STALE) || bounds.isLocallyRetired())
                return execute.without(Ranges.of(bounds.range));

            return execute;
        }

        static Participants<?> withoutRedundantAnd_StaleOrPreBootstrap(Bounds bounds, @Nonnull Participants<?> execute, TxnId txnId, @Nullable Timestamp executeAt)
        {
            if (bounds == null)
                return execute;

            boolean outOfBounds = executeAt == null ? bounds.outOfBounds(txnId) : bounds.outOfBounds(txnId, executeAt);
            Invariants.expect(!outOfBounds, "Trying to apply withoutRedundantAnd_StaleOrPreBootstrap to %s for a range we don't own (%s), suggesting we computed ownership without an up-to-date epoch", txnId, bounds);
            if (outOfBounds || bounds.is(txnId, SHARD_ONLY_APPLIED, PRE_BOOTSTRAP_OR_STALE))
                return execute.without(Ranges.of(bounds.range));

            return execute;
        }

        static Participants<?> withoutRedundantAnd_StaleOrPreBootstrapOrRetiredOrNotOwned(Bounds bounds, @Nonnull Participants<?> execute, TxnId txnId, @Nullable Timestamp executeAtIfKnown)
        {
            if (bounds == null)
                return execute;

            if (bounds.is(txnId, SHARD_ONLY_APPLIED)
                && ((bounds.endEpoch <= txnId.epoch() && (!txnId.awaitsPreviouslyOwned() || bounds.isLocallyRetired()))
                    || (executeAtIfKnown != null && executeAtIfKnown.epoch() < bounds.startEpoch)
                    || bounds.is(txnId, PRE_BOOTSTRAP_OR_STALE)))
                return execute.without(Ranges.of(bounds.range));

            return execute;
        }

        static Participants<?> withoutNotOwnedShardOnlyRedundant(Bounds bounds, @Nonnull Participants<?> ownedOrNotRedundant, TxnId txnId, @Nullable Timestamp executeAtIfKnown)
        {
            if (bounds == null)
                return ownedOrNotRedundant;

            if (txnId.compareTo(bounds.maxBound(SHARD_ONLY_APPLIED)) <= 0
                && (bounds.endEpoch <= txnId.epoch()
                    || (executeAtIfKnown != null && executeAtIfKnown.epoch() < bounds.startEpoch)))
                return ownedOrNotRedundant.without(Ranges.of(bounds.range));

            return ownedOrNotRedundant;
        }

        static Ranges withoutBeforeGc(Bounds entry, @Nonnull Ranges notGarbage, TxnId txnId, @Nullable Timestamp executeAt)
        {
            if (entry == null || (executeAt == null ? entry.outOfBounds(txnId) : entry.outOfBounds(txnId, executeAt)))
                return notGarbage;

            if (txnId.compareTo(entry.gcBefore) < 0)
                return notGarbage.without(Ranges.of(entry.range));

            return notGarbage;
        }

        static Ranges withoutAnyRetired(Bounds bounds, @Nonnull Ranges notRetired)
        {
            if (bounds == null || bounds.endEpoch > bounds.maxBound(SHARD_ONLY_APPLIED).epoch())
                return notRetired;

            return notRetired.without(Ranges.of(bounds.range));
        }

        static Ranges withoutPreBootstrap(Bounds bounds, @Nonnull Ranges notPreBootstrap, TxnId txnId, Object ignore)
        {
            if (bounds == null)
                return notPreBootstrap;

            if (bounds.is(txnId, PRE_BOOTSTRAP))
                return notPreBootstrap.without(Ranges.of(bounds.range));

            return notPreBootstrap;
        }

        RedundantStatus get(TxnId txnId, @Nullable Timestamp executeAtIfKnown)
        {
            if (wasOwned(txnId))
            {
                if (isShardRetired()) return WAS_OWNED_SHARD_RETIRED;
                if (isLocallyRetired()) return WAS_OWNED_LOCALLY_RETIRED;
                return WAS_OWNED_ONLY;
            }
            return getIgnoringOwnership(txnId, executeAtIfKnown);
        }

        RedundantStatus getIgnoringOwnership(TxnId txnId, @Nullable Timestamp executeAtIfKnown)
        {
            int i = findStatusIndex(txnId);
            if (i < 0)
                return noMatch;

            int status = statuses[i];
            if (any(status, GC_BEFORE))
            {
                if (requiresUniqueHlcs() && executeAtIfKnown != null && bounds[i/2].hlc() <= executeAtIfKnown.uniqueHlc())
                {
                    long uniqueHlc = executeAtIfKnown.uniqueHlc();
                    i/= 2;
                    while (--i >= 0 && bounds[i].hlc() <= uniqueHlc) {}
                    if (i < 0 || !any(statuses[i*2+1], GC_BEFORE))
                        status &= ~mask(GC_BEFORE, ALL);
                }
            }

            RedundantStatus reuse = last;
            if (status == reuse.encoded)
                return reuse;
            return last = new RedundantStatus(status);
        }

        private static int compareStaleUntilAtLeast(@Nullable Timestamp a, @Nullable Timestamp b)
        {
            boolean aIsNull = a == null, bIsNull = b == null;
            if (aIsNull != bIsNull) return aIsNull ? -1 : 1;
            return aIsNull ? 0 : a.compareTo(b);
        }

        public final TxnId gcBefore()
        {
            return gcBefore;
        }

        public final TxnId shardRedundantBefore()
        {
            return bound(SHARD_APPLIED_AND_LOCALLY_REDUNDANT);
        }

        public final TxnId locallyWitnessedBefore()
        {
            return bound(LOCALLY_WITNESSED);
        }

        public final TxnId locallyRedundantBefore()
        {
            return bound(LOCALLY_REDUNDANT);
        }

        // we may not have actually applied all earlier TxnId
        // TODO (expected): use LOCALLY_SYNCED?
        public final TxnId maxLocallyAppliedBefore()
        {
            return maxBound(LOCALLY_APPLIED);
        }

        // TODO (expected): check call-sites to see if can use something weaker such as redundantBefore
        public final TxnId maxBootstrappedAt()
        {
            return bootstrappedAt;
        }

        private boolean outOfBounds(EpochSupplier lb, EpochSupplier ub)
        {
            return ub.epoch() < startEpoch || lb.epoch() >= endEpoch;
        }

        private boolean wasOwned(EpochSupplier lb)
        {
            return lb.epoch() >= endEpoch;
        }

        private boolean isShardRetired()
        {
            return endEpoch <= maxBound(SHARD_APPLIED_AND_LOCALLY_SYNCED).epoch();
        }

        private boolean isLocallyRetired()
        {
            return endEpoch <= maxBound(LOCALLY_SYNCED).epoch();
        }

        private boolean outOfBounds(Timestamp lb)
        {
            return lb.epoch() >= endEpoch;
        }

        public Bounds withRange(Range range)
        {
            return new Bounds(range, startEpoch, endEpoch, bounds, statuses, staleUntilAtLeast);
        }

        public boolean equals(Object that)
        {
            return that instanceof Bounds && equals((Bounds) that);
        }

        public boolean equals(Bounds that)
        {
            return this.range.equals(that.range) && equalsIgnoreRange(that);
        }

        public boolean equalsIgnoreRange(Bounds that)
        {
            return this.startEpoch == that.startEpoch
                   && this.endEpoch == that.endEpoch
                   && Arrays.equals(this.bounds, that.bounds)
                   && Arrays.equals(this.statuses, that.statuses)
                   && Objects.equals(this.staleUntilAtLeast, that.staleUntilAtLeast);
        }

        private static Property[] PROPERTIES = Property.values();
        @Override
        public String toString()
        {
            TreeMap<TxnId, Set<Property>> build = new TreeMap<>(Comparator.reverseOrder());
            for (Property property : PROPERTIES)
                build.computeIfAbsent(maxBound(property), ignore -> new TreeSet<>(Comparator.reverseOrder()))
                     .add(property);
            return build.toString();
        }
    }

    public static RedundantBefore EMPTY = new RedundantBefore();

    private final Ranges staleRanges, locallyRetiredRanges;
    private final TxnId maxBootstrap, maxGcBefore;
    private final TxnId minShardRedundantBefore, minGcBefore;
    private final long maxStartEpoch, minLocallyRetiredEpoch;

    private RedundantBefore()
    {
        staleRanges = locallyRetiredRanges = Ranges.EMPTY;
        maxBootstrap = maxGcBefore = TxnId.NONE;
        minShardRedundantBefore = minGcBefore = TxnId.MAX;
        maxStartEpoch = 0;
        minLocallyRetiredEpoch = Long.MAX_VALUE;
    }

    RedundantBefore(boolean inclusiveEnds, RoutingKey[] starts, Bounds[] values)
    {
        super(inclusiveEnds, starts, values);
        staleRanges = extractRanges(values, b -> b.staleUntilAtLeast != null);
        locallyRetiredRanges = extractRanges(values, Bounds::isLocallyRetired);
        TxnId maxBootstrap = TxnId.NONE, maxGcBefore = TxnId.NONE, minShardRedundantBefore = TxnId.MAX, minGcBefore = TxnId.MAX;
        long minLocallyRetiredEpoch = Long.MAX_VALUE, maxStartEpoch = 0;
        boolean hasLocallyRetired = !locallyRetiredRanges.isEmpty();
        for (Bounds bounds : values)
        {
            if (bounds == null)
                continue;

            {
                TxnId bootstrappedAt = bounds.maxBound(PRE_BOOTSTRAP_OR_STALE);
                if (bootstrappedAt.compareTo(maxBootstrap) > 0)
                    maxBootstrap = bootstrappedAt;
            }
            {
                TxnId gcBefore = bounds.maxBound(GC_BEFORE);
                if (gcBefore.compareTo(maxGcBefore) > 0)
                    maxGcBefore = gcBefore;
                if (gcBefore.compareTo(minGcBefore) < 0)
                    minGcBefore = gcBefore;
            }
            {
                TxnId shardRedundantBefore = bounds.shardRedundantBefore();
                if (shardRedundantBefore.compareTo(minShardRedundantBefore) < 0)
                    minShardRedundantBefore = shardRedundantBefore;
            }
            if (bounds.startEpoch > maxStartEpoch)
                maxStartEpoch = bounds.startEpoch;
            if (hasLocallyRetired && bounds.isLocallyRetired() && bounds.endEpoch < minLocallyRetiredEpoch)
                minLocallyRetiredEpoch = bounds.endEpoch;
        }
        this.maxBootstrap = maxBootstrap;
        this.maxGcBefore = maxGcBefore;
        this.minShardRedundantBefore = minShardRedundantBefore;
        this.minGcBefore = minGcBefore;
        this.maxStartEpoch = maxStartEpoch;
        this.minLocallyRetiredEpoch = minLocallyRetiredEpoch;
        checkParanoid(starts, values);
    }

    private static Ranges extractRanges(Bounds[] values, Predicate<Bounds> include)
    {
        int count = 0;
        for (Bounds bounds : values)
        {
            if (bounds != null && include.test(bounds))
                ++count;
        }

        if (count == 0)
            return Ranges.EMPTY;

        Range[] result = new Range[count];
        count = 0;
        for (Bounds bounds : values)
        {
            if (bounds != null && include.test(bounds))
                result[count++] = bounds.range;
        }
        return Ranges.ofSortedAndDeoverlapped(result).mergeTouching();
    }

    public static RedundantBefore create(AbstractRanges ranges, TxnId bound, RedundantStatus status)
    {
        return create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, bound, status);
    }

    public static RedundantBefore createStale(AbstractRanges ranges, Timestamp staleUntilAtLeast)
    {
        return create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, TxnId.NONE, RedundantStatus.NONE, staleUntilAtLeast);
    }

    public static RedundantBefore create(AbstractRanges ranges, long startEpoch, long endEpoch, TxnId bound, RedundantStatus status)
    {
        return create(ranges, startEpoch, endEpoch, bound, status, null);
    }

    public static RedundantBefore create(AbstractRanges ranges, long startEpoch, long endEpoch, TxnId bound, RedundantStatus status, @Nullable Timestamp staleUntilAtLeast)
    {
        if (ranges.isEmpty())
            return new RedundantBefore();

        Bounds bounds = new Bounds(null, startEpoch, endEpoch, new TxnId[] { bound }, new int[] { status.encoded & ONLY_LE_MASK, status.encoded }, staleUntilAtLeast);
        Builder builder = new Builder(ranges.get(0).endInclusive(), ranges.size() * 2);
        for (int i = 0 ; i < ranges.size() ; ++i)
        {
            Range cur = ranges.get(i);
            builder.append(cur.start(), cur.end(), bounds.withRange(cur));
        }
        return builder.build();
    }

    public static RedundantBefore merge(RedundantBefore a, RedundantBefore b)
    {
        return ReducingIntervalMap.mergeIntervals(a, b, Builder::new);
    }

    public RedundantStatus status(TxnId txnId, @Nullable Timestamp applyAtIfKnown, Participants<?> participants)
    {
        RedundantStatus result = foldl(participants, Bounds::getAndMerge, null, txnId, applyAtIfKnown);
        return result == null ? NOT_OWNED_ONLY : result;
    }

    public RedundantStatus status(TxnId txnId, @Nullable Timestamp applyAtIfKnown, RoutingKey key)
    {
        Bounds bounds = get(key);
        return bounds == null ? NOT_OWNED_ONLY : bounds.get(txnId, applyAtIfKnown);
    }

    public boolean isShardOnlyApplied(TxnId txnId, Unseekables<?> participants)
    {
        return foldl(participants, Bounds::isShardOnlyApplied, false, txnId);
    }

    /**
     * RedundantStatus.REDUNDANT overrides PRE_BOOTSTRAP; to avoid complicating that state machine,
     * for cases where we care independently about the overall pre-bootstrap state we have a separate mechanism
     */
    public Coverage preBootstrapOrStale(TxnId txnId, Participants<?> participants)
    {
        return status(txnId, null, participants).get(PRE_BOOTSTRAP_OR_STALE);
    }

    public <T extends Deps> RangeDeps.BuilderByRange collectDeps(Routables<?> participants, RangeDeps.BuilderByRange builder, EpochSupplier minEpoch, EpochSupplier executeAt)
    {
        return foldl(participants, Bounds::collectDep, builder, minEpoch, executeAt);
    }

    public Ranges validateSafeToRead(Timestamp forBootstrapAt, Ranges ranges)
    {
        return foldl(ranges, Bounds::validateSafeToRead, ranges, forBootstrapAt, null);
    }

    public TxnId min(Routables<?> participants, Function<Bounds, TxnId> get)
    {
        return TxnId.nonNullOrMax(TxnId.NONE, foldl(participants, Bounds::min, null, get));
    }

    public TxnId max(Routables<?> participants, Function<Bounds, TxnId> get)
    {
        return foldl(participants, Bounds::max, TxnId.NONE, get);
    }

    /**
     * Subtract any ranges that are before a GC point
     */
    @VisibleForImplementation
    public Ranges removeGcBefore(TxnId txnId, @Nonnull Timestamp executeAt, Ranges ranges)
    {
        Invariants.requireArgument(executeAt != null, "executeAt must not be null");
        if (txnId.compareTo(maxGcBefore) >= 0)
            return ranges;
        return foldl(ranges, Bounds::withoutBeforeGc, ranges, txnId, executeAt);
    }

    /**
     * Subtract any ranges we consider stale or pre-bootstrap
     */
    public Ranges removeRetired(Ranges ranges)
    {
        return foldl(ranges, Bounds::withoutAnyRetired, ranges, r -> false);
    }

    public TxnId minShardRedundantBefore()
    {
        return minShardRedundantBefore;
    }

    public TxnId minGcBefore()
    {
        return minGcBefore;
    }

    public TxnId maxGcBefore()
    {
        return maxGcBefore;
    }

    /**
     * Subtract any ranges we consider stale or pre-bootstrap
     */
    public Ranges removePreBootstrap(TxnId txnId, Ranges ranges)
    {
        if (maxBootstrap.compareTo(txnId) <= 0)
            return ranges;
        return foldl(ranges, Bounds::withoutPreBootstrap, ranges, txnId, null);
    }

    /**
     * Subtract anything we don't need to coordinate (because they are known to be shard durable),
     * and we don't execute locally, i.e. are pre-bootstrap or stale (or for RX are on ranges that are already retired)
     */
    public Participants<?> expectToOwn(TxnId txnId, @Nullable Timestamp executeAt, Participants<?> participants)
    {
        if (txnId.is(ExclusiveSyncPoint))
        {
            if (!mayFilterStaleOrPreBootstrapOrRetiredOrNotOwned(txnId, executeAt, participants))
                return participants;

            return foldl(participants, Bounds::withoutRedundantAnd_StaleOrPreBootstrapOrRetiredOrNotOwned, participants, txnId, executeAt);
        }
        else
        {
            if (!mayFilterStaleOrPreBootstrap(txnId, participants))
                return participants;

            return foldl(participants, Bounds::withoutRedundantAnd_StaleOrPreBootstrap, participants, txnId, executeAt);
        }
    }

    /**
     * Subtract anything we won't execute locally, i.e. are pre-bootstrap or stale (or for RX are on ranges that are already retired)
     */
    public Participants<?> expectToExecute(TxnId txnId, @Nullable Timestamp executeAt, Participants<?> participants)
    {
        if (!mayFilterStaleOrPreBootstrap(txnId, participants))
            return participants;

        return foldl(participants, Bounds::participantsWithoutStaleOrPreBootstrap, participants, txnId, executeAt);
    }

    /**
     * Subtract anything we won't execute locally, i.e. are pre-bootstrap or stale (or for RX are on ranges that are already retired)
     */
    public Participants<?> expectToWaitOn(TxnId txnId, @Nullable Timestamp executeAt, Participants<?> participants)
    {
        Invariants.require(txnId.is(ExclusiveSyncPoint));
        if (!mayFilterStaleOrPreBootstrapOrRetiredOrNotOwned(txnId, executeAt, participants))
            return participants;

        return foldl(participants, Bounds::withoutStaleOrPreBootstrapOrLocallyRetired, participants, txnId);
    }

    public boolean mayFilter(TxnId txnId, @Nullable Timestamp executeAtIfKnown, Participants<?> participants)
    {
        return mayFilterStaleOrPreBootstrapOrRetiredOrNotOwned(txnId, executeAtIfKnown, participants);
    }

    private boolean mayFilterStaleOrPreBootstrapOrRetiredOrNotOwned(TxnId txnId, @Nullable Timestamp executeAt, Participants<?> participants)
    {
        return (minLocallyRetiredEpoch < txnId.epoch() && locallyRetiredRanges.intersects(participants))
               || (executeAt != null && executeAt.epoch() < maxStartEpoch)
               || mayFilterStaleOrPreBootstrap(txnId, participants);
    }

    private boolean mayFilterStaleOrPreBootstrap(TxnId txnId, Participants<?> participants)
    {
        return maxBootstrap.compareTo(txnId) > 0 || (staleRanges != null && staleRanges.intersects(participants));
    }

    /**
     * Subtract any ranges we consider stale, pre-bootstrap, or that were previously owned and have been retired
     */
    public Participants<?> expectToCalculateDependenciesOrConsultOnRecovery(TxnId txnId, @Nullable Timestamp executeAtIfKnown, Participants<?> participants)
    {
        if (!mayFilterStaleOrPreBootstrapOrRetiredOrNotOwned(txnId, executeAtIfKnown, participants))
            return participants;
        return foldl(participants, Bounds::withoutNotOwnedShardOnlyRedundant, participants, txnId, executeAtIfKnown);
    }

    /**
     * Subtract any ranges we consider stale, pre-bootstrap, or that were previously owned and have been retired
     */
    public Participants<?> expectToOwnOrExecuteOrConsultOnRecovery(TxnId txnId, @Nullable Timestamp executeAtIfKnown, Participants<?> participants)
    {
        if (!mayFilterStaleOrPreBootstrapOrRetiredOrNotOwned(txnId, executeAtIfKnown, participants))
            return participants;
        return foldl(participants, Bounds::withoutRedundantAnd_StaleOrPreBootstrapOrRetiredOrNotOwned, participants, txnId, executeAtIfKnown);
    }

    public static class Builder extends AbstractIntervalBuilder<RoutingKey, Bounds, RedundantBefore>
    {
        public Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected Bounds slice(RoutingKey start, RoutingKey end, Bounds v)
        {
            if (v.range.start().equals(start) && v.range.end().equals(end))
                return v;

            return new Bounds(v.range.newRange(start, end), v.startEpoch, v.endEpoch, v.bounds, v.statuses, v.staleUntilAtLeast);
        }

        @Override
        protected Bounds reduce(Bounds a, Bounds b)
        {
            return Bounds.reduce(a, b);
        }

        @Override
        protected Bounds tryMergeEqual(Bounds a, Bounds b)
        {
            if (!a.equalsIgnoreRange(b))
                return null;

            Invariants.require(a.range.compareIntersecting(b.range) == 0 || a.range.end().equals(b.range.start()) || a.range.start().equals(b.range.end()));
            return new Bounds(a.range.newRange(
                a.range.start().compareTo(b.range.start()) <= 0 ? a.range.start() : b.range.start(),
                a.range.end().compareTo(b.range.end()) >= 0 ? a.range.end() : b.range.end()
            ), a.startEpoch, a.endEpoch, a.bounds, a.statuses, a.staleUntilAtLeast);
        }

        @Override
        public void append(RoutingKey start, RoutingKey end, @Nonnull Bounds value)
        {
            if (value.range.start().compareTo(start) != 0 || value.range.end().compareTo(end) != 0)
                throw illegalState();
            super.append(start, end, value);
        }

        @Override
        protected RedundantBefore buildInternal()
        {
            return new RedundantBefore(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new Bounds[0]));
        }
    }

    private static void checkParanoid(RoutingKey[] starts, Bounds[] values)
    {
        if (!Invariants.isParanoid())
            return;

        for (int i = 0 ; i < values.length ; ++i)
        {
            if (values[i] != null)
            {
                Invariants.requireArgument(starts[i].equals(values[i].range.start()));
                Invariants.requireArgument(starts[i + 1].equals(values[i].range.end()));
            }
        }
    }

    public final void removeRedundantDependencies(Unseekables<?> participants, Command.WaitingOn.Update builder)
    {
        // Note: we do not need to track the bootstraps we implicitly depend upon, because we will not serve any read requests until this has completed
        //  and since we are a timestamp store, and we write only this will sort itself out naturally
        // TODO (required): make sure we have no races on HLC around SyncPoint else this resolution may not work (we need to know the micros equivalent timestamp of the snapshot)
        class KeyState
        {
            Int2ObjectHashMap<RoutingKeys> partiallyBootstrapping;

            /**
             * Are the participating ranges for the txn fully covered by bootstrapping ranges for this command store
             */
            boolean isFullyBootstrapping(Command.WaitingOn.Update builder, Range range, int txnIdx)
            {
                if (builder.directKeyDeps.foldEachKey(txnIdx, range, true, (r0, k, p) -> p && r0.contains(k)))
                    return true;

                if (partiallyBootstrapping == null)
                    partiallyBootstrapping = new Int2ObjectHashMap<>();
                RoutingKeys prev = partiallyBootstrapping.get(txnIdx);
                RoutingKeys remaining = prev;
                if (remaining == null) remaining = builder.directKeyDeps.participants(txnIdx);
                else Invariants.require(!remaining.isEmpty());
                remaining = remaining.without(range);
                if (prev == null) Invariants.require(!remaining.isEmpty());
                partiallyBootstrapping.put(txnIdx, remaining);
                return remaining.isEmpty();
            }
        }

        KeyDeps directKeyDeps = builder.directKeyDeps;
        if (!directKeyDeps.isEmpty())
        {
            foldl(directKeyDeps.keys(), (e, s, d, b) -> {
                // TODO (desired, efficiency): foldlInt so we can track the lower rangeidx bound and not revisit unnecessarily
                // find the txnIdx below which we are known to be fully redundant locally due to having been applied or invalidated
                int bootstrapIdx = d.txnIdsWithFlags().find(e.maxBootstrappedAt());
                if (bootstrapIdx < 0) bootstrapIdx = -1 - bootstrapIdx;
                int appliedIdx = d.txnIdsWithFlags().find(e.maxLocallyAppliedBefore());
                if (appliedIdx < 0) appliedIdx = -1 - appliedIdx;

                // remove intersecting transactions with known redundant txnId
                // note that we must exclude all transactions that are pre-bootstrap, and perform the more complicated dance below,
                // as these transactions may be only partially applied, and we may need to wait for them on another key.
                if (appliedIdx > bootstrapIdx)
                {
                    d.forEach(e.range, bootstrapIdx, appliedIdx, b, s, (b0, s0, txnIdx) -> {
                        b0.removeWaitingOnDirectKeyTxnId(txnIdx);
                    });
                }

                if (bootstrapIdx > 0)
                {
                    d.forEach(e.range, 0, bootstrapIdx, b, s, e.range, (b0, s0, r, txnIdx) -> {
                        if (b0.isWaitingOnDirectKeyTxnIdx(txnIdx) && s0.isFullyBootstrapping(b0, r, txnIdx))
                            b0.removeWaitingOnDirectKeyTxnId(txnIdx);
                    });
                }
                return s;
            }, new KeyState(), directKeyDeps, builder);
        }

        /**
         * If we have to handle bootstrapping ranges for range transactions, these may only partially cover the
         * transaction, in which case we should not remove the transaction as a dependency. But if it is fully
         * covered by bootstrapping ranges then we *must* remove it as a dependency.
         */
        class RangeState
        {
            Range range;
            int bootstrapIdx, appliedIdx;
            Map<Integer, Ranges> partiallyBootstrapping;

            /**
             * Are the participating ranges for the txn fully covered by bootstrapping ranges for this command store
             */
            boolean isFullyBootstrapping(int rangeTxnIdx)
            {
                // if all deps for the txnIdx are contained in the range, don't inflate any shared object state
                if (builder.directRangeDeps.foldEachRange(rangeTxnIdx, range, true, (r1, r2, p) -> p && r1.contains(r2)))
                    return true;

                if (partiallyBootstrapping == null)
                    partiallyBootstrapping = new HashMap<>();
                Ranges prev = partiallyBootstrapping.get(rangeTxnIdx);
                Ranges remaining = prev;
                if (remaining == null) remaining = builder.directRangeDeps.ranges(rangeTxnIdx);
                else Invariants.require(!remaining.isEmpty());
                remaining = remaining.without(Ranges.of(range));
                if (prev == null) Invariants.require(!remaining.isEmpty());
                partiallyBootstrapping.put(rangeTxnIdx, remaining);
                return remaining.isEmpty();
            }
        }

        RangeDeps rangeDeps = builder.directRangeDeps;
        foldl(participants, (e, s, d, b) -> {
            int bootstrapIdx = d.txnIdsWithFlags().find(e.maxBootstrappedAt());
            if (bootstrapIdx < 0) bootstrapIdx = -1 - bootstrapIdx;
            s.bootstrapIdx = bootstrapIdx;

            TxnId locallyAppliedBefore = e.maxLocallyAppliedBefore();
            int appliedIdx = d.txnIdsWithFlags().find(locallyAppliedBefore);
            if (appliedIdx < 0) appliedIdx = -1 - appliedIdx;
            if (locallyAppliedBefore.epoch() >= e.endEpoch)
            {
                // for range transactions, we should not infer that a still-owned range is redundant because a not-owned range that overlaps is redundant
                int altAppliedIdx = d.txnIdsWithFlags().find(TxnId.minForEpoch(e.endEpoch));
                if (altAppliedIdx < 0) altAppliedIdx = -1 - altAppliedIdx;
                if (altAppliedIdx < appliedIdx) appliedIdx = altAppliedIdx;
            }
            s.appliedIdx = appliedIdx;

            // remove intersecting transactions with known redundant txnId
            if (appliedIdx > bootstrapIdx)
            {
                // TODO (desired):
                // TODO (desired): move the bounds check into forEach, matching structure used for keys
                d.forEach(e.range, b, s, (b0, s0, txnIdx) -> {
                    if (txnIdx >= s0.bootstrapIdx && txnIdx < s0.appliedIdx)
                        b0.removeWaitingOnDirectRangeTxnId(txnIdx);
                });
            }

            if (bootstrapIdx > 0)
            {
                // if we have any ranges where bootstrap is involved, we have to do a more complicated dance since
                // this may imply only partial redundancy (we may still depend on the transaction for some other range)
                s.range = e.range;
                // TODO (desired): move the bounds check into forEach, matching structure used for keys
                d.forEach(e.range, b, s, (b0, s0, txnIdx) -> {
                    if (txnIdx < s0.bootstrapIdx && b0.isWaitingOnDirectRangeTxnIdx(txnIdx) && s0.isFullyBootstrapping(txnIdx))
                        b0.removeWaitingOnDirectRangeTxnId(txnIdx);
                });
            }
            return s;
        }, new RangeState(), rangeDeps, builder);
    }

    public final boolean hasLocallyRedundantDependencies(TxnId minimumDependencyId, Timestamp executeAt, Participants<?> participantsOfWaitingTxn)
    {
        // TODO (required): consider race conditions when bootstrapping into an active command store, that may have seen a higher txnId than this?
        //   might benefit from maintaining a per-CommandStore largest TxnId register to ensure we allocate a higher TxnId for our ExclSync,
        //   or from using whatever summary records we have for the range, once we maintain them
        return status(minimumDependencyId, executeAt, participantsOfWaitingTxn).any(LOCALLY_REDUNDANT);
    }

    @Override
    public String toString()
    {
        return "gc:" + toString(GC_BEFORE) + "\nlocal:" + toString(LOCALLY_APPLIED) + "\nbootstrap:" + toString(PRE_BOOTSTRAP);
    }

    private String toString(Property property)
    {
        TreeMap<TxnId, List<Range>> map = new TreeMap<>();
        foldl((e, m, p, o) -> {
            m.computeIfAbsent(e.maxBound(p), ignore -> new ArrayList<>())
             .add(e.range);
            return m;
        }, map, property, null, i -> false);

        return map.descendingMap().entrySet().stream()
                  .map(e -> (e.getKey().equals(TxnId.NONE) ? "none" : e.getKey().toString()) + ":" + Ranges.ofSorted(e.getValue().toArray(new Range[0])).mergeTouching())
                  .collect(Collectors.joining(", ", "{", "}"));
    }
}
