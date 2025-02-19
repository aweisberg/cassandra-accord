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

package accord.primitives;

import accord.api.RoutingKey;
import accord.utils.Invariants;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface RoutableKey extends Routable, Comparable<RoutableKey>
{
    @Override
    int compareTo(@Nonnull RoutableKey that);

    @Override
    default Domain domain() { return Domain.Key; }

    @Override
    RoutingKey toUnseekable();

    default int compareAsRoutingKey(@Nonnull RoutableKey that) { return toUnseekable().compareTo(that.toUnseekable()); }

    /**
     * Some suffix that, combined with prefix(), uniquely identifies the Key.
     *
     * TODO (expected): distinguish this from printableSuffix
     */
    Object suffix();

    default Object printableSuffix() { return suffix(); }

    @Override default RoutingKey someIntersectingRoutingKey(@Nullable Ranges ranges)
    {
        Invariants.paranoid(ranges == null || ranges.contains(this));
        return toUnseekable();
    }
}
