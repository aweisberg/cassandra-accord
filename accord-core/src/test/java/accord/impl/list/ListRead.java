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

package accord.impl.list;

import accord.api.*;
import accord.impl.basic.DelayedCommandStores;
import accord.impl.basic.SimulatedDelayedExecutorService;
import accord.local.CommandStore;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.*;
import accord.primitives.Ranges;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public class ListRead implements Read
{
    private static final Logger logger = LoggerFactory.getLogger(ListRead.class);

    private final Function<CommandStore, ListExecutor> executor;
    public final Seekables<?, ?> readKeys;
    public final Seekables<?, ?> keys;

    public ListRead(Function<CommandStore, ListExecutor> executor, Seekables<?, ?> readKeys, Seekables<?, ?> keys)
    {
        this.executor = executor;
        this.readKeys = readKeys;
        this.keys = keys;
    }

    @Override
    public Seekables keys()
    {
        return keys;
    }

    @Override
    public AsyncChain<Data> read(Seekable key, Txn.Kind kind, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
    {
        ListStore s = (ListStore)store;
        return executor.apply(commandStore.commandStore()).submit(() -> {
            ListData result = new ListData();
            switch (key.domain())
            {
                default: throw new AssertionError();
                case Key:
                    int[] data = s.get((Key)key);
                    logger.trace("READ on {} at {} key:{} -> {}", s.node, executeAt, key, data);
                    result.put((Key)key, data);
                    break;
                case Range:
                    for (Map.Entry<Key, int[]> e : s.get((Range)key))
                        result.put(e.getKey(), e.getValue());
            }
            return result;
        }, 1, TimeUnit.SECONDS);
    }

    @Override
    public Read slice(Ranges ranges)
    {
        return new ListRead(executor, readKeys.slice(ranges), keys.slice(ranges));
    }

    @Override
    public Read merge(Read other)
    {
        return new ListRead(executor, ((Seekables)readKeys).with(((ListRead)other).readKeys), ((Seekables)keys).with(((ListRead)other).keys));
    }

    @Override
    public String toString()
    {
        return keys.toString();
    }
}
