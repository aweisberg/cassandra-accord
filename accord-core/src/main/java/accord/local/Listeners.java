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

import java.util.Collection;
import java.util.function.Predicate;

import accord.utils.DeterministicIdentitySet;

public class Listeners<L extends Command.Listener> extends DeterministicIdentitySet<L>
{
    public static Listeners EMPTY = new Listeners<>();

    public Listeners()
    {
    }

    public Listeners(Listeners<L> copy)
    {
        super(copy);
    }

    public static class Immutable<L extends Command.Listener> extends Listeners<L>
    {
        public static final Immutable EMPTY = new Immutable();

        private Immutable()
        {
            super();
        }

        public Immutable(Listeners<L> listeners)
        {
            super(listeners);
        }

        Listeners<L> mutable()
        {
            return new Listeners<>(this);
        }

        @Override
        public boolean add(L item)
        {
            throw new UnsupportedOperationException("Cannot modify immutable set");
        }

        @Override
        public boolean remove(Object item)
        {
            throw new UnsupportedOperationException("Cannot modify immutable set");
        }

        @Override
        public boolean removeAll(Collection<?> c)
        {
            throw new UnsupportedOperationException("Cannot modify immutable set");
        }

        @Override
        public boolean addAll(Collection<? extends L> c)
        {
            throw new UnsupportedOperationException("Cannot modify immutable set");
        }

        @Override
        public boolean retainAll(Collection<?> c)
        {
            throw new UnsupportedOperationException("Cannot modify immutable set");
        }

        @Override
        public boolean removeIf(Predicate<? super L> filter)
        {
            throw new UnsupportedOperationException("Cannot modify immutable set");
        }
    }

    public static class ImmutableDurable extends Immutable<Command.DurableAndIdempotentListener>
    {
        public static final ImmutableDurable EMPTY = new ImmutableDurable();
    }

    public static class ImmutableTransient extends Immutable<Command.TransientListener>
    {
        public static final ImmutableTransient EMPTY = new ImmutableTransient();

        public ImmutableTransient()
        {
            super();
        }

        public ImmutableTransient(Listeners<Command.TransientListener> listeners)
        {
            super(listeners);
        }

        public ImmutableTransient copyOf(Listeners<Command.TransientListener> listeners)
        {
            if (listeners instanceof ImmutableTransient)
                return (ImmutableTransient)listeners;
            return new ImmutableTransient(listeners);
        }
    }
}
