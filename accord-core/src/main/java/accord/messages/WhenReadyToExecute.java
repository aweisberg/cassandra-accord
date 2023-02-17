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

package accord.messages;

import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import accord.primitives.*;

import accord.local.*;
import accord.api.Data;
import accord.topology.Topologies;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static accord.local.Status.Committed;
import static accord.messages.MessageType.EXECUTE_RSP;
import static accord.messages.WhenReadyToExecute.ExecuteNack.NotCommitted;
import static accord.messages.WhenReadyToExecute.ExecuteNack.Redundant;
import static accord.messages.TxnRequest.*;
import static accord.utils.MapReduceConsume.forEach;

// TODO (required, efficiency): dedup - can currently have infinite pending reads that will be executed independently
public abstract class WhenReadyToExecute extends AbstractEpochRequest<WhenReadyToExecute.ExecuteNack> implements CommandListener
{
    public enum ExecuteType
    {
        readData(0),
        waitAndApply(1);

        public final byte val;

        ExecuteType(int val)
        {
            this.val = (byte)val;
        }

        @SuppressWarnings("unused")
        public static ExecuteType fromValue(byte val)
        {
            switch (val)
            {
                case 0:
                    return readData;
                case 1:
                    return waitAndApply;
                default:
                    throw new IllegalArgumentException("Unrecognized ExecuteType value " + val);
            }
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(WhenReadyToExecute.class);

    public final long executeAtEpoch;
    public final Seekables<?, ?> scope; // TODO (low priority, efficiency): this should be RoutingKeys, as we have the Keys locally, but for simplicity we use this to implement keys()
    private final long waitForEpoch;
    private transient boolean isObsolete; // TODO (low priority, semantics): respond with the Executed result we have stored?
    private transient BitSet waitingOn;
    private transient int waitingOnCount;

    public WhenReadyToExecute(Node.Id to, Topologies topologies, TxnId txnId, Seekables<?, ?> scope, Timestamp executeAt)
    {
        super(txnId);
        this.executeAtEpoch = executeAt.epoch();
        int startIndex = latestRelevantEpochIndex(to, topologies, scope);
        // TODO computeScope could create the unseekables directly
        this.scope = computeScope(to, topologies, (Seekables)scope, startIndex, Seekables::slice, Seekables::with);
        this.waitForEpoch = computeWaitForEpoch(to, topologies, startIndex);
    }

    WhenReadyToExecute(TxnId txnId, Seekables<?, ?> scope, long executeAtEpoch, long waitForEpoch)
    {
        super(txnId);
        this.executeAtEpoch = executeAtEpoch;
        this.scope = scope;
        this.waitForEpoch = waitForEpoch;
    }

    @Override
    public long waitForEpoch()
    {
        return waitForEpoch;
    }

    @Override
    protected void process()
    {
        waitingOn = new BitSet();
        node.mapReduceConsumeLocal(this, scope, executeAtEpoch, executeAtEpoch, this);
    }

    @Override
    public PreLoadContext listenerPreLoadContext(TxnId caller)
    {
        Set<TxnId> ids = new HashSet<>();
        ids.add(txnId);
        ids.add(caller);
        return PreLoadContext.contextFor(ids, keys());
    }

    @Override
    public boolean isTransient()
    {
        return true;
    }

    @Override
    public synchronized void onChange(SafeCommandStore safeStore, Command command)
    {
        logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                this, command.txnId(), command.status(), command);
        switch (command.status())
        {
            default: throw new AssertionError();
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
            case Committed:
                return;

            case PreApplied:
            case Applied:
            case Invalidated:
                obsolete();
            case ReadyToExecute:
        }

        command.removeListener(this);
        if (!isObsolete)
            readyToExecute(safeStore, command);
    }

    @Override
    public synchronized ExecuteNack apply(SafeCommandStore safeStore)
    {
        Command command = safeStore.command(txnId);
        Status status = command.status();
        logger.trace("{}: setting up read with status {} on {}", txnId, status, safeStore);
        switch (command.status()) {
            default:
                throw new AssertionError();
            case Committed:
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
                waitingOn.set(safeStore.commandStore().id());
                ++waitingOnCount;
                command.addListener(this);

                if (status == Committed)
                    return null;

                safeStore.progressLog().waiting(txnId, Committed.minKnown, scope.toUnseekables());
                return NotCommitted;

            case ReadyToExecute:
                waitingOn.set(safeStore.commandStore().id());
                ++waitingOnCount;
                if (!isObsolete)
                    readyToExecute(safeStore, command);
                return null;

            case PreApplied:
            case Applied:
            case Invalidated:
                isObsolete = true;
                return Redundant;
        }
    }

    @Override
    public ExecuteNack reduce(ExecuteNack r1, ExecuteNack r2)
    {
        return r1 == null || r2 == null
                ? r1 == null ? r2 : r1
                : r1.compareTo(r2) >= 0 ? r1 : r2;
    }

    // TODO should this be synchronized or can it be removed?
    @Override
    public synchronized void accept(ExecuteNack reply, Throwable failure)
    {
        if (reply != null)
        {
            node.reply(replyTo, replyContext, reply);
        }
        else if (failure != null)
        {
            // TODO (expected, testing): test
            node.reply(replyTo, replyContext, ExecuteNack.Error);
            failed();
            // TODO (expected, exceptions): probably a better way to handle this, as might not be uncaught
            node.agent().onUncaughtException(failure);
            node.commandStores().mapReduceConsume(this, waitingOn.stream(), forEach(in -> in.command(txnId).removeListener(this), node.agent()));
        }
        else
        {
            ack();
        }
    }

    public abstract ExecuteType kind();

    /*
     * Perform any local cleanup needed after failure
     **/
    protected abstract void failed();

    protected abstract void sendSuccessReply();

    abstract protected void readyToExecute(SafeCommandStore safeStore, Command command);

    synchronized protected void onExecuteComplete(CommandStore commandStore)
    {
        Preconditions.checkState(waitingOn.get(commandStore.id()));
        waitingOn.clear(commandStore.id());
        ack();
    }

    void obsolete()
    {
        if (!isObsolete)
        {
            isObsolete = true;
            node.reply(replyTo, replyContext, Redundant);
        }
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return Keys.EMPTY;
    }

    @Override
    public MessageType type()
    {
        return MessageType.EXECUTE_REQ;
    }

    private void ack()
    {
        if (-1 == --waitingOnCount)
            sendSuccessReply();
    }

    public interface ExecuteReply extends Reply
    {
        boolean isOk();
    }

    public enum ExecuteNack implements ExecuteReply
    {
        Invalid, NotCommitted, Redundant, Error;

        @Override
        public String toString()
        {
            return "Execute" + name();
        }

        @Override
        public MessageType type()
        {
            return EXECUTE_RSP;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public boolean isFinal()
        {
            return this != NotCommitted;
        }
    }

    public static class ExecuteOk implements ExecuteReply
    {
        public final @Nullable Data data;

        public ExecuteOk(@Nullable Data data)
        {
            this.data = data;
        }

        @Override
        public String toString()
        {
            return "ExecuteOk{" + data + '}';
        }

        @Override
        public MessageType type()
        {
            return EXECUTE_RSP;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }
    }
}
