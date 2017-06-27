/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.ws.internal.routable.stream;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.ws.internal.routable.Source;
import org.reaktivity.nukleus.ws.internal.routable.Target;
import org.reaktivity.nukleus.ws.internal.router.Correlation;
import org.reaktivity.nukleus.ws.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.ws.internal.types.ListFW;
import org.reaktivity.nukleus.ws.internal.types.OctetsFW;
import org.reaktivity.nukleus.ws.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.ws.internal.types.stream.DataFW;
import org.reaktivity.nukleus.ws.internal.types.stream.EndFW;
import org.reaktivity.nukleus.ws.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.ws.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.ws.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.ws.internal.types.stream.WsBeginExFW;
import org.reaktivity.nukleus.ws.internal.types.stream.WsDataExFW;

public final class TargetOutputEstablishedStreamFactory
{
    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final WsBeginExFW wsBeginExRO = new WsBeginExFW();
    private final WsDataExFW wsDataExRO = new WsDataExFW();

    private final Source source;
    private final Function<String, Target> supplyTarget;
    private final LongSupplier supplyStreamId;
    private final LongFunction<Correlation> correlateEstablished;

    public TargetOutputEstablishedStreamFactory(
        Source source,
        Function<String, Target> supplyTarget,
        LongSupplier supplyStreamId,
        LongFunction<Correlation> correlateEstablished)
    {
        this.source = source;
        this.supplyTarget = supplyTarget;
        this.supplyStreamId = supplyStreamId;
        this.correlateEstablished = correlateEstablished;
    }

    public MessageHandler newStream()
    {
        return new TargetOutputEstablishedStream()::handleStream;
    }

    private final class TargetOutputEstablishedStream
    {
        private MessageHandler streamState;

        private long sourceId;

        private Target target;
        private long targetId;

        private int targetWindowBytes;
        private int targetWindowFrames;
        private int targetWindowBytesAdjustment;
        private int targetWindowFramesAdjustment;

        private Consumer<WindowFW> windowHandler;

        private TargetOutputEstablishedStream()
        {
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            streamState.onMessage(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                processBegin(buffer, index, length);
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
        }

        private void afterBeginOrData(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                processData(buffer, index, length);
                break;
            case EndFW.TYPE_ID:
                processEnd(buffer, index, length);
                break;
            default:
                processUnexpected(buffer, index, length);
                break;
            }
        }

        private void afterEnd(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            processUnexpected(buffer, index, length);
        }

        private void afterRejectOrReset(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == DataFW.TYPE_ID)
            {
                dataRO.wrap(buffer, index, index + length);
                final long streamId = dataRO.streamId();

                source.doWindow(streamId, length, 1);
            }
            else if (msgTypeId == EndFW.TYPE_ID)
            {
                endRO.wrap(buffer, index, index + length);
                final long streamId = endRO.streamId();

                source.removeStream(streamId);

                this.streamState = this::afterEnd;
            }
        }

        private void processUnexpected(
            DirectBuffer buffer,
            int index,
            int length)
        {
            frameRO.wrap(buffer, index, index + length);

            final long streamId = frameRO.streamId();

            source.doReset(streamId);

            this.streamState = this::afterRejectOrReset;
        }

        private void processBegin(
            DirectBuffer buffer,
            int index,
            int length)
        {
            final BeginFW begin = beginRO.wrap(buffer, index, index + length);

            final long sourceId = begin.streamId();
            final long sourceRef = begin.sourceRef();
            final long targetCorrelationId = begin.correlationId();

            final Correlation correlation = correlateEstablished.apply(targetCorrelationId);

            if (sourceRef == 0L && correlation != null)
            {
                final Target newTarget = supplyTarget.apply(correlation.source());
                final long newTargetId = supplyStreamId.getAsLong();
                final long sourceCorrelationId = correlation.id();
                String sourceHash = correlation.hash();
                String protocol = correlation.protocol();

                newTarget.doHttpBegin(newTargetId, 0L, sourceCorrelationId, setHttpHeaders(sourceHash, protocol));
                newTarget.addThrottle(newTargetId, this::handleThrottle);

                this.sourceId = sourceId;
                this.target = newTarget;
                this.targetId = newTargetId;

                this.streamState = this::afterBeginOrData;
                this.windowHandler = this::processInitialWindow;
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
        }

        private void processData(
            DirectBuffer buffer,
            int index,
            int length)
        {
            final DataFW data = dataRO.wrap(buffer, index, index + length);

            targetWindowBytes -= data.length();
            targetWindowFrames--;

            if (targetWindowBytes < 0 || targetWindowFrames < 0)
            {
                processUnexpected(buffer, index, length);
            }
            else
            {
                if (targetWindowBytes == 0 || targetWindowFrames == 0)
                {
                    source.doWindow(sourceId, 0, 0);
                }

                final OctetsFW payload = data.payload();
                final OctetsFW extension = data.extension();

                int flags = 0x82;
                if (extension.sizeof() > 0)
                {
                    final WsDataExFW wsDataEx = extension.get(wsDataExRO::wrap);
                    flags = wsDataEx.flags();
                }

                final int wsHeaderSize = target.doHttpData(targetId, payload, flags);

                targetWindowBytesAdjustment -= wsHeaderSize;
                if (payload.sizeof() + wsHeaderSize > Target.MAXIMUM_DATA_LENGTH)
                {
                    targetWindowFramesAdjustment--;
                }
            }
        }

        private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);

            target.doHttpEnd(targetId);
            target.removeThrottle(targetId);
            source.removeStream(sourceId);
        }

        private Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> setHttpHeaders(
            String handshakeHash,
            String protocol)
        {
            return headers ->
            {
                headers.item(h -> h.name(":status").value("101"));
                headers.item(h -> h.name("upgrade").value("websocket"));
                headers.item(h -> h.name("connection").value("upgrade"));
                headers.item(h -> h.name("sec-websocket-accept").value(handshakeHash));

                // TODO: auto-exclude header if value is null
                final OctetsFW extension = beginRO.extension();
                if (extension.sizeof() > 0)
                {
                    final WsBeginExFW wsBeginEx = extension.get(wsBeginExRO::wrap);
                    final String wsProtocol = wsBeginEx.protocol().asString();
                    final String negotiated = wsProtocol == null ? protocol : wsProtocol;
                    if (negotiated != null)
                    {
                        headers.item(h -> h.name("sec-websocket-protocol").value(negotiated));
                    }
                }
                else if (protocol != null)
                {
                    headers.item(h -> h.name("sec-websocket-protocol").value(protocol));
                }
            };
        }

        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                this.windowHandler.accept(window);
                break;
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        public void processInitialWindow(WindowFW window)
        {
            final int sourceWindowBytesDelta = window.update();
            final int sourceWindowFramesDelta = window.frames();

            final int targetWindowBytesDelta = sourceWindowBytesDelta + targetWindowBytesAdjustment;
            final int targetWindowFramesDelta = sourceWindowFramesDelta + targetWindowFramesAdjustment;

            targetWindowBytes += Math.max(targetWindowBytesDelta, 0);
            targetWindowBytesAdjustment = Math.abs(Math.min(targetWindowBytesDelta, 0));
            targetWindowBytesAdjustment -= sourceWindowBytesDelta * 20 / 100;

            targetWindowFrames += Math.max(targetWindowFramesDelta, 0);
            targetWindowFramesAdjustment = Math.abs(Math.min(targetWindowFramesDelta, 0));

            if (targetWindowBytesDelta > 0 || targetWindowFramesDelta > 0)
            {
                source.doWindow(sourceId, targetWindowBytesDelta, Math.max(targetWindowFramesDelta, 0));
                this.windowHandler = this::processWindow;
            }
        }

        private void processWindow(WindowFW window)
        {
            final int sourceWindowBytesDelta = window.update();
            final int sourceWindowFramesDelta = window.frames();

            final int targetWindowBytesDelta = sourceWindowBytesDelta + targetWindowBytesAdjustment;
            final int targetWindowFramesDelta = sourceWindowFramesDelta + targetWindowFramesAdjustment;

            targetWindowBytes += Math.max(targetWindowBytesDelta, 0);
            targetWindowBytesAdjustment = Math.abs(Math.min(targetWindowBytesDelta, 0));

            targetWindowFrames += Math.max(targetWindowFramesDelta, 0);
            targetWindowFramesAdjustment = Math.abs(Math.min(targetWindowFramesDelta, 0));

            if (targetWindowBytesDelta > 0 || targetWindowFramesDelta > 0)
            {
                source.doWindow(sourceId, targetWindowBytesDelta, Math.max(targetWindowFramesDelta, 0));
            }
        }

        private void processReset(
            DirectBuffer buffer,
            int index,
            int length)
        {
            resetRO.wrap(buffer, index, index + length);

            source.doReset(sourceId);
        }
    }
}
