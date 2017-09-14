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
package org.reaktivity.nukleus.ws.internal.stream;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.agrona.BitUtil.SIZE_OF_SHORT;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.ws.internal.types.codec.WsHeaderFW.STATUS_NORMAL_CLOSURE;
import static org.reaktivity.nukleus.ws.internal.types.codec.WsHeaderFW.STATUS_PROTOCOL_ERROR;
import static org.reaktivity.nukleus.ws.internal.types.codec.WsHeaderFW.STATUS_UNEXPECTED_CONDITION;
import static org.reaktivity.nukleus.ws.internal.util.BufferUtil.xor;

import java.security.MessageDigest;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.ws.internal.types.Flyweight;
import org.reaktivity.nukleus.ws.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.ws.internal.types.ListFW;
import org.reaktivity.nukleus.ws.internal.types.OctetsFW;
import org.reaktivity.nukleus.ws.internal.types.codec.WsHeaderFW;
import org.reaktivity.nukleus.ws.internal.types.control.RouteFW;
import org.reaktivity.nukleus.ws.internal.types.control.WsRouteExFW;
import org.reaktivity.nukleus.ws.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.ws.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.ws.internal.types.stream.DataFW;
import org.reaktivity.nukleus.ws.internal.types.stream.EndFW;
import org.reaktivity.nukleus.ws.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.ws.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.ws.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.ws.internal.types.stream.WsBeginExFW;
import org.reaktivity.nukleus.ws.internal.types.stream.WsDataExFW;
import org.reaktivity.nukleus.ws.internal.types.stream.WsEndExFW;
import org.reaktivity.nukleus.ws.internal.util.function.LongObjectBiConsumer;

public final class ServerStreamFactory implements StreamFactory
{
    private static final int MAXIMUM_DATA_LENGTH = (1 << Short.SIZE) - 1;

    private static final byte[] HANDSHAKE_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11".getBytes(UTF_8);
    private static final String WEBSOCKET_VERSION_13 = "13";

    private final MessageDigest sha1 = initSHA1();

    private final RouteFW routeRO = new RouteFW();
    private final WsRouteExFW wsRouteExRO = new WsRouteExFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final WsBeginExFW.Builder wsBeginExRW = new WsBeginExFW.Builder();
    private final WsDataExFW.Builder wsDataExRW = new WsDataExFW.Builder();
    private final WsEndExFW.Builder wsEndExRW = new WsEndExFW.Builder();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final OctetsFW octetsRO = new OctetsFW();

    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();

    private final WsBeginExFW wsBeginExRO = new WsBeginExFW();
    private final WsDataExFW wsDataExRO = new WsDataExFW();

    private final WsHeaderFW wsHeaderRO = new WsHeaderFW();
    private final WsHeaderFW.Builder wsHeaderRW = new WsHeaderFW.Builder();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final BufferPool bufferPool;
    private final LongSupplier supplyStreamId;
    private final LongSupplier supplyCorrelationId;

    private final Long2ObjectHashMap<ServerHandshake> correlations;
    private final MessageFunction<RouteFW> wrapRoute;

    public ServerStreamFactory(
        Configuration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<ServerHandshake> correlations)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = requireNonNull(bufferPool);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);
        this.correlations = requireNonNull(correlations);
        this.wrapRoute = this::wrapRoute;
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long sourceRef = begin.sourceRef();

        MessageConsumer newStream = null;

        if (sourceRef == 0L)
        {
            newStream = newConnectReplyStream(begin, throttle);
        }
        else
        {
            newStream = newAcceptStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
        final BeginFW begin,
        final MessageConsumer acceptThrottle)
    {
        final long acceptRef = begin.sourceRef();
        final String acceptName = begin.source().asString();

        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            return acceptRef == route.sourceRef() &&
                    acceptName.equals(route.source().asString());
        };

        final RouteFW route = router.resolve(filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long acceptId = begin.streamId();

            newStream = new ServerAcceptStream(acceptThrottle, acceptId, acceptRef)::handleStream;
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(
        final BeginFW begin,
        final MessageConsumer connectReplyThrottle)
    {
        final long connectReplyId = begin.streamId();

        return new ServerConnectReplyStream(connectReplyThrottle, connectReplyId)::handleStream;
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    private final class ServerAcceptStream
    {
        private final MessageConsumer acceptThrottle;
        private final long acceptId;

        private MessageConsumer connectTarget;
        private long connectId;

        private MessageConsumer streamState;
        private LongObjectBiConsumer<OctetsFW> decodeState;

        private int slotIndex = NO_SLOT;
        private int slotLimit;
        private int slotOffset;

        private int payloadProgress;
        private int payloadLength;
        private int maskingKey;

        private int acceptWindowBytes;
        private int acceptWindowFrames;
        private int sourceWindowBytesAdjustment;
        private int sourceWindowFramesAdjustment;

        private ServerAcceptStream(
            MessageConsumer acceptThrottle,
            long acceptId,
            long acceptRef)
        {
            this.acceptThrottle = acceptThrottle;
            this.acceptId = acceptId;

            this.streamState = this::beforeBegin;
            this.decodeState = this::decodeHeader;
        }

        private void handleStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                doReset(acceptThrottle, acceptId);
            }
        }

        private void afterBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                handleData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
                break;
            default:
                doReset(acceptThrottle, acceptId);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final String acceptName = begin.source().asString();
            final long acceptRef = begin.sourceRef();
            final long correlationId = begin.correlationId();
            final OctetsFW extension = begin.extension();

            // TODO: need lightweight approach (start)
            final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
            final Map<String, String> headers = new LinkedHashMap<>();
            httpBeginEx.headers().forEach(header ->
            {
                final String name = header.name().asString();
                final String value = header.value().asString();
                headers.merge(name, value, (v1, v2) -> String.format("%s, %s", v1, v2));
            });

            final String version = headers.get("sec-websocket-version");
            final String key = headers.get("sec-websocket-key");
            final String protocols = headers.get("sec-websocket-protocol");
            // TODO: need lightweight approach (end)

            if (key != null && WEBSOCKET_VERSION_13.equals(version))
            {
                final MessagePredicate filter = (t, b, o, l) ->
                {
                    final RouteFW route = routeRO.wrap(b, o, l);
                    final WsRouteExFW routeEx = route.extension().get(wsRouteExRO::wrap);
                    final String protocol = routeEx.protocol().asString();

                    return acceptRef == route.sourceRef() &&
                            acceptName.equals(route.source().asString()) &&
                            (protocols == null || protocols.contains(protocol));
                };

                final RouteFW route = router.resolve(filter, wrapRoute);

                if (route != null)
                {
                    final WsRouteExFW wsRouteEx = route.extension().get(wsRouteExRO::wrap);

                    sha1.reset();
                    sha1.update(key.getBytes(US_ASCII));
                    final byte[] digest = sha1.digest(HANDSHAKE_GUID);
                    final Encoder encoder = Base64.getEncoder();
                    final String handshakeHash = new String(encoder.encode(digest), US_ASCII);

                    final String connectName = route.target().asString();
                    final MessageConsumer connectTarget = router.supplyTarget(connectName);
                    final long connectRef = route.targetRef();
                    final long newConnectId = supplyStreamId.getAsLong();
                    final long newCorrelationId = supplyCorrelationId.getAsLong();
                    final String protocol = resolveProtocol(protocols, wsRouteEx.protocol().asString());

                    final ServerHandshake handshake =
                            new ServerHandshake(acceptName, correlationId, handshakeHash, protocol);

                    correlations.put(newCorrelationId, handshake);

                    doWsBegin(connectTarget, newConnectId, connectRef, newCorrelationId, protocol);
                    router.setThrottle(connectName, newConnectId, this::handleThrottle);

                    this.connectTarget = connectTarget;
                    this.connectId = newConnectId;
                }
                else
                {
                    doReset(acceptThrottle, acceptId); // 400
                }
            }
            else
            {
                doReset(acceptThrottle, acceptId); // 404
            }

            this.streamState = this::afterBegin;
        }

        private void handleData(
            DataFW data)
        {
            acceptWindowBytes -= data.length();
            acceptWindowFrames--;

            if (acceptWindowBytes < 0 || acceptWindowFrames < 0)
            {
                doReset(acceptThrottle, acceptId);
            }
            else
            {
                if (acceptWindowBytes == 0 || acceptWindowFrames == 0)
                {
                    doZeroWindow(acceptThrottle, acceptId);
                }

                OctetsFW payload = data.payload();
                if(this.slotLimit != 0)
                {
                    MutableDirectBuffer acceptBuffer = bufferPool.buffer(this.slotIndex, this.slotOffset);
                    acceptBuffer.putBytes(this.slotLimit, payload.buffer(), payload.offset(), payload.sizeof());
                    this.slotLimit += payload.sizeof();
                    payload = octetsRO.wrap(acceptBuffer, 0, slotLimit);
                }

                decodeState.accept(acceptId, payload);
            }
        }

        private void handleEnd(
            EndFW end)
        {
            doWsEnd(connectTarget, connectId, STATUS_NORMAL_CLOSURE);
        }

        private void handleAbort(
            AbortFW abort)
        {
            // TODO: WsAbortEx
            doWsAbort(connectTarget, connectId, STATUS_UNEXPECTED_CONDITION);
        }

        private void decodeHeader(
            final long streamId,
            final OctetsFW httpPayload)
        {
            final DirectBuffer buffer = httpPayload.buffer();
            final int offset = httpPayload.offset();
            final int limit = httpPayload.limit();

            if(wsHeaderRO.canWrap(buffer, offset, limit))
            {
                final WsHeaderFW wsHeader = wsHeaderRO.wrap(buffer, offset, limit);
                if (wsHeader.mask() && wsHeader.maskingKey() != 0L)
                {
                    this.maskingKey = wsHeader.maskingKey();
                    this.payloadLength = wsHeader.length();
                    this.payloadProgress = 0;

                    switch (wsHeader.opcode())
                    {
                    case 1:
                        this.decodeState = this::decodeText;
                        break;
                    case 2:
                        this.decodeState = this::decodeBinary;
                        break;
                    case 8:
                        this.decodeState = this::decodeClose;
                        break;
                    default:
                        throw new IllegalStateException("not yet implemented");
                    }

                    sourceWindowBytesAdjustment += wsHeader.sizeof();

                    httpPayload.wrap(httpPayload.buffer(), httpPayload.offset() + wsHeader.sizeof(), httpPayload.limit());
                    this.decodeState.accept(streamId, httpPayload);
                }
                else
                {
                    doWsAbort(connectTarget, connectId, STATUS_PROTOCOL_ERROR);
                }
            }
            else
            {
                if (this.slotIndex == NO_SLOT)
                {
                    this.slotIndex = bufferPool.acquire(streamId);
                    MutableDirectBuffer acceptBuffer = bufferPool.buffer(slotIndex);
                    acceptBuffer.putBytes(0, buffer, offset, limit);
                    this.slotLimit = limit - offset;
                }
                else
                {
                    this.slotOffset = offset;
                }
            }

            if (offset == limit && this.slotIndex != NO_SLOT)
            {
                bufferPool.release(this.slotIndex);

                this.slotOffset = 0;
                this.slotIndex = NO_SLOT;
            }
        }

        private void decodeText(
            final long streamId,
            final OctetsFW payload)
        {
            // TODO canWrap for UTF-8 split multi-byte characters
            final int payloadSize = payload.sizeof();

            if (payloadSize > 0)
            {
                // TODO: limit acceptReply bytes by acceptReply window, or RESET on overflow?

                final int decodeBytes = Math.min(payloadSize, payloadLength - payloadProgress);

                payload.wrap(payload.buffer(), payload.offset(), payload.offset() + decodeBytes);
                doWsData(connectTarget, connectId, 0x81, maskingKey, payload);

                payloadProgress += decodeBytes;
                maskingKey = (maskingKey >>> decodeBytes & 0x03) | (maskingKey << (Integer.SIZE - decodeBytes & 0x03));

                if (payloadProgress == payloadLength)
                {
                    this.decodeState = this::decodeHeader;
                }

                if (payloadSize > payload.sizeof())
                {
                    sourceWindowFramesAdjustment--;

                    payload.wrap(payload.buffer(), payload.sizeof(), payloadSize);
                    this.decodeState.accept(streamId, payload);
                }
            }
        }

        private void decodeBinary(
            final long streamId,
            final OctetsFW payload)
        {
            final int payloadSize = payload.sizeof();
            if (payloadSize > 0)
            {
                // TODO: limit acceptReply bytes by acceptReply window, or RESET on overflow?

                final int decodeBytes = Math.min(payloadSize, payloadLength - payloadProgress);

                final int payloadLimit = payload.limit();
                payload.wrap(payload.buffer(), payload.offset(), payload.offset() + decodeBytes);
                doWsData(connectTarget, connectId, 0x82, maskingKey, payload);

                payloadProgress += decodeBytes;
                maskingKey = (maskingKey >>> decodeBytes & 0x03) | (maskingKey << (Integer.SIZE - decodeBytes & 0x03));

                if (payloadProgress == payloadLength)
                {
                    this.decodeState = this::decodeHeader;
                }

                if (payloadLimit > payload.limit())
                {
                    sourceWindowFramesAdjustment--;

                    payload.wrap(payload.buffer(), payload.limit(), payloadLimit);
                    this.decodeState.accept(streamId, payload);
                }
            }
        }

        private void decodeClose(
            final long streamId,
            final OctetsFW payload)
        {
            // canWrap?
            final short status = payload.sizeof() >=
                SIZE_OF_SHORT ? payload.buffer().getShort(payload.offset()) : STATUS_NORMAL_CLOSURE;

            doWsEnd(connectTarget, connectId, status);
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
                handleWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                handleReset(reset);
                break;
            default:
                // ignore
                break;
            }
        }

        private void handleWindow(
            WindowFW window)
        {
            final int targetWindowBytesDelta = window.update();
            final int targetWindowFramesDelta = window.frames();

            final int sourceWindowBytesDelta = targetWindowBytesDelta + sourceWindowBytesAdjustment;
            final int sourceWindowFramesDelta = targetWindowFramesDelta + sourceWindowFramesAdjustment;

            acceptWindowBytes += Math.max(sourceWindowBytesDelta, 0);
            sourceWindowBytesAdjustment = Math.min(sourceWindowBytesDelta, 0);

            acceptWindowFrames += Math.max(sourceWindowFramesDelta, 0);
            sourceWindowFramesAdjustment = Math.min(sourceWindowFramesDelta, 0);

            if (sourceWindowBytesDelta > 0 || sourceWindowFramesDelta > 0)
            {
                doWindow(acceptThrottle, acceptId, Math.max(sourceWindowBytesDelta, 0), Math.max(sourceWindowFramesDelta, 0));
            }
        }

        private void handleReset(
            ResetFW reset)
        {
            doReset(acceptThrottle, acceptId);
        }
    }

    private final class ServerConnectReplyStream
    {
        private final MessageConsumer connectReplyThrottle;
        private final long connectReplyId;

        private MessageConsumer acceptReply;
        private long acceptReplyId;

        private MessageConsumer streamState;

        private int targetWindowBytes;
        private int targetWindowFrames;
        private int targetWindowBytesAdjustment;
        private int targetWindowFramesAdjustment;

        private Consumer<WindowFW> windowHandler;

        private ServerConnectReplyStream(
            MessageConsumer connectReplyThrottle,
            long connectReplyId)
        {
            this.connectReplyThrottle = connectReplyThrottle;
            this.connectReplyId = connectReplyId;
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                doReset(connectReplyThrottle, connectReplyId);
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
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                handleData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
                break;
            default:
                doReset(connectReplyThrottle, connectReplyId);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final long connectRef = begin.sourceRef();
            final long correlationId = begin.correlationId();

            final ServerHandshake handshake = correlations.remove(correlationId);

            if (connectRef == 0L && handshake != null)
            {
                final String acceptReplyName = handshake.acceptName();

                final MessageConsumer newAcceptReply = router.supplyTarget(acceptReplyName);
                final long newAcceptReplyId = supplyStreamId.getAsLong();
                final long newCorrelationId = handshake.correlationId();
                String handshakeHash = handshake.handshakeHash();
                String protocol = handshake.protocol();

                doHttpBegin(newAcceptReply, newAcceptReplyId, 0L, newCorrelationId, setHttpHeaders(handshakeHash, protocol));
                router.setThrottle(acceptReplyName, newAcceptReplyId, this::handleThrottle);

                this.acceptReply = newAcceptReply;
                this.acceptReplyId = newAcceptReplyId;

                this.streamState = this::afterBeginOrData;
                this.windowHandler = this::processInitialWindow;
            }
            else
            {
                doReset(connectReplyThrottle, connectReplyId);
            }
        }

        private void handleData(
            DataFW data)
        {
            targetWindowBytes -= data.length();
            targetWindowFrames--;

            if (targetWindowBytes < 0 || targetWindowFrames < 0)
            {
                doReset(connectReplyThrottle, connectReplyId);
            }
            else
            {
                if (targetWindowBytes == 0 || targetWindowFrames == 0)
                {
                    doZeroWindow(connectReplyThrottle, connectReplyId);
                }

                final OctetsFW payload = data.payload();
                final OctetsFW extension = data.extension();

                int flags = 0x82;
                if (extension.sizeof() > 0)
                {
                    final WsDataExFW wsDataEx = extension.get(wsDataExRO::wrap);
                    flags = wsDataEx.flags();
                }

                final int wsHeaderSize = doHttpData(acceptReply, acceptReplyId, payload, flags);

                targetWindowBytesAdjustment -= wsHeaderSize;
                if (payload.sizeof() + wsHeaderSize > MAXIMUM_DATA_LENGTH)
                {
                    targetWindowFramesAdjustment--;
                }
            }
        }

        private void handleEnd(
            EndFW end)
        {
            doHttpEnd(acceptReply, acceptReplyId);
        }

        private void handleAbort(
            AbortFW abort)
        {
            doHttpAbort(acceptReply, acceptReplyId);
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
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                handleReset(reset);
                break;
            default:
                // ignore
                break;
            }
        }

        private void processInitialWindow(WindowFW window)
        {
            final int sourceWindowBytesDelta = window.update();

            targetWindowBytesAdjustment -= sourceWindowBytesDelta * 20 / 100;

            this.windowHandler = this::processWindow;
            this.windowHandler.accept(window);
        }

        private void processWindow(WindowFW window)
        {
            final int sourceWindowBytesDelta = window.update();
            final int sourceWindowFramesDelta = window.frames();

            final int targetWindowBytesDelta = sourceWindowBytesDelta + targetWindowBytesAdjustment;
            final int targetWindowFramesDelta = sourceWindowFramesDelta + targetWindowFramesAdjustment;

            targetWindowBytes += Math.max(targetWindowBytesDelta, 0);
            targetWindowBytesAdjustment = Math.min(targetWindowBytesDelta, 0);

            targetWindowFrames += Math.max(targetWindowFramesDelta, 0);
            targetWindowFramesAdjustment = Math.min(targetWindowFramesDelta, 0);

            if (targetWindowBytesDelta > 0 || targetWindowFramesDelta > 0)
            {
                doWindow(connectReplyThrottle, connectReplyId,
                        Math.max(targetWindowBytesDelta, 0), Math.max(targetWindowFramesDelta, 0));
            }
        }

        private void handleReset(
            ResetFW reset)
        {
            doReset(connectReplyThrottle, connectReplyId);
        }
    }

    private void doHttpBegin(
        MessageConsumer stream,
        long targetId,
        long targetRef,
        long correlationId,
        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .source("ws")
                .sourceRef(targetRef)
                .correlationId(correlationId)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();

        stream.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private Flyweight.Builder.Visitor visitHttpBeginEx(
        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)
    {
        return (buffer, offset, limit) ->
            httpBeginExRW.wrap(buffer, offset, limit)
                         .headers(headers)
                         .build()
                         .sizeof();
    }

    private int doHttpData(
        MessageConsumer stream,
        long targetId,
        OctetsFW payload,
        int flagsAndOpcode)
    {
        final int payloadSize = payload.sizeof();

        WsHeaderFW wsHeader = wsHeaderRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                        .length(payloadSize)
                                        .flagsAndOpcode(flagsAndOpcode)
                                        .build();

        final int wsHeaderSize = wsHeader.sizeof();
        final int payloadFragmentSize = Math.min(MAXIMUM_DATA_LENGTH - wsHeaderSize,  payloadSize);

        DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .payload(p -> p.set((b, o, m) -> wsHeaderSize)
                               .put(payload.buffer(), payload.offset(), payloadFragmentSize))
                .build();

        stream.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());

        final int payloadRemaining = payloadSize - payloadFragmentSize;
        if (payloadRemaining > 0)
        {
            DataFW data2 = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .payload(p -> p.set(payload.buffer(), payload.offset() + payloadFragmentSize, payloadRemaining))
                .build();

            stream.accept(data2.typeId(), data2.buffer(), data2.offset(), data2.sizeof());
        }

        return wsHeaderSize;
    }

    private void doHttpEnd(
        MessageConsumer stream,
        long streamId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .streamId(streamId)
                               .build();

        stream.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doHttpAbort(
        MessageConsumer stream,
        long streamId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(streamId)
                                     .build();

        stream.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doWsBegin(
        MessageConsumer stream,
        long streamId,
        long streamRef,
        long correlationId,
        String protocol)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(streamId)
                                     .source("ws")
                                     .sourceRef(streamRef)
                                     .correlationId(correlationId)
                                     .extension(e -> e.set(visitWsBeginEx(protocol)))
                                     .build();

        stream.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doWsData(
        MessageConsumer stream,
        long streamId,
        int flags,
        int maskKey,
        OctetsFW payload)
    {
        final int capacity = payload.sizeof();
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .payload(p -> p.set(payload).set((b, o, l) -> xor(b, o, o + capacity, maskKey)))
                .extension(e -> e.set(visitWsDataEx(flags)))
                .build();

        stream.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private Flyweight.Builder.Visitor visitWsDataEx(
        int flags)
    {
        return (buffer, offset, limit) ->
            wsDataExRW.wrap(buffer, offset, limit)
                      .flags(flags)
                      .build()
                      .sizeof();
    }

    private void doWsAbort(
        MessageConsumer stream,
        long streamId,
        short code)
    {
        // TODO: WsAbortEx
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .build();

        stream.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doWsEnd(
        MessageConsumer stream,
        long streamId,
        short code)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .streamId(streamId)
                               .extension(e -> e.set(visitWsEndEx(code)))
                               .build();

        stream.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private Flyweight.Builder.Visitor visitWsBeginEx(
        String protocol)
    {
        return (buffer, offset, limit) ->
            protocol == null ? 0 :
            wsBeginExRW.wrap(buffer, offset, limit)
                       .protocol(protocol)
                       .build()
                       .sizeof();
    }

    private Flyweight.Builder.Visitor visitWsEndEx(
        short code)
    {
        return (buffer, offset, limit) ->
            wsEndExRW.wrap(buffer, offset, limit)
                     .code(code)
                     .reason("")
                     .build()
                     .sizeof();
    }

    private void doWindow(
        final MessageConsumer throttle,
        final long throttleId,
        final int writableBytes,
        final int writableFrames)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .update(writableBytes)
                .frames(writableFrames)
                .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doZeroWindow(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .update(0)
                .frames(0)
                .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .streamId(throttleId)
               .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private static String resolveProtocol(
        final String protocols,
        final String protocol)
    {
        return (protocols != null) && protocols.contains(protocol) ? protocol : null;
    }

    private static MessageDigest initSHA1()
    {
        try
        {
            return MessageDigest.getInstance("SHA-1");
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
            return null;
        }
    }
}
