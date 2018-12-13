/**
 * Copyright 2016-2018 The Reaktivity Project
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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.nativeOrder;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.ws.internal.types.codec.WsHeaderFW.STATUS_NORMAL_CLOSURE;
import static org.reaktivity.nukleus.ws.internal.types.codec.WsHeaderFW.STATUS_PROTOCOL_ERROR;
import static org.reaktivity.nukleus.ws.internal.types.codec.WsHeaderFW.STATUS_UNEXPECTED_CONDITION;
import static org.reaktivity.nukleus.ws.internal.util.BufferUtil.xor;

import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
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

public final class ServerStreamFactory implements StreamFactory
{
    private static final int MAXIMUM_CONTROL_FRAME_PAYLOAD_SIZE = 125;
    private static final int MAXIMUM_DATA_LENGTH = (1 << Short.SIZE) - 1;

    private static final byte[] HANDSHAKE_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11".getBytes(UTF_8);
    private static final String WEBSOCKET_VERSION_13 = "13";
    private static final int MAXIMUM_HEADER_SIZE = 14;

    private static final DirectBuffer CLOSE_PAYLOAD = new UnsafeBuffer(new byte[0]);

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

    private final OctetsFW payload = new OctetsFW();

    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();

    private final WsBeginExFW wsBeginExRO = new WsBeginExFW();
    private final WsDataExFW wsDataExRO = new WsDataExFW();

    private final WsHeaderFW wsHeaderRO = new WsHeaderFW();
    private final WsHeaderFW.Builder wsHeaderRW = new WsHeaderFW.Builder();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final LongSupplier supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final LongSupplier supplyCorrelationId;

    private final Long2ObjectHashMap<ServerHandshake> correlations;
    private final MessageFunction<RouteFW> wrapRoute;

    public ServerStreamFactory(
        Configuration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongSupplier supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<ServerHandshake> correlations)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
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
        final long acceptRouteId = begin.routeId();
        final long acceptRef = begin.sourceRef();
        final String acceptName = begin.source().asString();

        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, o + l);
            return acceptRef == route.sourceRef() &&
                    acceptName.equals(route.source().asString());
        };

        final RouteFW route = router.resolve(begin.authorization(), filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long acceptId = begin.streamId();

            newStream = new ServerAcceptStream(acceptThrottle, acceptRouteId, acceptId, acceptRef)::handleStream;
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(
        final BeginFW begin,
        final MessageConsumer connectReplyThrottle)
    {
        final long connectRouteId = begin.routeId();
        final long connectReplyId = begin.streamId();

        return new ServerConnectReplyStream(connectReplyThrottle, connectRouteId, connectReplyId)::handleStream;
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
        private static final String WEBSOCKET_UPGRADE = "websocket";

        private final MessageConsumer acceptThrottle;
        private final long acceptRouteId;
        private final long acceptId;

        private MessageConsumer connectTarget;
        private long connectRouteId;
        private long connectId;

        private MessageConsumer streamState;
        private DecoderState decodeState;

        private MutableDirectBuffer header;
        private int headerLength;

        private int payloadProgress;
        private int payloadLength;
        private int maskingKey;

        private int acceptBudget;
        private int acceptPadding;
        private int connectBudget;
        private int connectPadding;
        private long connectTraceId;

        private int statusLength;
        private MutableDirectBuffer status;

        private ServerAcceptStream(
            MessageConsumer acceptThrottle,
            long acceptRouteId,
            long acceptId,
            long acceptRef)
        {
            this.acceptThrottle = acceptThrottle;
            this.acceptRouteId = acceptRouteId;
            this.acceptId = acceptId;

            header = new UnsafeBuffer(new byte[MAXIMUM_HEADER_SIZE]);
            status = new UnsafeBuffer(new byte[2]);

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
                doReset(acceptThrottle, acceptRouteId, acceptId, 0L);
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
                doReset(acceptThrottle, acceptRouteId, acceptId, 0L);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final long acceptId = begin.streamId();
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

            final String upgrade = headers.get("upgrade");
            final String version = headers.get("sec-websocket-version");
            final String key = headers.get("sec-websocket-key");
            final String protocols = headers.get("sec-websocket-protocol");
            // TODO: need lightweight approach (end)

            if (upgrade == null)
            {
                final String acceptReplyName = acceptName;
                final MessageConsumer newAcceptReply = router.supplyTarget(acceptReplyName);
                final long newAcceptReplyId = supplyReplyId.applyAsLong(acceptId);
                doHttpBegin(newAcceptReply, acceptRouteId, newAcceptReplyId, 0L, correlationId, supplyTraceId.getAsLong(),
                        hs -> hs.item(h -> h.name(":status").value("400"))
                                .item(h -> h.name("connection").value("close")));
                doHttpEnd(newAcceptReply, acceptRouteId, newAcceptReplyId, supplyTraceId.getAsLong());
                this.streamState = (t, b, o, l) -> {};
            }
            else if (key != null &&
                    WEBSOCKET_UPGRADE.equalsIgnoreCase(upgrade) &&
                    WEBSOCKET_VERSION_13.equals(version))
            {
                final MessagePredicate filter = (t, b, o, l) ->
                {
                    final RouteFW route = routeRO.wrap(b, o, o + l);
                    final WsRouteExFW routeEx = route.extension().get(wsRouteExRO::wrap);
                    final String protocol = routeEx.protocol().asString();

                    return acceptRef == route.sourceRef() &&
                            acceptName.equals(route.source().asString()) &&
                            (protocols == null || protocols.contains(protocol));
                };

                final RouteFW route = router.resolve(begin.authorization(), filter, wrapRoute);

                if (route != null)
                {
                    final long traceId = begin.trace();
                    final WsRouteExFW wsRouteEx = route.extension().get(wsRouteExRO::wrap);

                    sha1.reset();
                    sha1.update(key.getBytes(US_ASCII));
                    final byte[] digest = sha1.digest(HANDSHAKE_GUID);
                    final Encoder encoder = Base64.getEncoder();
                    final String handshakeHash = new String(encoder.encode(digest), US_ASCII);

                    final long connectRouteId = route.correlationId();
                    final String connectName = route.target().asString();
                    final MessageConsumer connectTarget = router.supplyTarget(connectName);
                    final long connectRef = route.targetRef();
                    final long newConnectId = supplyInitialId.getAsLong();
                    final long newCorrelationId = supplyCorrelationId.getAsLong();
                    final String protocol = resolveProtocol(protocols, wsRouteEx.protocol().asString());

                    final ServerHandshake handshake =
                            new ServerHandshake(acceptRouteId, acceptId, acceptName, correlationId, handshakeHash, protocol);

                    correlations.put(newCorrelationId, handshake);

                    doWsBegin(connectTarget, connectRouteId, newConnectId, connectRef, newCorrelationId, traceId, protocol);
                    router.setThrottle(connectName, newConnectId, this::handleThrottle);

                    this.connectTarget = connectTarget;
                    this.connectRouteId = connectRouteId;
                    this.connectId = newConnectId;

                    this.streamState = this::afterBegin;
                }
                else
                {
                    doReset(acceptThrottle, acceptRouteId, acceptId, 0L); // 400
                }
            }
            else
            {
                doReset(acceptThrottle, acceptRouteId, acceptId, 0L); // 404
            }
        }

        private void handleData(
            DataFW data)
        {
            acceptBudget -= data.length() + data.padding();

            if (acceptBudget < 0)
            {
                doReset(acceptThrottle, acceptRouteId, acceptId, 0L);
            }
            else
            {
                connectTraceId = data.trace();

                OctetsFW payload = dataRO.payload();
                int offset = payload.offset();
                int length = payload.sizeof();
                while (length > 0)
                {
                    int consumed = decodeState.decode(dataRO.buffer(), offset, length);
                    offset += consumed;
                    length -= consumed;
                }

                // Since we have two decoding states for a frame, the following is
                // needed to handle empty close, empty ping etc. Otherwise, it will be
                // delayed until next handleData() (which may not come for e.g empty close frame)
                if (payloadLength == 0)
                {
                    decodeState.decode(dataRO.buffer(), 0, 0);
                }
            }

        }

        private void handleEnd(
            EndFW end)
        {
            final long traceId = end.trace();
            doWsEnd(connectTarget, connectRouteId, connectId, traceId, STATUS_NORMAL_CLOSURE);
        }

        private void handleAbort(
            AbortFW abort)
        {
            final long traceId = abort.trace();
            // TODO: WsAbortEx
            doWsAbort(connectTarget, connectRouteId, connectId, traceId, STATUS_UNEXPECTED_CONDITION);
        }

        private int wsHeaderLength(DirectBuffer buffer)
        {
            int wsHeaderLength = 2;
            byte secondByte = buffer.getByte(1);
            wsHeaderLength += lengthSize(secondByte) - 1;

            if (isMasked(secondByte))
            {
                wsHeaderLength+= 4;
            }

            return wsHeaderLength;
        }

        private boolean isMasked(byte b)
        {
            return (b & 0x80) != 0;
        }

        private int lengthSize(byte b)
        {
            switch (b & 0x7f)
            {
                case 0x7e:
                    return 3;

                case 0x7f:
                    return 9;

                default:
                    return 1;
            }
        }

        // @return no bytes consumed to assemble websocket header
        private int assembleHeader(DirectBuffer buffer, int offset, int length)
        {
            int remaining = Math.min(length, MAXIMUM_HEADER_SIZE - headerLength);
            // may copy more than actual header length (up to max header length), but will adjust at the end
            header.putBytes(headerLength, buffer, offset, remaining);

            int consumed = remaining;
            if (headerLength + remaining >= 2)
            {
                int wsHeaderLength = wsHeaderLength(header);
                // eventual headLength must not be more than wsHeaderLength
                if (headerLength + remaining > wsHeaderLength)
                {
                    consumed = wsHeaderLength - headerLength;
                }
            }

            headerLength += consumed;
            return consumed;
        }

        private int decodeHeader(
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            int consumed;
            if (headerLength > 0 || length < MAXIMUM_HEADER_SIZE)
            {
                consumed = assembleHeader(buffer, offset, length);
                if (headerLength >= 2 && headerLength == wsHeaderLength(header))
                {
                    wsHeaderRO.wrap(header, 0, headerLength);
                    headerLength = 0;
                }
                else
                {
                    return consumed;            // partial header
                }
            }
            else
            {
                // No need to assemble header as complete header is available
                wsHeaderRO.wrap(buffer, offset, offset + MAXIMUM_HEADER_SIZE);
                consumed = wsHeaderRO.sizeof();
            }

            if (wsHeaderRO.mask() && wsHeaderRO.maskingKey() != 0L)
            {
                this.maskingKey = wsHeaderRO.maskingKey();
                this.payloadLength = wsHeaderRO.length();
                this.payloadProgress = 0;

                switch (wsHeaderRO.opcode())
                {
                case 0x00:
                    this.decodeState = this::decodeContinuation;
                    break;
                case 0x01:
                    this.decodeState = this::decodeText;
                    break;
                case 0x02:
                    this.decodeState = this::decodeBinary;
                    break;
                case 0x08:
                    this.decodeState = this::decodeClose;
                    break;
                case 0x0a:
                    this.decodeState = this::decodePong;
                    break;
                default:
                    this.decodeState = this::decodeUnexpected;
                    break;
                }
            }
            else
            {
                doReset(acceptThrottle, acceptRouteId, acceptId, 0L);
                doWsAbort(connectTarget, connectRouteId, connectId, connectTraceId, STATUS_PROTOCOL_ERROR);
            }

            return consumed;
        }

        private int decodeContinuation(
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {

            // TODO: limit acceptReply bytes by acceptReply window, or RESET on overflow?

            final int decodeBytes = Math.min(length, payloadLength - payloadProgress);

            payload.wrap(buffer, offset, offset + decodeBytes);
            doWsData(connectTarget, connectRouteId, connectId, 0x80, maskingKey, payload);

            payloadProgress += decodeBytes;
            maskingKey = rotateMaskingKey(maskingKey, decodeBytes);

            if (payloadProgress == payloadLength)
            {
                this.decodeState = this::decodeHeader;
            }

            return decodeBytes;
        }

        private int decodeText(
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            // TODO canWrap for UTF-8 split multi-byte characters

            // TODO: limit acceptReply bytes by acceptReply window, or RESET on overflow?

            final int decodeBytes = Math.min(length, payloadLength - payloadProgress);

            payload.wrap(buffer, offset, offset + decodeBytes);
            doWsData(connectTarget, connectRouteId, connectId, 0x81, maskingKey, payload);

            payloadProgress += decodeBytes;
            maskingKey = rotateMaskingKey(maskingKey, decodeBytes);

            if (payloadProgress == payloadLength)
            {
                this.decodeState = this::decodeHeader;
            }

            return decodeBytes;
        }

        private int decodeBinary(
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            // TODO: limit acceptReply bytes by acceptReply window, or RESET on overflow?

            final int decodeBytes = Math.min(length, payloadLength - payloadProgress);

            payload.wrap(buffer, offset, offset + decodeBytes);
            doWsData(connectTarget, connectRouteId, connectId, 0x82, maskingKey, payload);

            payloadProgress += decodeBytes;
            maskingKey = rotateMaskingKey(maskingKey, decodeBytes);

            if (payloadProgress == payloadLength)
            {
                this.decodeState = this::decodeHeader;
            }

            return decodeBytes;
        }

        private int rotateMaskingKey(int maskingKey, int decodeBytes)
        {
            decodeBytes = decodeBytes % 4;
            int left;
            int right;
            if (nativeOrder() == BIG_ENDIAN)
            {
                left = decodeBytes * 8;
                right = Integer.SIZE - left;
            }
            else
            {
                right = decodeBytes * 8;
                left = Integer.SIZE - right;
            }
            return (maskingKey << left) | (maskingKey >>> right);
        }

        private int decodeClose(
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            if (payloadLength > MAXIMUM_CONTROL_FRAME_PAYLOAD_SIZE)
            {
                doReset(acceptThrottle, acceptRouteId, acceptId, 0L);
                doWsAbort(connectTarget, connectRouteId, connectId, connectTraceId, STATUS_PROTOCOL_ERROR);
                return length;
            }
            else
            {
                final int decodeBytes = Math.min(length, payloadLength - payloadProgress);
                payloadProgress += decodeBytes;

                int remaining = Math.min(length, 2 - statusLength);
                if (remaining > 0)
                {
                    status.putBytes(statusLength, buffer, offset, remaining);
                    statusLength += remaining;
                }

                if (payloadProgress == payloadLength)
                {
                    short code = STATUS_NORMAL_CLOSURE;
                    if (statusLength == 2)
                    {
                        xor(status, 0, 2, maskingKey);
                        code = status.getShort(0, ByteOrder.BIG_ENDIAN);
                    }
                    statusLength = 0;
                    doWsEnd(connectTarget, connectRouteId, connectId, connectTraceId, code);
                    this.decodeState = this::decodeHeader;
                }

                return decodeBytes;
            }
        }

        private int decodePong(
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            if (payloadLength > MAXIMUM_CONTROL_FRAME_PAYLOAD_SIZE)
            {
                doReset(acceptThrottle, acceptRouteId, acceptId, 0L);
                doWsAbort(connectTarget, connectRouteId, connectId, connectTraceId, STATUS_PROTOCOL_ERROR);
                return length;
            }
            else
            {
                final int decodeBytes = Math.min(length, payloadLength - payloadProgress);

                payload.wrap(buffer, offset, offset + decodeBytes);

                payloadProgress += decodeBytes;
                maskingKey = rotateMaskingKey(maskingKey, decodeBytes);

                if (payloadProgress == payloadLength)
                {
                    this.decodeState = this::decodeHeader;
                }

                return decodeBytes;
            }
        }

        private int decodeUnexpected(
            final DirectBuffer directBuffer,
            final int offset,
            final int length)
        {
            doReset(acceptThrottle, acceptRouteId, acceptId, 0L);
            doWsAbort(connectTarget, connectRouteId, connectId, connectTraceId, STATUS_PROTOCOL_ERROR);
            return length;
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
            connectBudget += window.credit();
            connectPadding = window.padding();

            final int acceptCredit = connectBudget - acceptBudget;

            if (acceptCredit > 0)
            {
                acceptBudget += acceptCredit;
                acceptPadding = Math.max(acceptPadding, window.padding());
                final long acceptTraceId = window.trace();
                doWindow(acceptThrottle, acceptRouteId, acceptId, acceptTraceId, acceptCredit, acceptPadding);
            }
        }

        private void handleReset(
            ResetFW reset)
        {
            final long traceId = reset.trace();
            doReset(acceptThrottle, acceptRouteId, acceptId, traceId);
        }

        private void doWsData(
            MessageConsumer stream,
            long routeId,
            long streamId,
            int flags,
            int maskKey,
            OctetsFW payload)
        {
            connectBudget -= payload.sizeof() + connectPadding;

            final int capacity = payload.sizeof();
            final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(streamId)
                    .trace(connectTraceId)
                    .groupId(0)
                    .padding(connectPadding)
                    .payload(p -> p.set(payload).set((b, o, l) -> xor(b, o, o + capacity, maskKey)))
                    .extension(e -> e.set(visitWsDataEx(flags)))
                    .build();

            stream.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
        }
    }

    private final class ServerConnectReplyStream
    {
        private final MessageConsumer connectReplyThrottle;
        private final long connectRouteId;
        private final long connectReplyId;

        private MessageConsumer acceptReply;
        private long acceptReplyId;

        private MessageConsumer streamState;

        private int acceptReplyBudget;
        private int acceptReplyPadding;
        private int connectReplyBudget;
        private long acceptRouteId;

        private ServerConnectReplyStream(
            MessageConsumer connectReplyThrottle,
            long connectRouteId,
            long connectReplyId)
        {
            this.connectReplyThrottle = connectReplyThrottle;
            this.connectRouteId = connectRouteId;
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
                doReset(connectReplyThrottle, connectRouteId, connectReplyId, 0L);
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
                doReset(connectReplyThrottle, connectRouteId, connectReplyId, 0L);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final long connectRef = begin.sourceRef();
            final long correlationId = begin.correlationId();
            final long traceId = begin.trace();

            final ServerHandshake handshake = correlations.remove(correlationId);

            if (connectRef == 0L && handshake != null)
            {
                final long acceptRouteId = handshake.acceptRouteId();
                final String acceptReplyName = handshake.acceptName();

                final MessageConsumer newAcceptReply = router.supplyTarget(acceptReplyName);
                final long newAcceptReplyId = supplyReplyId.applyAsLong(handshake.acceptId());
                final long newCorrelationId = handshake.correlationId();
                String handshakeHash = handshake.handshakeHash();
                String protocol = handshake.protocol();

                doHttpBegin(newAcceptReply, acceptRouteId, newAcceptReplyId, 0L, newCorrelationId, traceId,
                        setHttpHeaders(handshakeHash, protocol));
                router.setThrottle(acceptReplyName, newAcceptReplyId, this::handleThrottle);

                this.acceptReply = newAcceptReply;
                this.acceptRouteId = acceptRouteId;
                this.acceptReplyId = newAcceptReplyId;

                this.streamState = this::afterBeginOrData;
            }
            else
            {
                doReset(connectReplyThrottle, connectRouteId, connectReplyId, 0L);
            }
        }

        private void handleData(
            DataFW data)
        {
            connectReplyBudget -= data.length() + data.padding();

            if (connectReplyBudget < 0)
            {
                doReset(connectReplyThrottle, connectRouteId, connectReplyId, 0L);
            }
            else
            {
                final OctetsFW payload = data.payload();
                final OctetsFW extension = data.extension();

                int flags = 0x82;
                if (extension.sizeof() > 0)
                {
                    final WsDataExFW wsDataEx = extension.get(wsDataExRO::wrap);
                    flags = wsDataEx.flags();
                }

                final long traceId = data.trace();
                doHttpData(acceptReply, acceptRouteId, acceptReplyId, traceId, acceptReplyPadding, payload, flags);
            }
        }

        private void handleEnd(
            EndFW end)
        {
            final long traceId = end.trace();
            payload.wrap(CLOSE_PAYLOAD, 0, 0);
            doHttpData(acceptReply, acceptRouteId, acceptReplyId, traceId, acceptReplyPadding, payload, 0x88);
            doHttpEnd(acceptReply, acceptRouteId, acceptReplyId, traceId);
        }

        private void handleAbort(
            AbortFW abort)
        {
            doHttpAbort(acceptReply, acceptRouteId, acceptReplyId);
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
            acceptReplyBudget += window.credit();
            acceptReplyPadding = window.padding();

            final int connectReplyCredit = acceptReplyBudget - connectReplyBudget;
            if (connectReplyCredit > 0)
            {
                connectReplyBudget += connectReplyCredit;
                int connectReplyPadding = acceptReplyPadding + MAXIMUM_HEADER_SIZE;
                final long connectTraceId = window.trace();
                doWindow(connectReplyThrottle, connectRouteId, connectReplyId,
                        connectTraceId, connectReplyCredit, connectReplyPadding);
            }
        }

        private void handleReset(
            ResetFW reset)
        {
            final long connectReplyTraceId = reset.trace();
            doReset(connectReplyThrottle, connectRouteId, connectReplyId, connectReplyTraceId);
        }

        private int doHttpData(
            MessageConsumer receiver,
            long routeId,
            long streamId,
            long traceId,
            int padding,
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

            acceptReplyBudget -= wsHeaderSize + payloadFragmentSize + acceptReplyPadding;
            DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(streamId)
                    .groupId(0)
                    .padding(padding)
                    .payload(p -> p.set((b, o, m) -> wsHeaderSize)
                                   .put(payload.buffer(), payload.offset(), payloadFragmentSize))
                    .build();

            receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());

            final int payloadRemaining = payloadSize - payloadFragmentSize;
            if (payloadRemaining > 0)
            {
                acceptReplyBudget -= payloadRemaining + acceptReplyPadding;
                DataFW data2 = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                        .routeId(routeId)
                        .streamId(streamId)
                        .trace(traceId)
                        .groupId(0)
                        .padding(padding)
                        .payload(payload.buffer(), payload.offset() + payloadFragmentSize, payloadRemaining)
                        .build();

                receiver.accept(data2.typeId(), data2.buffer(), data2.offset(), data2.sizeof());
            }

            return wsHeaderSize;
        }
    }

    private void doHttpBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long targetRef,
        long correlationId,
        long traceId,
        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .source("ws")
                .sourceRef(targetRef)
                .correlationId(correlationId)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
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

    private void doHttpEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doHttpAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doWsBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long streamRef,
        long correlationId,
        long traceId,
        String protocol)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .source("ws")
                .sourceRef(streamRef)
                .correlationId(correlationId)
                .extension(e -> e.set(visitWsBeginEx(protocol)))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
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
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        short code)
    {
        // TODO: WsAbortEx
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doWsEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        short code)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .extension(e -> e.set(visitWsEndEx(code)))
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
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
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long traceId,
        final int credit,
        final int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .credit(credit)
                .padding(padding)
                .groupId(0)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long traceId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .trace(traceId)
               .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
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

    @FunctionalInterface
    private interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int length);
    }
}
