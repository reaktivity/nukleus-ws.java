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
import static org.reaktivity.nukleus.ws.internal.util.WsMaskUtil.xor;

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
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.ws.internal.WsConfiguration;
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

public final class WsServerStreamFactory implements StreamFactory
{
    private static final int MAXIMUM_CONTROL_FRAME_PAYLOAD_SIZE = 125;
    private static final int MAXIMUM_DATA_LENGTH = (1 << Short.SIZE) - 1;

    private static final byte[] HANDSHAKE_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11".getBytes(UTF_8);
    private static final String WEBSOCKET_UPGRADE = "websocket";
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

    private final OctetsFW payloadRO = new OctetsFW();

    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();

    private final WsDataExFW wsDataExRO = new WsDataExFW();

    private final WsHeaderFW wsHeaderRO = new WsHeaderFW();
    private final WsHeaderFW.Builder wsHeaderRW = new WsHeaderFW.Builder();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;

    private final Long2ObjectHashMap<WsServerConnectStream> correlations;
    private final MessageFunction<RouteFW> wrapRoute;

    public WsServerStreamFactory(
        WsConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.correlations = new Long2ObjectHashMap<>();
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
        final long streamId = begin.streamId();

        MessageConsumer newStream = null;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newAcceptStream(begin, throttle);
        }
        else
        {
            newStream = newConnectReplyStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
        final BeginFW begin,
        final MessageConsumer acceptReply)
    {

        MessageConsumer newStream = null;

        final long acceptRouteId = begin.routeId();
        final long acceptInitialId = begin.streamId();
        final long acceptCorrelationId = begin.correlationId();
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
            final long newAcceptReplyId = supplyReplyId.applyAsLong(acceptInitialId);
            doHttpBegin(acceptReply, acceptRouteId, newAcceptReplyId, acceptCorrelationId, supplyTraceId.getAsLong(),
                    hs -> hs.item(h -> h.name(":status").value("400"))
                            .item(h -> h.name("connection").value("close")));
            doHttpEnd(acceptReply, acceptRouteId, newAcceptReplyId, supplyTraceId.getAsLong());
            newStream = (t, b, o, l) -> {};
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

                return protocols == null || protocols.contains(protocol);
            };

            final RouteFW route = router.resolve(acceptRouteId, begin.authorization(), filter, wrapRoute);

            if (route != null)
            {
                final WsRouteExFW wsRouteEx = route.extension().get(wsRouteExRO::wrap);

                sha1.reset();
                sha1.update(key.getBytes(US_ASCII));
                final byte[] digest = sha1.digest(HANDSHAKE_GUID);
                final Encoder encoder = Base64.getEncoder();
                final String handshakeHash = new String(encoder.encode(digest), US_ASCII);
                final String protocol = resolveProtocol(protocols, wsRouteEx.protocol().asString());

                final long connectRouteId = route.correlationId();
                final long connectInitialId = supplyInitialId.applyAsLong(connectRouteId);
                final MessageConsumer connectInitial = router.supplyReceiver(connectInitialId);

                final WsServerAcceptStream accept =
                        new WsServerAcceptStream(acceptReply, acceptRouteId, acceptInitialId, acceptCorrelationId,
                                handshakeHash, protocol);

                final WsServerConnectStream connect = new WsServerConnectStream(connectInitial, connectRouteId,
                        connectInitialId);

                accept.correlate(connect);
                connect.correlate(accept);

                final long connectReplyId = supplyReplyId.applyAsLong(connectInitialId);
                correlations.put(connectReplyId, connect);

                newStream = accept::handleStream;
            }
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(
        final BeginFW begin,
        final MessageConsumer connectInitial)
    {
        final long connectReplyId = begin.streamId();
        final WsServerConnectStream connectStream = correlations.remove(connectReplyId);

        MessageConsumer newStream = null;
        if (connectStream != null)
        {
            newStream = connectStream::handleStream;
        }

        return newStream;
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    private final class WsServerAcceptStream
    {
        private final MessageConsumer acceptReply;
        private final long acceptRouteId;
        private final long acceptInitialId;
        private final long acceptCorrelationId;
        private final long acceptReplyId;
        private final String handshakeHash;
        private final String protocol;

        private WsServerConnectStream connect;

        private DecoderState decodeState;

        private MutableDirectBuffer header;
        private int headerLength;

        private int payloadProgress;
        private int payloadLength;
        private int maskingKey;

        private int acceptBudget;
        private int acceptPadding;
        private int acceptReplyBudget;
        private int acceptReplyPadding;

        private long decodeTraceId;

        private int statusLength;
        private MutableDirectBuffer status;

        private WsServerAcceptStream(
            MessageConsumer acceptReply,
            long acceptRouteId,
            long acceptInitialId,
            long acceptCorrelationId,
            String handshakeHash,
            String protocol)
        {
            this.acceptReply = acceptReply;
            this.acceptRouteId = acceptRouteId;
            this.acceptInitialId = acceptInitialId;
            this.acceptCorrelationId = acceptCorrelationId;
            this.acceptReplyId = supplyReplyId.applyAsLong(acceptInitialId);
            this.handshakeHash = handshakeHash;
            this.protocol = protocol;

            this.header = new UnsafeBuffer(new byte[MAXIMUM_HEADER_SIZE]);
            this.status = new UnsafeBuffer(new byte[2]);

            this.decodeState = this::decodeHeader;
        }

        private void correlate(
            WsServerConnectStream connect)
        {
            this.connect = connect;
        }

        private void doBegin(
            long traceId)
        {
            doHttpBegin(acceptReply, acceptRouteId, acceptReplyId, acceptCorrelationId, traceId,
                    setHttpHeaders(handshakeHash, protocol));
            router.setThrottle(acceptReplyId, this::handleThrottle);
        }

        private void doData(
            long traceId,
            OctetsFW payload,
            int flags)
        {
            final int payloadSize = payload.sizeof();

            WsHeaderFW wsHeader = wsHeaderRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                            .length(payloadSize)
                                            .flagsAndOpcode(flags)
                                            .build();

            final int wsHeaderSize = wsHeader.sizeof();
            final int payloadFragmentSize = Math.min(MAXIMUM_DATA_LENGTH - wsHeaderSize,  payloadSize);

            acceptReplyBudget -= wsHeaderSize + payloadFragmentSize + acceptReplyPadding;
            DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(acceptRouteId)
                    .streamId(acceptReplyId)
                    .groupId(0)
                    .padding(acceptReplyPadding)
                    .payload(p -> p.set((b, o, m) -> wsHeaderSize)
                                   .put(payload.buffer(), payload.offset(), payloadFragmentSize))
                    .build();

            acceptReply.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());

            final int payloadRemaining = payloadSize - payloadFragmentSize;
            if (payloadRemaining > 0)
            {
                acceptReplyBudget -= payloadRemaining + acceptReplyPadding;
                DataFW data2 = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                        .routeId(acceptRouteId)
                        .streamId(acceptReplyId)
                        .trace(traceId)
                        .groupId(0)
                        .padding(acceptReplyPadding)
                        .payload(payload.buffer(), payload.offset() + payloadFragmentSize, payloadRemaining)
                        .build();

                acceptReply.accept(data2.typeId(), data2.buffer(), data2.offset(), data2.sizeof());
            }
        }

        private void doEnd(
            long traceId)
        {
            final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(acceptRouteId)
                    .streamId(acceptReplyId)
                    .trace(traceId)
                    .build();

            acceptReply.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
        }

        private void doAbort()
        {
            final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(acceptRouteId)
                    .streamId(acceptReplyId)
                    .build();

            acceptReply.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
        }

        private void doReset(
            long traceId)
        {
            final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(acceptRouteId)
                    .streamId(acceptInitialId)
                    .trace(traceId)
                    .build();

            acceptReply.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
        }

        private void doWindow(
            long traceId,
            int maxBudget,
            int minPadding)
        {
            int acceptCredit = maxBudget - acceptBudget;
            if (acceptCredit > 0)
            {
                acceptBudget += acceptCredit;
                acceptPadding = Math.max(acceptPadding, minPadding);

                final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                        .routeId(acceptRouteId)
                        .streamId(acceptInitialId)
                        .trace(traceId)
                        .credit(acceptCredit)
                        .padding(acceptPadding)
                        .groupId(0)
                        .build();

                acceptReply.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
            }
        }

        private void handleStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onAbort(abort);
                break;
            default:
                break;
            }
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
                onWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onReset(reset);
                break;
            default:
                // ignore
                break;
            }
        }

        private void onBegin(
            BeginFW begin)
        {
            final long traceId = begin.trace();
            connect.doBegin(traceId, protocol);
        }

        private void onData(
            DataFW data)
        {
            acceptBudget -= data.length() + data.padding();

            if (acceptBudget < 0)
            {
                doReset(supplyTraceId.getAsLong());
            }
            else
            {
                decodeTraceId = data.trace();

                final OctetsFW payload = data.payload();
                final DirectBuffer buffer = payload.buffer();

                int offset = payload.offset();
                int length = payload.sizeof();
                while (length > 0)
                {
                    int consumed = decodeState.decode(buffer, offset, length);
                    offset += consumed;
                    length -= consumed;
                }

                // Since we have two decoding states for a frame, the following is
                // needed to handle empty close, empty ping etc. Otherwise, it will be
                // delayed until next handleData() (which may not come for e.g empty close frame)
                if (payloadLength == 0)
                {
                    decodeState.decode(buffer, 0, 0);
                }
            }
        }

        private void onEnd(
            EndFW end)
        {
            final long traceId = end.trace();
            connect.doEnd(traceId, STATUS_NORMAL_CLOSURE);
        }

        private void onAbort(
            AbortFW abort)
        {
            final long traceId = abort.trace();
            connect.doAbort(traceId, STATUS_UNEXPECTED_CONDITION);
        }

        private void onWindow(
            WindowFW window)
        {
            acceptReplyBudget += window.credit();
            acceptReplyPadding = window.padding();

            final long traceId = window.trace();
            connect.doWindow(traceId, acceptReplyBudget, acceptReplyPadding);
        }

        private void onReset(
            ResetFW reset)
        {
            final long traceId = reset.trace();
            connect.doReset(traceId);
        }

        // @return no bytes consumed to assemble websocket header
        private int assembleHeader(
            DirectBuffer buffer,
            int offset,
            int length)
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
                doReset(supplyTraceId.getAsLong());
                connect.doAbort(decodeTraceId, STATUS_PROTOCOL_ERROR);
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

            final OctetsFW payload = payloadRO.wrap(buffer, offset, offset + decodeBytes);
            connect.doData(decodeTraceId, 0x80, maskingKey, payload);

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

            final OctetsFW payload = payloadRO.wrap(buffer, offset, offset + decodeBytes);
            connect.doData(decodeTraceId, 0x81, maskingKey, payload);

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

            final OctetsFW payload = payloadRO.wrap(buffer, offset, offset + decodeBytes);
            connect.doData(decodeTraceId, 0x82, maskingKey, payload);

            payloadProgress += decodeBytes;
            maskingKey = rotateMaskingKey(maskingKey, decodeBytes);

            if (payloadProgress == payloadLength)
            {
                this.decodeState = this::decodeHeader;
            }

            return decodeBytes;
        }

        private int rotateMaskingKey(
            int maskingKey,
            int decodeBytes)
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
                doReset(supplyTraceId.getAsLong());
                connect.doAbort(decodeTraceId, STATUS_PROTOCOL_ERROR);
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
                    connect.doEnd(decodeTraceId, code);
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
                doReset(supplyTraceId.getAsLong());
                connect.doAbort(decodeTraceId, STATUS_PROTOCOL_ERROR);
                return length;
            }
            else
            {
                final int decodeBytes = Math.min(length, payloadLength - payloadProgress);

                payloadRO.wrap(buffer, offset, offset + decodeBytes);

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
            doReset(supplyTraceId.getAsLong());
            connect.doAbort(decodeTraceId, STATUS_PROTOCOL_ERROR);
            return length;
        }
    }

    private final class WsServerConnectStream
    {
        private final MessageConsumer connectInitial;
        private final long connectRouteId;
        private final long connectInitialId;

        private long connectReplyId;
        private int connectBudget;
        private int connectPadding;
        private int connectReplyBudget;

        private WsServerAcceptStream accept;

        private WsServerConnectStream(
            MessageConsumer connectInitial,
            long connectRouteId,
            long connectInitialId)
        {
            this.connectInitial = connectInitial;
            this.connectRouteId = connectRouteId;
            this.connectInitialId = connectInitialId;
        }

        private void correlate(
            WsServerAcceptStream accept)
        {
            this.accept = accept;
        }

        private void doBegin(
            long traceId,
            String protocol)
        {
            final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(connectRouteId)
                    .streamId(connectInitialId)
                    .trace(traceId)
                    .correlationId(connectReplyId)
                    .extension(e -> e.set(visitWsBeginEx(protocol)))
                    .build();

            connectInitial.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());

            router.setThrottle(connectInitialId, this::handleThrottle);
        }

        private void doData(
            long traceId,
            int flags,
            int maskingKey,
            OctetsFW payload)
        {
            connectBudget -= payload.sizeof() + connectPadding;

            final int capacity = payload.sizeof();
            final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(connectRouteId)
                    .streamId(connectInitialId)
                    .trace(traceId)
                    .groupId(0)
                    .padding(connectPadding)
                    .payload(p -> p.set(payload).set((b, o, l) -> xor(b, o, o + capacity, maskingKey)))
                    .extension(e -> e.set(visitWsDataEx(flags)))
                    .build();

            connectInitial.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
        }

        private void doEnd(
            long traceId,
            short code)
        {
            final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(connectRouteId)
                    .streamId(connectInitialId)
                    .trace(traceId)
                    .extension(e -> e.set(visitWsEndEx(code)))
                    .build();

            connectInitial.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
        }

        private void doAbort(
            long traceId,
            short code)
        {
            // TODO: WsAbortEx
            final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(connectRouteId)
                    .streamId(connectInitialId)
                    .trace(traceId)
                    .build();

            connectInitial.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
        }

        private void doReset(
            long traceId)
        {
            final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(connectRouteId)
                    .streamId(connectReplyId)
                    .trace(traceId)
                    .build();

            connectInitial.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
        }

        private void doWindow(
            long traceId,
            int maxBudget,
            int minPadding)
        {
            final int connectReplyCredit = maxBudget - connectReplyBudget;
            if (connectReplyCredit > 0)
            {
                connectReplyBudget += connectReplyCredit;
                int connectReplyPadding = minPadding + MAXIMUM_HEADER_SIZE;

                final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                        .routeId(connectRouteId)
                        .streamId(connectReplyId)
                        .trace(traceId)
                        .credit(connectReplyCredit)
                        .padding(connectReplyPadding)
                        .groupId(0)
                        .build();

                connectInitial.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
            }
        }

        private void handleStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onAbort(abort);
                break;
            default:
                break;
            }
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
                onWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onReset(reset);
                break;
            default:
                // ignore
                break;
            }
        }

        private void onBegin(
            BeginFW begin)
        {
            final long traceId = begin.trace();
            this.connectReplyId = begin.streamId();

            accept.doBegin(traceId);
        }

        private void onData(
            DataFW data)
        {
            connectReplyBudget -= data.length() + data.padding();

            if (connectReplyBudget < 0)
            {
                doReset(supplyTraceId.getAsLong());
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
                accept.doData(traceId, payload, flags);
            }
        }

        private void onEnd(
            EndFW end)
        {
            final long traceId = end.trace();
            final OctetsFW payload = payloadRO.wrap(CLOSE_PAYLOAD, 0, 0);

            accept.doData(traceId, payload, 0x88);
            accept.doEnd(traceId);
        }

        private void onAbort(
            AbortFW abort)
        {
            accept.doAbort();
        }

        private void onWindow(
            WindowFW window)
        {
            final int connectCredit = window.credit();

            connectBudget += connectCredit;
            connectPadding = window.padding();

            final long traceId = window.trace();
            accept.doWindow(traceId, connectBudget, connectPadding);
        }

        private void onReset(
            ResetFW reset)
        {
            final long traceId = reset.trace();
            accept.doReset(traceId);
        }
    }

    private void doHttpBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long correlationId,
        long traceId,
        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
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

    private Flyweight.Builder.Visitor visitWsDataEx(
        int flags)
    {
        return (buffer, offset, limit) ->
            wsDataExRW.wrap(buffer, offset, limit)
                      .flags(flags)
                      .build()
                      .sizeof();
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

            if (protocol != null)
            {
                headers.item(h -> h.name("sec-websocket-protocol").value(protocol));
            }
        };
    }

    private static String resolveProtocol(
        final String protocols,
        final String protocol)
    {
        return (protocols != null) && protocols.contains(protocol) ? protocol : null;
    }

    private static int wsHeaderLength(
        DirectBuffer buffer)
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

    private static boolean isMasked(
        byte b)
    {
        return (b & 0x80) != 0;
    }

    private static int lengthSize(
        byte b)
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
