/**
 * Copyright 2016-2019 The Reaktivity Project
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
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

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
import org.reaktivity.nukleus.ws.internal.WsNukleus;
import org.reaktivity.nukleus.ws.internal.types.ArrayFW;
import org.reaktivity.nukleus.ws.internal.types.Flyweight;
import org.reaktivity.nukleus.ws.internal.types.HttpHeaderFW;
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

public final class WsClientFactory implements StreamFactory
{
    private static final int MAXIMUM_CONTROL_FRAME_PAYLOAD_SIZE = 125;

    private static final byte[] HANDSHAKE_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11".getBytes(UTF_8);
    private static final String WEBSOCKET_METHOD = "GET";
    private static final String WEBSOCKET_STATUS = "101";
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

    private final WsBeginExFW wsBeginExRO = new WsBeginExFW();
    private final WsDataExFW wsDataExRO = new WsDataExFW();

    private final WsHeaderFW wsHeaderRO = new WsHeaderFW();
    private final WsHeaderFW.Builder wsHeaderRW = new WsHeaderFW.Builder();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;

    private final Long2ObjectHashMap<WsClientConnect> correlations;
    private final MessageFunction<RouteFW> wrapRoute;
    private final int wsTypeId;
    private final int httpTypeId;

    public WsClientFactory(
        WsConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.correlations = new Long2ObjectHashMap<>();
        this.wrapRoute = this::wrapRoute;
        this.wsTypeId = supplyTypeId.applyAsInt(WsNukleus.NAME);
        this.httpTypeId = supplyTypeId.applyAsInt("http");
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
            newStream = newInitialStream(begin, throttle);
        }
        else
        {
            newStream = newReplyStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newInitialStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long routeId = begin.routeId();
        final long initialId = begin.streamId();
        final OctetsFW extension = begin.extension();
        final boolean hasExtension = extension.sizeof() != 0;

        String protocol = null;
        String scheme = null;
        String authority = null;
        String path = null;
        if (hasExtension)
        {
            final WsBeginExFW wsBeginEx = wsBeginExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
            protocol = wsBeginEx.protocol().asString();
            scheme = wsBeginEx.scheme().asString();
            authority = wsBeginEx.authority().asString();
            path = wsBeginEx.path().asString();
        }

        final String protocol0 = protocol;
        final String scheme0 = scheme;
        final String authority0 = authority;
        final String path0 = path;
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, o + l);
            final OctetsFW routeEx = route.extension();

            String routeProtocol = null;
            String routeScheme = null;
            String routeAuthority = null;
            String routePath = null;

            if (routeEx.sizeof() != 0)
            {
                final WsRouteExFW wsRouteEx = routeEx.get(wsRouteExRO::wrap);
                routeProtocol = wsRouteEx.protocol().asString();
                routeScheme = wsRouteEx.scheme().asString();
                routeAuthority = wsRouteEx.authority().asString();
                routePath = wsRouteEx.path().asString();
            }

            return (protocol0 == null || routeProtocol == null || protocol0.equals(routeProtocol)) &&
                    (scheme0 == null || routeScheme == null || scheme0.equals(routeScheme)) &&
                    (authority0 == null || routeAuthority == null || authority0.equals(routeAuthority)) &&
                    (path0 == null || routePath == null || path0.startsWith(routePath));

        };

        final RouteFW route = router.resolve(routeId, begin.authorization(), filter, wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long wsRouteId = route.correlationId();

            WsRouteExFW wsRouteEx = null;
            final OctetsFW routeEx = route.extension();
            if (routeEx.sizeof() != 0)
            {
                wsRouteEx = routeEx.get(wsRouteExRO::wrap);
            }

            final String wsProtocol = wsRouteEx != null ? wsRouteEx.protocol().asString() : null;
            final String wsScheme = hasExtension ? scheme : wsRouteEx != null ? wsRouteEx.scheme().asString() : null;
            final String wsAuthority = hasExtension ? authority : wsRouteEx != null ? wsRouteEx.authority().asString() : null;
            final String wsPath = hasExtension ? path : wsRouteEx != null ? wsRouteEx.path().asString() : null;

            final WsClientAccept accept = new WsClientAccept(routeId, initialId, sender, protocol);
            final WsClientConnect connect = new WsClientConnect(wsRouteId, wsScheme, wsAuthority, wsPath, wsProtocol);

            accept.correlate(connect);
            connect.correlate(accept);

            correlations.put(connect.replyId, connect);

            newStream = accept::handleStream;
        }

        return newStream;
    }

    private MessageConsumer newReplyStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long replyId = begin.streamId();
        final WsClientConnect connect = correlations.remove(replyId);

        MessageConsumer newStream = null;

        if (connect != null)
        {
            final OctetsFW extension = begin.extension();
            if (extension.sizeof() != 0)
            {
                // TODO: need lightweight approach (start)
                final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
                final Map<String, String> headers = new LinkedHashMap<>();
                httpBeginEx.headers().forEach(header ->
                {
                    final String name = header.name().asString();
                    final String value = header.value().asString();
                    headers.merge(name, value, (v1, v2) -> String.format("%s, %s", v1, v2));
                });

                final String status = headers.get(":status");
                final String upgrade = headers.get("upgrade");
                final String hash = headers.get("sec-websocket-accept");
                final String protocol = headers.get("sec-websocket-protocol");
                // TODO: need lightweight approach (end)

                if (WEBSOCKET_STATUS.equals(status) &&
                    WEBSOCKET_UPGRADE.equals(upgrade) &&
                    Objects.equals(connect.protocol, protocol))
                {
                    sha1.reset();
                    sha1.update(connect.key.getBytes(US_ASCII));
                    final byte[] digest = sha1.digest(HANDSHAKE_GUID);
                    final Encoder encoder = Base64.getEncoder();
                    final String handshakeHash = new String(encoder.encode(digest), US_ASCII);

                    if (handshakeHash.equals(hash))
                    {
                        newStream = connect::handleStream;
                    }
                }
            }

            if (newStream == null)
            {
                final long traceId = begin.trace();
                connect.accept.doReset(traceId);
            }
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

    private final class WsClientAccept
    {
        private final MessageConsumer receiver;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final String protocol;

        private WsClientConnect connect;

        private int initialBudget;
        private int replyBudget;
        private int replyPadding;

        private WsClientAccept(
            long routeId,
            long initialId,
            MessageConsumer receiver,
            String protocol)
        {
            this.routeId = routeId;
            this.initialId = initialId;
            this.receiver = receiver;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.protocol = protocol;
        }

        private void correlate(
            WsClientConnect connect)
        {
            this.connect = connect;
        }

        private void doBegin(
            long traceId)
        {
            final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(replyId)
                    .trace(traceId)
                    .extension(e -> e.set(visitWsBeginEx(protocol)))
                    .build();

            router.setThrottle(replyId, this::handleThrottle);
            receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
        }

        private void doData(
            long traceId,
            int flags,
            int maskingKey,
            OctetsFW payload)
        {
            replyBudget -= payload.sizeof() + replyPadding;

            final int capacity = payload.sizeof();
            final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(replyId)
                    .trace(traceId)
                    .groupId(0)
                    .reserved(payload.sizeof() + replyPadding)
                    .payload(p -> p.set(payload).set((b, o, l) -> xor(b, o, o + capacity, maskingKey)))
                    .extension(e -> e.set(visitWsDataEx(flags)))
                    .build();

            receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
        }

        private void doEnd(
            long traceId,
            short code)
        {
            final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(replyId)
                    .trace(traceId)
                    .extension(e -> e.set(visitWsEndEx(code)))
                    .build();

            receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
        }

        private void doAbort(
            long traceId,
            short code)
        {
            // TODO: WsAbortEx
            final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(replyId)
                    .trace(traceId)
                    .build();

            receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
        }

        private void doReset(
            long traceId)
        {
            final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(initialId)
                    .trace(traceId)
                    .build();

            receiver.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
        }

        private void doWindow(
            long traceId,
            int maxBudget,
            int minPadding)
        {
            int initialCredit = maxBudget - initialBudget;
            if (initialCredit > 0)
            {
                initialBudget += initialCredit;
                int initialPadding = minPadding + MAXIMUM_HEADER_SIZE;

                final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                        .routeId(routeId)
                        .streamId(initialId)
                        .trace(traceId)
                        .credit(initialCredit)
                        .padding(initialPadding)
                        .groupId(0)
                        .build();

                receiver.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
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

            connect.doBegin(traceId);
        }

        private void onData(
            DataFW data)
        {
            initialBudget -= data.reserved();

            if (initialBudget < 0)
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
                connect.doData(traceId, payload, flags);
            }
        }

        private void onEnd(
            EndFW end)
        {
            final long traceId = end.trace();
            final OctetsFW payload = payloadRO.wrap(CLOSE_PAYLOAD, 0, 0);

            connect.doData(traceId, payload, 0x88);
            connect.doEnd(traceId);
        }

        private void onAbort(
            AbortFW abort)
        {
            final long traceId = abort.trace();
            connect.doAbort(traceId);
        }

        private void onWindow(
            WindowFW window)
        {
            replyBudget += window.credit();
            replyPadding = window.padding();

            final long traceId = window.trace();
            connect.doWindow(traceId, replyBudget, replyPadding);
        }

        private void onReset(
            ResetFW reset)
        {
            final long traceId = reset.trace();
            connect.doReset(traceId);
        }
    }

    private final class WsClientConnect
    {
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final MessageConsumer receiver;
        private final String scheme;
        private final String authority;
        private final String path;
        private final String key;
        private final String protocol;

        private int initialBudget;
        private int initialPadding;
        private int replyBudget;

        private WsClientAccept accept;

        private long decodeTraceId;
        private DecoderState decodeState;

        private MutableDirectBuffer header;
        private int headerLength;

        private int payloadProgress;
        private int payloadLength;
        private int maskingKey;

        private int statusLength;
        private MutableDirectBuffer status;

        private WsClientConnect(
            long routeId,
            String scheme,
            String authority,
            String path,
            String protocol)
        {
            this.routeId = routeId;
            this.scheme = scheme;
            this.authority = authority;
            this.path = path;
            this.key = newSecWebsocketKey();
            this.protocol = protocol;
            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId =  supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);
            this.header = new UnsafeBuffer(new byte[MAXIMUM_HEADER_SIZE]);
            this.decodeState = this::decodeHeader;
        }

        private void correlate(
            WsClientAccept accept)
        {
            this.accept = accept;
        }

        private void doBegin(
            long traceId)
        {
            router.setThrottle(initialId, this::handleThrottle);
            doHttpBegin(receiver, routeId, initialId, traceId, setHttpHeaders(scheme, authority, path, key, protocol));
        }

        private void doData(
            long traceId,
            OctetsFW payload,
            int flags)
        {
            final int payloadSize = payload.sizeof();
            final int maskingKey = ThreadLocalRandom.current().nextInt();

            WsHeaderFW wsHeader = wsHeaderRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                            .length(payloadSize)
                                            .flagsAndOpcode(flags)
                                            .maskingKey(maskingKey)
                                            .build();

            final int wsHeaderSize = wsHeader.sizeof();

            initialBudget -= wsHeaderSize + payloadSize + initialPadding;
            DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(initialId)
                    .trace(traceId)
                    .groupId(0)
                    .reserved(wsHeaderSize + payloadSize + initialPadding)
                    .payload(p -> p.set((b, o, m) -> wsHeaderSize)
                                   .put(payload.buffer(), payload.offset(), payloadSize)
                                   .set((b, o, l) -> wsHeaderSize +
                                           xor(b, o + wsHeaderSize, o + wsHeaderSize + payloadSize, maskingKey)))
                    .build();

            receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
        }

        private void doEnd(
            long traceId)
        {
            final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(initialId)
                    .trace(traceId)
                    .build();

            receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
        }

        private void doAbort(
            long traceId)
        {
            final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(initialId)
                    .trace(traceId)
                    .build();

            receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
        }

        private void doReset(
            long traceId)
        {
            final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(replyId)
                    .trace(traceId)
                    .build();

            receiver.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
        }

        private void doWindow(
            long traceId,
            int maxBudget,
            int minPadding)
        {
            final int replyCredit = maxBudget - replyBudget;
            if (replyCredit > 0)
            {
                replyBudget += replyCredit;
                int replyPadding = Math.max(minPadding - MAXIMUM_HEADER_SIZE, 0);

                final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                        .routeId(routeId)
                        .streamId(replyId)
                        .trace(traceId)
                        .credit(replyCredit)
                        .padding(replyPadding)
                        .groupId(0)
                        .build();

                receiver.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
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
            accept.doBegin(traceId);
        }


        private void onData(
            DataFW data)
        {
            replyBudget -= data.reserved();

            if (replyBudget < 0)
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
            if (accept != null)
            {
                final long traceId = end.trace();
                accept.doEnd(traceId, STATUS_PROTOCOL_ERROR);
            }
        }

        private void onAbort(
            AbortFW abort)
        {
            if (accept != null)
            {
                final long traceId = abort.trace();
                accept.doAbort(traceId, STATUS_UNEXPECTED_CONDITION);
            }
        }

        private void onWindow(
            WindowFW window)
        {
            initialBudget += window.credit();
            initialPadding = window.padding();

            final long traceId = window.trace();
            accept.doWindow(traceId, initialBudget, initialPadding);
        }

        private void onReset(
            ResetFW reset)
        {
            final long traceId = reset.trace();
            accept.doReset(traceId);

            correlations.remove(replyId);
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

            WsHeaderFW wsHeader;
            if (headerLength > 0 || length < MAXIMUM_HEADER_SIZE)
            {
                consumed = assembleHeader(buffer, offset, length);
                if (headerLength >= 2 && headerLength == wsHeaderLength(header))
                {
                    wsHeader = wsHeaderRO.wrap(header, 0, headerLength);
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
                wsHeader = wsHeaderRO.wrap(buffer, offset, offset + MAXIMUM_HEADER_SIZE);
                consumed = wsHeader.sizeof();
            }

            if (wsHeader.mask() && wsHeader.maskingKey() != 0L)
            {
                doReset(supplyTraceId.getAsLong());
                accept.doAbort(decodeTraceId, STATUS_PROTOCOL_ERROR);
            }
            else
            {
                this.maskingKey = 0;
                this.payloadLength = wsHeader.length();
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
            accept.doData(decodeTraceId, 0x80, maskingKey, payload);

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
            accept.doData(decodeTraceId, 0x81, maskingKey, payload);

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
            accept.doData(decodeTraceId, 0x82, maskingKey, payload);

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
                accept.doAbort(decodeTraceId, STATUS_PROTOCOL_ERROR);
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
                    accept.doEnd(decodeTraceId, code);
                    this.accept = null;
                    this.decodeState = this::decodeUnexpected;
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
                accept.doAbort(decodeTraceId, STATUS_PROTOCOL_ERROR);
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
            accept.doAbort(decodeTraceId, STATUS_PROTOCOL_ERROR);
            return length;
        }
    }

    private void doHttpBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        Consumer<ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private Flyweight.Builder.Visitor visitHttpBeginEx(
        Consumer<ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)
    {
        return (buffer, offset, limit) ->
            httpBeginExRW.wrap(buffer, offset, limit)
                         .typeId(httpTypeId)
                         .headers(headers)
                         .build()
                         .sizeof();
    }

    private Flyweight.Builder.Visitor visitWsDataEx(
        int flags)
    {
        return (buffer, offset, limit) ->
            wsDataExRW.wrap(buffer, offset, limit)
                      .typeId(wsTypeId)
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
                       .typeId(wsTypeId)
                       .protocol(protocol)
                       .build()
                       .sizeof();
    }

    private Flyweight.Builder.Visitor visitWsEndEx(
        short code)
    {
        return (buffer, offset, limit) ->
            wsEndExRW.wrap(buffer, offset, limit)
                     .typeId(wsTypeId)
                     .code(code)
                     .reason("")
                     .build()
                     .sizeof();
    }

    private Consumer<ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> setHttpHeaders(
        String scheme,
        String authority,
        String path,
        String handshakeKey,
        String protocol)
    {
        return headers ->
        {
            headers.item(h -> h.name(":method").value(WEBSOCKET_METHOD));
            if (scheme != null)
            {
                headers.item(h -> h.name(":scheme").value(scheme));
            }
            if (authority != null)
            {
                headers.item(h -> h.name(":authority").value(authority));
            }
            if (path != null)
            {
                headers.item(h -> h.name(":path").value(path));
            }
            headers.item(h -> h.name("upgrade").value(WEBSOCKET_UPGRADE));
            headers.item(h -> h.name("connection").value("upgrade"));
            headers.item(h -> h.name("sec-websocket-key").value(handshakeKey));
            headers.item(h -> h.name("sec-websocket-version").value(WEBSOCKET_VERSION_13));

            if (protocol != null)
            {
                headers.item(h -> h.name("sec-websocket-protocol").value(protocol));
            }
        };
    }

    private static String newSecWebsocketKey()
    {
        byte[] bytes = new byte[16];
        ThreadLocalRandom.current().nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static int wsHeaderLength(
        DirectBuffer buffer)
    {
        int wsHeaderLength = 2;
        byte secondByte = buffer.getByte(1);
        wsHeaderLength += lengthSize(secondByte) - 1;

        if (isMasked(secondByte))
        {
            wsHeaderLength += 4;
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
