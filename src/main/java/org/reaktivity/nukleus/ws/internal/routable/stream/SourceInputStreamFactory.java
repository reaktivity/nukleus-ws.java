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

import static java.lang.Integer.highestOneBit;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.agrona.BitUtil.SIZE_OF_SHORT;
import static org.reaktivity.nukleus.ws.internal.routable.Route.protocolMatches;
import static org.reaktivity.nukleus.ws.internal.router.RouteKind.OUTPUT_ESTABLISHED;
import static org.reaktivity.nukleus.ws.internal.types.stream.WsHeaderFW.STATUS_NORMAL_CLOSURE;
import static org.reaktivity.nukleus.ws.internal.types.stream.WsHeaderFW.STATUS_PROTOCOL_ERROR;

import java.security.MessageDigest;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.ws.internal.routable.Route;
import org.reaktivity.nukleus.ws.internal.routable.Source;
import org.reaktivity.nukleus.ws.internal.routable.Target;
import org.reaktivity.nukleus.ws.internal.router.Correlation;
import org.reaktivity.nukleus.ws.internal.types.OctetsFW;
import org.reaktivity.nukleus.ws.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.ws.internal.types.stream.DataFW;
import org.reaktivity.nukleus.ws.internal.types.stream.EndFW;
import org.reaktivity.nukleus.ws.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.ws.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.ws.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.ws.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.ws.internal.types.stream.WsHeaderFW;
import org.reaktivity.nukleus.ws.internal.util.function.LongObjectBiConsumer;

public final class SourceInputStreamFactory
{
    private static final byte[] HANDSHAKE_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11".getBytes(UTF_8);
    private static final String WEBSOCKET_VERSION_13 = "13";

    private static final int HEADER_SIZE_PAYLOAD_8_WITH_MASKING_KEY = 1 + 1 + 4;
    private static final int HEADER_SIZE_EXTENDED_PAYLOAD_16_WITH_MASKING_KEY = HEADER_SIZE_PAYLOAD_8_WITH_MASKING_KEY + 2;
    private static final int HEADER_SIZE_EXTENDED_PAYLOAD_64_WITH_MASKING_KEY = HEADER_SIZE_PAYLOAD_8_WITH_MASKING_KEY + 8;

    private static final int SLAB_SLOT_NOT_ALLOCATED = -1;

    private final MessageDigest sha1 = initSHA1();

    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final OctetsFW octetsRO = new OctetsFW();

    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();

    private final WsHeaderFW wsHeaderRO = new WsHeaderFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Source source;
    private final LongFunction<List<Route>> supplyRoutes;
    private final LongSupplier supplyTargetId;
    private final LongObjectBiConsumer<Correlation> correlateNew;
    private final Slab slab;

    public SourceInputStreamFactory(
        Source source,
        LongFunction<List<Route>> supplyRoutes,
        LongSupplier supplyTargetId,
        LongObjectBiConsumer<Correlation> correlateNew,
        Slab slab)
    {
        this.source = source;
        this.supplyRoutes = supplyRoutes;
        this.supplyTargetId = supplyTargetId;
        this.correlateNew = correlateNew;
        this.slab = slab;
    }

    public MessageHandler newStream()
    {
        return new SourceInputStream()::handleStream;
    }

    private final class SourceInputStream
    {
        private MessageHandler streamState;
        private LongObjectBiConsumer<OctetsFW> decodeState;

        private long sourceId;

        private Target target;
        private long targetId;

        private int slabSlot = SLAB_SLOT_NOT_ALLOCATED;
        private int slabSlotLimit = 0;
        private int slabSlotOffset = 0;

        private int payloadProgress;
        private int payloadLength;
        private int maskingKey;

        private SourceInputStream()
        {
            this.streamState = this::beforeBegin;
            this.decodeState = this::decodeHeader;
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

        private void afterReplyOrReset(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == DataFW.TYPE_ID)
            {
                dataRO.wrap(buffer, index, index + length);
                final long streamId = dataRO.streamId();

                source.doWindow(streamId, length);
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

            this.streamState = this::afterReplyOrReset;
        }

        private void processInvalidRequest(
            DirectBuffer buffer,
            int index,
            int length,
            long sourceRef,
            String status)
        {
            final Optional<Route> optional = resolveReplyTo(sourceRef);

            if (optional.isPresent())
            {
                final Route route = optional.get();
                final Target replyTo = route.target();
                final long targetRef = route.targetRef();
                final long newTargetId = supplyTargetId.getAsLong();

                replyTo.doHttpBegin(newTargetId, targetRef, newTargetId,
                        hs -> hs.item(h -> h.name(":status").value(status)));

                replyTo.doHttpEnd(newTargetId);

                this.streamState = this::afterReplyOrReset;
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
        }

        private void processBegin(
            DirectBuffer buffer,
            int index,
            int length)
        {
            final BeginFW begin = beginRO.wrap(buffer, index, index + length);

            final long newSourceId = begin.streamId();
            final long sourceRef = begin.sourceRef();
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
                final Optional<Route> optional = resolveTarget(sourceRef, protocols);

                if (optional.isPresent())
                {
                    final long newTargetId = supplyTargetId.getAsLong();
                    final long targetCorrelationId = newTargetId;

                    sha1.reset();
                    sha1.update(key.getBytes(US_ASCII));
                    final byte[] digest = sha1.digest(HANDSHAKE_GUID);
                    final Encoder encoder = Base64.getEncoder();
                    final String handshakeHash = new String(encoder.encode(digest), US_ASCII);

                    final Route route = optional.get();
                    final Target newTarget = route.target();
                    final long targetRef = route.targetRef();
                    final String protocol = resolveProtocol(protocols, route.protocol());

                    final Correlation correlation =
                            new Correlation(correlationId, source.routableName(), OUTPUT_ESTABLISHED, handshakeHash, protocol);

                    correlateNew.accept(targetCorrelationId, correlation);

                    newTarget.doWsBegin(newTargetId, targetRef, targetCorrelationId, protocol);
                    newTarget.addThrottle(newTargetId, this::handleThrottle);

                    this.sourceId = newSourceId;

                    this.target = newTarget;
                    this.targetId = newTargetId;
                }
                else
                {
                    processInvalidRequest(buffer, index, length, sourceRef, "400");
                }
            }
            else
            {
                processInvalidRequest(buffer, index, length, sourceRef, "404");
            }

            this.streamState = this::afterBeginOrData;
        }

        private void processData(
            DirectBuffer buffer,
            int index,
            int length)
        {
            final DataFW data = dataRO.wrap(buffer, index, index + length);

            OctetsFW payload = data.payload();
            if(this.slabSlotLimit != 0)
            {
                MutableDirectBuffer slabBuffer = slab.buffer(this.slabSlot, this.slabSlotOffset);
                slabBuffer.putBytes(this.slabSlotLimit, payload.buffer(), payload.offset(), payload.sizeof());
                this.slabSlotLimit += data.length();
                payload = octetsRO.wrap(slabBuffer, 0, slabSlotLimit);
            }

            long streamId = data.streamId();
            decodeState.accept(streamId, payload);
        }

        private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);
            final long streamId = endRO.streamId();

            target.doWsEnd(targetId, STATUS_NORMAL_CLOSURE);

            this.streamState = this::afterEnd;

            source.removeStream(streamId);
            target.removeThrottle(targetId);
        }

        private int decodeHeader(
            final long streamId,
            final OctetsFW httpPayload)
        {
            final DirectBuffer buffer = httpPayload.buffer();
            final int offset = httpPayload.offset();
            final int limit = httpPayload.limit();

            int bytesWritten = 0;
            int nextOffset = offset;
            for (; nextOffset < limit; nextOffset = wsHeaderRO.limit())
            {
                if(wsHeaderRO.canWrap(buffer, nextOffset, limit))
                {
                    final WsHeaderFW wsHeader = wsHeaderRO.wrap(buffer, nextOffset, limit);
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
                        httpPayload.wrap(httpPayload.buffer(), httpPayload.offset() + wsHeader.sizeof(), httpPayload.limit());
                        this.decodeState.accept(streamId, httpPayload);
                    }
                    else
                    {
                        target.doWsEnd(targetId, STATUS_PROTOCOL_ERROR);
                    }
                }
                else
                {
                    if(this.slabSlot == SLAB_SLOT_NOT_ALLOCATED)
                    {
                        // if not in SLAB already, then add to SLAB
                        this.slabSlot = slab.acquire(streamId);
                        MutableDirectBuffer slabBuffer = slab.buffer(slabSlot);
                        slabBuffer.putBytes(0, buffer, nextOffset, limit);
                        this.slabSlotLimit = limit - nextOffset;
                    }
                    else
                    {
                        this.slabSlotOffset = nextOffset;
                    }
                    break;
                }
            }

            if(nextOffset == limit && this.slabSlot != SLAB_SLOT_NOT_ALLOCATED)
            {
                slab.release(this.slabSlot);
                this.slabSlotOffset = 0;
                this.slabSlot = SLAB_SLOT_NOT_ALLOCATED;
            }

            return bytesWritten;
        }

        private void decodeText(
            final long streamId,
            final OctetsFW payload)
        {
            // TODO canWrap for UTF-8 split multi-byte characters
            final int payloadSize = payload.sizeof();
            final int decodeBytes = Math.min(payloadSize, payloadLength - payloadProgress);

            payload.wrap(payload.buffer(), payload.offset(), payload.offset() + decodeBytes);
            target.doWsData(targetId, 0x81, maskingKey, payload);

            payloadProgress += decodeBytes;
            maskingKey = (maskingKey >>> decodeBytes & 0x03) | (maskingKey << (Integer.SIZE - decodeBytes & 0x03));

            if (payloadProgress == payloadLength)
            {
                this.decodeState = this::decodeHeader;
            }

            if (payloadSize > payload.sizeof())
            {
                payload.wrap(payload.buffer(), payload.sizeof(), payloadSize);
                this.decodeState.accept(streamId, payload);
            }
        }

        private void decodeBinary(
            final long streamId,
            final OctetsFW payload)
        {
            final int payloadSize = payload.sizeof();
            if (payloadSize > 0)
            {
                final int decodeBytes = Math.min(payloadSize, payloadLength - payloadProgress);

                payload.wrap(payload.buffer(), payload.offset(), payload.offset() + decodeBytes);
                target.doWsData(targetId, 0x82, maskingKey, payload);

                payloadProgress += decodeBytes;
                maskingKey = (maskingKey >>> decodeBytes & 0x03) | (maskingKey << (Integer.SIZE - decodeBytes & 0x03));

                if (payloadProgress == payloadLength)
                {
                    this.decodeState = this::decodeHeader;
                }

                if (payloadSize > payload.sizeof())
                {
                    payload.wrap(payload.buffer(), payload.sizeof(), payloadSize);
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
            target.doWsEnd(targetId, status);
        }

        private Optional<Route> resolveTarget(
            long sourceRef,
            String protocols)
        {
            final List<Route> routes = supplyRoutes.apply(sourceRef);
            final Predicate<Route> predicate = protocolMatches(protocols);

            return routes.stream().filter(predicate).findFirst();
        }

        private String resolveProtocol(
            final String protocols,
            final String protocol)
        {
            return (protocols != null) && protocols.contains(protocol) ? protocol : null;
        }

        private Optional<Route> resolveReplyTo(
            long sourceRef)
        {
            final List<Route> routes = supplyRoutes.apply(sourceRef);
            final Predicate<Route> predicate = Route.sourceMatches(source.routableName());

            return routes.stream().filter(predicate).findFirst();
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
                processWindow(buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        private void processWindow(
            DirectBuffer buffer,
            int index,
            int length)
        {
            windowRO.wrap(buffer, index, index + length);

            final int update = windowRO.update();

            source.doWindow(sourceId, update - headerSize(update));
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

    private static int headerSize(
        int payloadSize)
    {
        switch (highestOneBit(payloadSize))
        {
        case 0:
        case 1:
        case 2:
        case 4:
        case 8:
        case 16:
        case 32:
            return HEADER_SIZE_PAYLOAD_8_WITH_MASKING_KEY;
        case 64:
            return headerSize64to127(payloadSize);
        case 128:
            return HEADER_SIZE_EXTENDED_PAYLOAD_16_WITH_MASKING_KEY;
        default:
            return HEADER_SIZE_EXTENDED_PAYLOAD_64_WITH_MASKING_KEY;
        }
    }

    private static int headerSize64to127(
        int payloadSize)
    {
        switch (payloadSize)
        {
        case 126:
        case 127:
            return HEADER_SIZE_EXTENDED_PAYLOAD_16_WITH_MASKING_KEY;
        default:
            return HEADER_SIZE_PAYLOAD_8_WITH_MASKING_KEY;
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
}
