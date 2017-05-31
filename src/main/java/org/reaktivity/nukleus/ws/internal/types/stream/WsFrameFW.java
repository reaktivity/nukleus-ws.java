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
package org.reaktivity.nukleus.ws.internal.types.stream;

import static java.lang.Integer.highestOneBit;

import java.nio.ByteOrder;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.ws.internal.types.Flyweight;

public final class WsFrameFW extends Flyweight
{
    public static final short STATUS_NORMAL_CLOSURE = (short) 1000;
    public static final short STATUS_GOING_AWAY = (short) 1001;
    public static final short STATUS_PROTOCOL_ERROR = (short) 1002;
    public static final short STATUS_UNSUPPORTED_DATA = (short) 1003;
    public static final short STATUS_INVALID_UTF8 = (short) 1007;
    public static final short STATUS_POLICY_VIOLATION = (short) 1008;
    public static final short STATUS_MESSAGE_TOO_LARGE = (short) 1009;
    public static final short STATUS_EXTENSIONS_MISSING = (short) 1010;
    public static final short STATUS_UNEXPECTED_CONDITION = (short) 1011;

    private static final int FIELD_OFFSET_FLAGS_AND_OPCODE = 0;
    private static final int FIELD_SIZE_FLAGS_AND_OPCODE = BitUtil.SIZE_OF_BYTE;

    private static final int FIELD_OFFSET_MASK_AND_LENGTH = FIELD_OFFSET_FLAGS_AND_OPCODE + FIELD_SIZE_FLAGS_AND_OPCODE;

    private static final int FIELD_SIZE_MASKING_KEY = BitUtil.SIZE_OF_INT;

    private final AtomicBuffer payloadRO = new UnsafeBuffer(new byte[0]);

    public boolean fin()
    {
        return (buffer().getByte(offset() + FIELD_OFFSET_FLAGS_AND_OPCODE) & 0x80) != 0x00;
    }

    public boolean rsv1()
    {
        return (buffer().getByte(offset() + FIELD_OFFSET_FLAGS_AND_OPCODE) & 0x40) != 0x00;
    }

    public boolean rsv2()
    {
        return (buffer().getByte(offset() + FIELD_OFFSET_FLAGS_AND_OPCODE) & 0x20) != 0x00;
    }

    public boolean rsv3()
    {
        return (buffer().getByte(offset() + FIELD_OFFSET_FLAGS_AND_OPCODE) & 0x10) != 0x00;
    }

    public int opcode()
    {
        return buffer().getByte(offset() + FIELD_OFFSET_FLAGS_AND_OPCODE) & 0x0f;
    }

    private boolean isMasked(byte b)
    {
        return (b & 0x80) != 0;
    }

    public boolean mask()
    {
        return isMasked(buffer().getByte(offset() + FIELD_OFFSET_MASK_AND_LENGTH));
    }

    public int maskingKey()
    {
        return buffer().getInt(offset() + FIELD_OFFSET_FLAGS_AND_OPCODE + FIELD_SIZE_FLAGS_AND_OPCODE + lengthSize());
    }

    public DirectBuffer payload()
    {
        return payloadRO;
    }

    @Override
    public int limit()
    {
        return payloadOffset() + length();
    }

    public boolean canWrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        int maxLength = maxLimit - offset;
        int wsFrameLength = 2;
        if(maxLength < wsFrameLength)
        {
           return false;
        }

        byte secondByte = buffer.getByte(offset + 1);
        wsFrameLength += lengthSize(secondByte) - 1;

        if(maxLength < wsFrameLength)
        {
            return false;
        }

        wsFrameLength += length(buffer, offset);

        if(isMasked(secondByte))
        {
            wsFrameLength+= 4;
        }

        return wsFrameLength <= maxLength;

    }

    @Override
    public WsFrameFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        payloadRO.wrap(buffer, payloadOffset(), length());

        checkLimit(limit(), maxLimit);

        return this;
    }

    @Override
    public String toString()
    {
        return String.format("[fin=%s, opcode=%d, payload.length=%d]", fin(), opcode(), length());
    }

    private int payloadOffset()
    {
        int payloadOffset = offset() + FIELD_SIZE_FLAGS_AND_OPCODE + lengthSize();

        if (mask())
        {
            payloadOffset += FIELD_SIZE_MASKING_KEY;
        }

        return payloadOffset;
    }

    private int lengthSize()
    {
        return lengthSize(buffer().getByte(offset() + FIELD_OFFSET_MASK_AND_LENGTH));
    }

    private int length()
    {
        return length(buffer(), offset());
    }

    public static final class Builder extends Flyweight.Builder<WsFrameFW>
    {
        public Builder()
        {
            super(new WsFrameFW());
        }

        @Override
        public Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);
            return this;
        }

        public Builder flagsAndOpcode(int flagsAndOpcode)
        {
            buffer().putByte(offset() + FIELD_OFFSET_FLAGS_AND_OPCODE, (byte) flagsAndOpcode);
            return this;
        }

        public Builder payload(DirectBuffer buffer)
        {
            return payload(buffer, 0, buffer.capacity());
        }

        public Builder payload(DirectBuffer buffer, int offset, int length)
        {
            switch (highestOneBit(length))
            {
            case 0:
            case 1:
            case 2:
            case 4:
            case 8:
            case 16:
            case 32:
                buffer().putByte(offset() + FIELD_OFFSET_MASK_AND_LENGTH, (byte) length);
                buffer().putBytes(offset() + FIELD_OFFSET_MASK_AND_LENGTH + 1, buffer, offset, length);
                super.limit(offset() + FIELD_OFFSET_MASK_AND_LENGTH + 1 + length);
                break;
            case 64:
                switch (length)
                {
                case 126:
                case 127:
                    buffer().putByte(offset() + FIELD_OFFSET_MASK_AND_LENGTH, (byte) 126);
                    buffer().putShort(offset() + FIELD_OFFSET_MASK_AND_LENGTH + 1, (short) length, ByteOrder.BIG_ENDIAN);
                    buffer().putBytes(offset() + FIELD_OFFSET_MASK_AND_LENGTH + 3, buffer, offset, length);
                    super.limit(offset() + FIELD_OFFSET_MASK_AND_LENGTH + 3 + length);
                    break;
                default:
                    buffer().putByte(offset() + FIELD_OFFSET_MASK_AND_LENGTH, (byte) length);
                    buffer().putBytes(offset() + FIELD_OFFSET_MASK_AND_LENGTH + 1, buffer, offset, length);
                    super.limit(offset() + FIELD_OFFSET_MASK_AND_LENGTH + 1 + length);
                    break;
                }
                break;
            case 128:
                buffer().putByte(offset() + FIELD_OFFSET_MASK_AND_LENGTH, (byte) 126);
                buffer().putShort(offset() + FIELD_OFFSET_MASK_AND_LENGTH + 1, (short) length, ByteOrder.BIG_ENDIAN);
                buffer().putBytes(offset() + FIELD_OFFSET_MASK_AND_LENGTH + 3, buffer, offset, length);
                super.limit(offset() + FIELD_OFFSET_MASK_AND_LENGTH + 3 + length);
                break;
            default:
                buffer().putByte(offset() + FIELD_OFFSET_MASK_AND_LENGTH, (byte) 127);
                buffer().putLong(offset() + FIELD_OFFSET_MASK_AND_LENGTH + 1, length, ByteOrder.BIG_ENDIAN);
                buffer().putBytes(offset() + FIELD_OFFSET_MASK_AND_LENGTH + 9, buffer, offset, length);
                super.limit(offset() + FIELD_OFFSET_MASK_AND_LENGTH + 9 + length);
                break;
            }

            return this;
        }
    }

    private static int length(DirectBuffer buffer, int offset)
    {
        int length = buffer.getByte(offset + FIELD_OFFSET_MASK_AND_LENGTH) & 0x7f;

        switch (length)
        {
        case 0x7e:
            return buffer.getShort(offset + FIELD_OFFSET_MASK_AND_LENGTH + 1, ByteOrder.BIG_ENDIAN) & 0xffff;

        case 0x7f:
            long length8bytes = buffer.getLong(offset + FIELD_OFFSET_MASK_AND_LENGTH + 1, ByteOrder.BIG_ENDIAN);
            validateLength(length8bytes);
            return (int) length8bytes & 0xffffffff;

        default:
            return length;
        }
    }

    private static void validateLength(
            long length8bytes)
    {
        if (length8bytes >> 17L != 0L)
        {
            throw new IllegalStateException("frame payload too long");
        }
    }

    private static int lengthSize(byte b)
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
}
