/*
 * The MIT License
 *
 * Copyright 2021 Mastfrog Technologies.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.mastfrog.storage;

import com.mastfrog.util.strings.Strings;
import java.nio.ByteBuffer;

/**
 * Value types used for generic sorting and searching algorithms.
 *
 * @author Tim Boudreau
 */
public enum ValueType {
    INT, LONG, UNSIGNED_INT, SHORT, UNSIGNED_SHORT, BYTE, UNSIGNED_BYTE,
    /**
     * Not generally usable; used to sort one-to-many indexes by key *and* value
     * where duplicates are present.
     */
    LONG128;

    public String toString() {
        switch (this) {
            case UNSIGNED_INT:
                return "u32";
            case LONG:
                return "i64";
            case SHORT:
                return "i16";
            case UNSIGNED_SHORT:
                return "u16";
            case BYTE:
                return "i8";
            case UNSIGNED_BYTE:
                return "u8";
            case INT:
                return "i32";
            case LONG128:
                return "u128";
            default:
                throw new AssertionError(this);
        }
    }

    public int size() {
        switch (this) {
            case BYTE:
            case UNSIGNED_BYTE:
                return 1;
            case SHORT:
            case UNSIGNED_SHORT:
                return Short.BYTES;
            case INT:
            case UNSIGNED_INT:
                return Integer.BYTES;
            case LONG:
                return Long.BYTES;
            case LONG128:
                return Long.BYTES * 2;
            default:
                throw new AssertionError(this);
        }
    }

    public long read(ByteBuffer buf) {
        switch (this) {
            case INT:
                return buf.getInt();
            case UNSIGNED_INT:
                return Integer.toUnsignedLong(buf.getInt());
            case LONG:
                return buf.getLong();
            case SHORT:
                return buf.getShort();
            case UNSIGNED_SHORT:
                return Short.toUnsignedLong(buf.getShort());
            case BYTE:
                return buf.get();
            case UNSIGNED_BYTE:
                return Byte.toUnsignedLong(buf.get());
            case LONG128:
                throw new AssertionError("Cannot read 128 bits as a long");
            default:
                throw new AssertionError(this);
        }
    }

    public long read(int offset, ByteBuffer buf) {
        switch (this) {
            case INT:
                return buf.getInt(offset);
            case UNSIGNED_INT:
                return Integer.toUnsignedLong(buf.getInt(offset));
            case LONG:
                return buf.getLong(offset);
            case SHORT:
                return buf.getShort(offset);
            case UNSIGNED_SHORT:
                return Short.toUnsignedLong(buf.getShort(offset));
            case BYTE:
                return buf.get(offset);
            case UNSIGNED_BYTE:
                return Byte.toUnsignedLong(buf.get(offset));
            case LONG128:
                throw new AssertionError("Cannot read 128 bits as long");
            default:
                throw new AssertionError(this);
        }
    }

    public static final class Long128 implements Comparable<Long128> {

        private static final int FUDGE_FACTOR = 1_000_000;
        private final long msqw;
        private final long lsqw;

        public Long128(long msqw, long lsqw) {
            this.msqw = msqw;
            this.lsqw = lsqw;
        }

        public long mostSignificantQuadWord() {
            return msqw;
        }

        public long leastSignificantQuadWord() {
            return lsqw;
        }

        public static Long128 read(ByteBuffer buffer) {
            return new Long128(buffer.getLong(), buffer.getLong());
        }

        public static Long128 from(Number num) {
            return new Long128(0, num.longValue());
        }

        public String toString() {
            StringBuilder result = new StringBuilder();
            Strings.appendPaddedHex(msqw, result);
            Strings.appendPaddedHex(lsqw, result);
            return result.toString();
        }

        @Override
        public int compareTo(Long128 o) {
            int result = Long.compare(msqw, o.msqw) * FUDGE_FACTOR;
            if (result == 0) {
                result = Long.compare(lsqw, o.lsqw);
            }
            return result;
        }
    }
}
