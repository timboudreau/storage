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

import static com.mastfrog.util.preconditions.Checks.greaterThanZero;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.function.IntFunction;

/**
 *
 * @author Tim Boudreau
 */
public class SingleMappedStorage implements Storage {

    private final int recordSize;
    private final MappedByteBuffer mapping;
    private final Buffers buffers;
    private int ix;

    public SingleMappedStorage(int recordSize, MappedByteBuffer mapping, boolean direct, IntFunction<Buffers> bufs) {
        this.recordSize = greaterThanZero("recordSize", recordSize);
        this.mapping = mapping;
        buffers = bufs.apply(recordSize);
    }

    @Override
    public int recordSize() {
        return recordSize;
    }

    @Override
    public ByteBuffer read(int record) {
        int offset = (int) offsetOf(record);
        return mapping.slice(offset, recordSize);
    }

    @Override
    public ByteBuffer forIndex(long index) {
        long off = offsetOf(index);
        return mapping.slice((int) off, recordSize);
    }

    @Override
    public void writeAtBytePosition(long position, ByteBuffer buffer) {
        mapping.put((int) position, buffer, 0, buffer.limit());
    }

    @Override
    public long sizeInBytes() {
        return mapping.capacity();
    }

    @Override
    public void swap(int left, int right) {
        if (left == right) {
            return;
        }
        int l = (int) offsetOf(left);
        int r = (int) offsetOf(right);

        ByteBuffer copy = buffers.get(ix++);
        copy.put(0, mapping, l, recordSize);
        mapping.put(l, mapping, r, recordSize);
        mapping.put(r, copy, 0, recordSize);
    }

    @Override
    public void bulkSwap(int left, int right, int count) {
        if (left == right) {
            return;
        }
        if (count == 1) {
            swap(left, right);
            return;
        }
        int l = (int) offsetOf(left);
        int r = (int) offsetOf(right);
        int len = recordSize * count;
        ByteBuffer copy = buffers.allocate(len);
        copy.put(0, mapping, l, len);
        mapping.put(l, mapping, r, len);
        mapping.put(r, copy, 0, len);
    }

    @Override
    public void writeLong(long record, long value, int byteOffset) {
        mapping.putLong((int) offsetOf(record) + byteOffset, value);
    }

    @Override
    public void writeInt(long record, int value, int byteOffset) {
        mapping.putInt((int) offsetOf(record) + byteOffset, value);
    }

    @Override
    public void writeShort(long record, short value, int byteOffset) {
        mapping.putShort((int) offsetOf(record) + byteOffset, value);
    }
}
