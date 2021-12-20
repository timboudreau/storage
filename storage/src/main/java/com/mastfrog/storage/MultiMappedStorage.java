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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.function.IntFunction;

/**
 *
 * @author Tim Boudreau
 */
public class MultiMappedStorage implements Storage {

    private final int recordSize;
    private final long partitionSize;
    private final int recordsPerPartition;
    private final MappedByteBuffer[] partitions;
    private final Buffers buffers;
    private int ix;

    MultiMappedStorage(int recordSize, FileChannel channel, MapMode mode,
            boolean direct, IntFunction<Buffers> bufs) throws IOException {
        this(recordSize, channel, mode, direct, (int) (2147483648L / (long) recordSize), bufs);
    }

    MultiMappedStorage(int recordSize, FileChannel channel, MapMode mode, boolean direct, int recordsPerPartition, IntFunction<Buffers> bufs) throws IOException {
        buffers = bufs.apply(recordSize);
        this.recordSize = greaterThanZero("recordSize", recordSize);
        long rs = recordSize;
        this.recordsPerPartition = recordsPerPartition;
        // Ensure a requested byte buffer can never straddle two map buffers
        partitionSize = recordsPerPartition * rs;
        int numPartitions = (int) (channel.size() / partitionSize);
        boolean reduce = false;
        long origSize = channel.size();
        if (origSize % partitionSize != 0) {
            numPartitions++;
            reduce = true;
        }
        partitions = new MappedByteBuffer[numPartitions];
        long start = 0;
        for (int i = 0; i < numPartitions; i++) {
            long sz = partitionSize;
            if (i == numPartitions - 1 && reduce) {
                sz = origSize - start;
            }
            partitions[i] = channel.map(mode, start, sz);
            start += partitionSize;
        }
    }

    interface PartitionConsumer<T> {

        T withPartition(int recordOffset, MappedByteBuffer partition);
    }

    private <T> T withPositionAndPartition(int record, PartitionConsumer<T> c) {
        int recordOffset = (record % recordsPerPartition) * recordSize;
        int partitionNumber = record / recordsPerPartition;
        MappedByteBuffer buf = partitions[partitionNumber];
        return c.withPartition(recordOffset, buf);
    }

    private <T> T withPositionAndPartition(long record, PartitionConsumer<T> c) {
        int recordOffset = (int) ((record % recordsPerPartition) * recordSize);
        int partitionNumber = (int) (record / recordsPerPartition);
        MappedByteBuffer buf = partitions[partitionNumber];
        return c.withPartition(recordOffset, buf);
    }

    @Override
    public int recordSize() {
        return recordSize;
    }

    @Override
    public ByteBuffer forIndex(long index) {
        return withPositionAndPartition(index, (offset, partition) -> {
            return partition.slice(offset, recordSize);
        });
    }

    @Override
    public ByteBuffer read(int record) {
        return withPositionAndPartition(record, (offset, partition) -> {
            return partition.slice(offset, recordSize);
        });
    }

    @Override
    public void writeAtBytePosition(long bytes, ByteBuffer buffer) {
        if (buffer.remaining() == 0) {
            buffer.flip();
        }
        int bufferIndex = (int) (bytes / partitionSize);
        int offset = (int) (bytes % partitionSize);
        partitions[bufferIndex].put(offset, buffer, 0, recordSize);
    }

    @Override
    public long sizeInBytes() {
        return partitions[partitions.length - 1].capacity()
                + partitionSize * (partitions.length - 1);
    }

    @Override
    public void swap(int left, int right) {
        if (left == right) {
            return;
        }
        withPositionAndPartition(left, (offsetLeft, partitionLeft) -> {
            return withPositionAndPartition(right, (offsetRight, partitionRight) -> {
                if (partitionLeft == partitionRight) {
                    ByteBuffer copy = buffers.get(ix++);
                    copy.put(0, partitionLeft, offsetLeft, recordSize);
                    partitionLeft.put(offsetLeft, partitionLeft, offsetRight, recordSize);
                    partitionRight.put(offsetRight, copy, 0, recordSize);
                } else {
                    ByteBuffer copy = buffers.get(ix++);
                    copy.put(0, partitionLeft, offsetLeft, recordSize);
                    partitionLeft.put(offsetLeft, partitionRight, offsetRight, recordSize);
                    partitionRight.put(offsetRight, copy, 0, recordSize);
                }
                return null;
            });
        });
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
        boolean swapped = withPositionAndPartition(left, (offsetLeft, partitionLeft) -> {
            return withPositionAndPartition(right, (offsetRight, partitionRight) -> {
                if (offsetLeft + count < recordsPerPartition && offsetRight + count < recordsPerPartition) {
                    int recordSize = this.recordSize * count;
                    ByteBuffer copy = buffers.get(ix++);
                    copy.put(0, partitionLeft, offsetLeft, recordSize);
                    partitionLeft.put(offsetLeft, partitionRight, offsetRight, recordSize);
                    partitionRight.put(offsetRight, copy, 0, recordSize);
                    return true;
                }
                return false;
            });
        });
        if (!swapped) {
            Storage.super.bulkSwap(left, right, count);
        }
    }

    @Override
    public void writeShort(long record, short value, int byteOffset) {
        withPositionAndPartition(record, (offset, partition) -> {
            partition.putShort(offset + byteOffset, value);
            return null;
        });
    }

    @Override
    public void writeInt(long record, int value, int byteOffset) {
        withPositionAndPartition(record, (offset, partition) -> {
            partition.putInt(offset + byteOffset, value);
            return null;
        });
    }

    @Override
    public void writeLong(long record, long value, int byteOffset) {
        withPositionAndPartition(record, (offset, partition) -> {
            partition.putLong(offset + byteOffset, value);
            return null;
        });
    }
}
