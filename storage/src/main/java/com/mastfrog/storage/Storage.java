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

import com.mastfrog.abstractions.list.LongIndexed;
import com.mastfrog.storage.ValueType.Long128;
import com.mastfrog.util.search.Bias;
import com.mastfrog.util.search.BinarySearch;
import com.mastfrog.util.sort.Sort;
import com.mastfrog.util.sort.Swapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntBinaryOperator;
import java.util.function.LongUnaryOperator;

/**
 * Disk-backed storage of index-addressed, fixed-length records which can be
 * binary-searched against, sorted, etc. Multiple implementations such as
 * memory-mapped and file-channel based are available.
 *
 * @author Tim Boudreau
 */
public interface Storage extends Swapper, LongIndexed<ByteBuffer>, Iterable<ByteBuffer> {

    public static long TWO_GB = 2147483648L;

    public static Storage create(FileChannel channel, StorageSpecification spec) throws IOException {
        if (spec.alwaysMapped) {
            if (channel.size() > TWO_GB) {
                return new MultiMappedStorage(spec.size, channel,
                        spec.writable ? MapMode.READ_WRITE : MapMode.READ_ONLY,
                        spec.preferDirect, sz -> new Buffers(sz, spec.concurrency,
                                spec.preferDirect));
            } else {
                MappedByteBuffer mapping = channel.map(
                        spec.writable ? MapMode.READ_WRITE : MapMode.READ_ONLY, 0, channel.size());
                return new SingleMappedStorage(spec.size, mapping, spec.preferDirect,
                        sz -> new Buffers(sz, spec.concurrency, spec.preferDirect));
            }
        } else {
            return new AdaptiveStorage(spec.size, channel, spec.writable,
                    spec.preferMapped, 0, spec.writable,
                    sz -> new Buffers(spec.size, spec.concurrency, spec.preferDirect),
                    spec.preferDirect);
        }
    }

    @Override
    default Iterator<ByteBuffer> iterator() {
        return iteratorFrom(0);
    }

    default Iterator<ByteBuffer> iteratorFrom(long startingRecord) {
        return new StorageIterator(this, startingRecord);
    }

    default PrimitiveIterator.OfLong longIterator(int byteOffset) {
        return new StorageIterator(this, 0).adapted(byteOffset);
    }

    default PrimitiveIterator.OfLong longIterator(long startingRecord, int byteOffset) {
        return new StorageIterator(this, startingRecord).adapted(byteOffset);
    }

    @Override
    default Iterable<ByteBuffer> toIterable() {
        return this;
    }

    @Override
    public default Spliterator<ByteBuffer> spliterator() {
        return new StorageSpliterator(this, 0, size());
    }

    @Override
    default Iterator<ByteBuffer> toIterator() {
        return iterator();
    }

    @Override
    default void forEach(Consumer<? super ByteBuffer> action) {
        long sz = size();
        for (long i = 0; i < sz; i++) {
            action.accept(forIndex(i));
        }
    }

    /**
     * Describes the desired characteristics of a desired Storage instance
     * without tying it to a specific implementation type. Setter methods mutate
     * the instance.
     */
    public static class StorageSpecification {

        boolean preferMapped = true;
        boolean preferDirect = true;
        boolean alwaysMapped = false;
        boolean writable = true;
        int concurrency = 4;
        final int size;

        public StorageSpecification(int size) {
            this.size = size;
        }

        public Buffers buffers() {
            return new Buffers(size, concurrency, preferDirect);
        }

        /**
         * Creates a new instance with defaults and a size of zero for use as a
         * template.
         *
         * @return A spec
         */
        public static StorageSpecification defaultSpec() {
            return new StorageSpecification(0);
        }

        public boolean isReadWrite() {
            return writable;
        }

        public boolean isDirect() {
            return preferDirect;
        }

        public boolean isMapped() {
            return alwaysMapped || preferMapped;
        }

        public StorageSpecification copy() {
            StorageSpecification result = new StorageSpecification(size);
            result.preferMapped = preferMapped;
            result.preferDirect = preferDirect;
            result.alwaysMapped = alwaysMapped;
            result.writable = writable;
            result.concurrency = concurrency;
            return result;
        }

        public StorageSpecification withSize(int size) {
            StorageSpecification result = new StorageSpecification(size);
            result.preferMapped = preferMapped;
            result.preferDirect = preferDirect;
            result.alwaysMapped = alwaysMapped;
            result.writable = writable;
            result.concurrency = concurrency;
            return result;
        }

        public StorageSpecification readOnly() {
            writable = false;
            return this;
        }

        public StorageSpecification readWrite() {
            writable = true;
            return this;
        }

        public StorageSpecification direct() {
            preferDirect = true;
            return this;
        }

        public StorageSpecification heap() {
            preferDirect = false;
            return this;
        }

        public StorageSpecification initiallyMapped() {
            preferMapped = true;
            return this;
        }

        public StorageSpecification initiallyUnmapped() {
            preferMapped = false;
            return this;
        }

        public StorageSpecification alwaysMapped() {
            preferMapped = true;
            alwaysMapped = true;
            return this;
        }

        /**
         * This is not the usual meaning of "concurrency" - it refers to how
         * many buffers may be used by a <i>single</i> thread such that none of
         * them should be recycled - i.e. when sorting, one needs at least two
         * buffers if you want to compare their contents - recycling the same
         * buffer and changing its contents would be counterproductive.
         * <p>
         * Storage uses a thread local pool of buffers.
         *
         * @param val The number of buffers in each thread's pool
         * @return this
         */
        public StorageSpecification concurrency(int val) {
            concurrency = val;
            return this;
        }
    }

    /**
     * The number of bytes one logical record in this storage takes up.
     *
     * @return An int
     */
    int recordSize();

    /**
     * Read one record at the specified index (recordLength() * record bytes
     * into the file).
     * <p>
     * <b>Do not hang on to the ByteBuffers returned here</b> - they are pooled
     * in various ways, and may even be mappings over live data - they are not
     * copies. Read what you need and be done with them.
     *
     * @param record The record index
     * @return A buffer
     */
    ByteBuffer read(int record);

    /**
     * Overwrite the record at the specified index with the passed buffer. If
     * the buffer has zero bytes remaining, it is flipped before writing.
     *
     * @param recordIndex The record index
     * @param buffer A buffer
     */
    default void write(long recordIndex, ByteBuffer buffer) {
        long rs = recordSize();
        if (buffer.remaining() == 0) {
            buffer.flip();
        }
        writeAtBytePosition(rs * recordIndex, buffer);
    }

    /**
     * Write a buffer at an absolute position in bytes into the backing file.
     * The buffer *must* be of the record size or some multiple of it - if it is
     * not, the consequences are undefined - they vary by implementation.
     *
     * @param bytes The byte position
     * @param buffer A buffer
     */
    void writeAtBytePosition(long bytes, ByteBuffer buffer);

    /**
     * Get the size in bytes of the backing storage.
     *
     * @return The size of the backing storage
     */
    long sizeInBytes();

    default void writeLong(long record, long value, int byteOffset) {
        ByteBuffer buf = forIndex(record);
        buf.putLong(byteOffset, value);
        write(record, buf);
    }

    default void writeInt(long record, int value, int byteOffset) {
        ByteBuffer buf = forIndex(record);
        buf.putInt(byteOffset, value);
        write(record, buf);
    }

    default void writeShort(long record, short value, int byteOffset) {
        ByteBuffer buf = forIndex(record);
        buf.putShort(byteOffset, value);
        write(record, buf);
    }

    /**
     * Read a value of the specified type at the specified byte offset from the
     * specified element index, coercing it to a long via the value type.
     *
     * @param index the index of the record
     * @param fieldOffset the offset in bytes into the record to read the value
     * @param valueType the size of the value to read and/or conversion to apply
     * to it
     * @return the requested value
     */
    default long readValue(long index, int fieldOffset, ValueType valueType) {
        ByteBuffer buf = forIndex(index);
        return valueType.read(fieldOffset, buf);
    }

    /**
     * Binary search this storage - you must KNOW the storage is sorted, fully,
     * on fields defined as the value type and offset into each record, before
     * calling this method.
     *
     * @param value The value sought
     * @param fieldOffset The number of bytes into each record to read the value
     * at
     * @param sortType The size of the value to read and/or conversion to apply
     * to it
     * @return the index of the matching value or -1
     */
    default long binarySearch(long value, int fieldOffset, ValueType sortType) {
        return binarySearch(value, fieldOffset, sortType, Bias.NONE);
    }

    default long binarySearch(long value, RecordElement item, Bias bias) {
        return binarySearch(value, item.byteOffset(), item.type(), bias);
    }

    /**
     * Binary search this storage - you must KNOW the storage is sorted, fully,
     * on fields defined as the value type and offset into each record, before
     * calling this method.
     *
     * @param value The value sought
     * @param fieldOffset The number of bytes into each record to read the value
     * at
     * @param sortType The size of the value to read and/or conversion to apply
     * to it
     * @param bias A bias to apply when an exact match is not found, to return
     * the nearest value along some axis (closest in value, next greatesr, next
     * lower, or none [returns -1])
     * @return the index of the matching value or -1
     */
    default long binarySearch(long value, int fieldOffset, ValueType sortType, Bias bias) {
        LongUnaryOperator comparer = test -> {
            long val = sortType.read(fieldOffset, forIndex(test));
            int result = Long.compare(value, val);
            if (result == 0) {
                switch (bias) {
                    case BACKWARD:
                        // The binary search algorithm is not duplicate-tolerant
                        // by default, so we can land on the *last* entry, and need
                        // to scan backwards to the first
                        int adjust = 0;
                        long off = test - 1;
                        while (off > 0) {
                            long prevVal = sortType.read(fieldOffset, forIndex(off));
                            if (prevVal == val) {
                                adjust++;
                            } else {
                                break;
                            }
                            off--;
                        }
                        return -adjust;
                }
            }
            return result;
        };
        return BinarySearch.search(0, size(), comparer, bias);
    }

    /**
     * Sort the contents of this storage, modifying it on disk, using the passed
     * specification for what to sort on.
     *
     * @param fieldOffset The offset into each record where the value to sort on
     * is found
     * @param sortType The size and/or conversion to apply to the value
     */
    default void sort(int fieldOffset, ValueType sortType) {
        long sz = size();
        if (sz > Integer.MAX_VALUE) {
            throw new IllegalStateException("Default sort impl can't handle size of " + size());
        }
        IntBinaryOperator sortOp;
        switch (sortType) {
            case INT:
                sortOp = intComparator(fieldOffset);
                break;
            case LONG:
                sortOp = longComparator(fieldOffset);
                break;
            case UNSIGNED_INT:
                sortOp = unsignedIntComparator(fieldOffset);
                break;
            case BYTE:
                sortOp = byteComparator(fieldOffset);
                break;
            case UNSIGNED_BYTE:
                sortOp = unsignedByteComparator(fieldOffset);
                break;
            case SHORT:
                sortOp = shortComparator(fieldOffset);
                break;
            case UNSIGNED_SHORT:
                sortOp = unsignedShortComparator(fieldOffset);
                break;
            case LONG128:
                sortOp = long128Comparator(fieldOffset);
                break;
            default:
                throw new AssertionError(sortType);
        }
        Sort.sortAdhoc(this, (int) sz, sortOp);
    }

    default long offsetOf(long record) {
        return record * (long) recordSize();
    }

    default long size() {
        long b = sizeInBytes();
        if (b == 0) {
            return 0;
        }
        long rs = recordSize();
        return b / rs;
    }

    default IntBinaryOperator longComparator(int offsetIntoRecord) {
        return (a, b) -> {
            if (a == b) {
                return 0;
            }
            ByteBuffer buf = read(a);
            if (offsetIntoRecord > 0) {
                buf.position(offsetIntoRecord);
            }
            long va  = buf.getLong();
            buf = read(b);
            if (offsetIntoRecord > 0) {
                buf.position(offsetIntoRecord);
            }
            long vb = buf.getLong();
            return Long.compare(va, vb);
        };
    }

    default IntBinaryOperator long128Comparator(int offsetIntoRecord) {
        return (a, b) -> {
            if (a == b) {
                return 0;
            }
            ByteBuffer buf = read(a);
            if (offsetIntoRecord > 0) {
                buf.position(offsetIntoRecord);
            }
            Long128 va  = Long128.read(buf);
            buf = read(b);
            if (offsetIntoRecord > 0) {
                buf.position(offsetIntoRecord);
            }
            Long128 vb = Long128.read(buf);
            return va.compareTo(vb);
        };
    }

    default IntBinaryOperator intComparator(int offsetIntoRecord) {
        return (a, b) -> {
            if (a == b) {
                return 0;
            }
            ByteBuffer buf = read(a);
            if (offsetIntoRecord > 0) {
                buf.position(offsetIntoRecord);
            }
            int va  = buf.getInt();
            buf = read(b);
            if (offsetIntoRecord > 0) {
                buf.position(offsetIntoRecord);
            }
            int vb = buf.getInt();
            return Integer.compare(va, vb);
        };
    }

    default IntBinaryOperator unsignedIntComparator(int offsetIntoRecord) {
        return (a, b) -> {
            if (a == b) {
                return 0;
            }
            ByteBuffer buf = read(a);
            if (offsetIntoRecord > 0) {
                buf.position(offsetIntoRecord);
            }
            long va  = Integer.toUnsignedLong(buf.getInt());
            buf = read(b);
            if (offsetIntoRecord > 0) {
                buf.position(offsetIntoRecord);
            }
            long vb = Integer.toUnsignedLong(buf.getInt());
            return Long.compare(va, vb);
        };
    }

    default IntBinaryOperator shortComparator(int offsetIntoRecord) {
        return (a, b) -> {
            if (a == b) {
                return 0;
            }
            ByteBuffer buf = read(a);
            long va  = ValueType.SHORT.read(offsetIntoRecord, buf);
            buf = read(b);
            long vb = ValueType.SHORT.read(offsetIntoRecord, buf);
            return Long.compare(va, vb);
        };
    }

    default IntBinaryOperator unsignedShortComparator(int offsetIntoRecord) {
        return (a, b) -> {
            if (a == b) {
                return 0;
            }
            ByteBuffer buf = read(a);
            long va  = ValueType.UNSIGNED_SHORT.read(offsetIntoRecord, buf);
            buf = read(b);
            long vb = ValueType.UNSIGNED_SHORT.read(offsetIntoRecord, buf);
            return Long.compare(va, vb);
        };
    }

    default IntBinaryOperator unsignedByteComparator(int offsetIntoRecord) {
        return (a, b) -> {
            if (a == b) {
                return 0;
            }
            ByteBuffer buf = read(a);
            long va  = ValueType.UNSIGNED_BYTE.read(offsetIntoRecord, buf);
            buf = read(b);
            long vb = ValueType.UNSIGNED_BYTE.read(offsetIntoRecord, buf);
            return Long.compare(va, vb);
        };
    }

    default IntBinaryOperator byteComparator(int offsetIntoRecord) {
        return (a, b) -> {
            if (a == b) {
                return 0;
            }
            ByteBuffer buf = read(a);
            long va  = ValueType.BYTE.read(offsetIntoRecord, buf);
            buf = read(b);
            long vb = ValueType.BYTE.read(offsetIntoRecord, buf);
            return Long.compare(va, vb);
        };
    }
}
