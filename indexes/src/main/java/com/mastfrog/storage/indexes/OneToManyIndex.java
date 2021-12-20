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
package com.mastfrog.storage.indexes;

import com.mastfrog.bits.Bits;
import com.mastfrog.bits.MutableBits;
import com.mastfrog.storage.indexes.OneToManyIndex.OneToManyIndexWriter;
import com.mastfrog.storage.Buffers;
import com.mastfrog.storage.Storage;
import com.mastfrog.storage.Storage.Spec;
import com.mastfrog.storage.StorageIterator;
import com.mastfrog.storage.ValueType;
import com.mastfrog.function.LongBiConsumer;
import com.mastfrog.function.LongBiPredicate;
import com.mastfrog.util.preconditions.Exceptions;
import com.mastfrog.util.search.Bias;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongPredicate;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A one-to-many, invertible index. The reader interface has some unavoidable
 * complexity, because both the indices into an Index and the values are stored.
 * Each row in the index consists of keyIndex:valueIndex:key:value where the
 * index items are ints and the values are longs. This is necessary in order to
 * build bit-set based graphs of the contents, which require int indices that
 * get *mapped* to sparse long ids.
 *
 * @author Tim Boudreau
 */
public class OneToManyIndex {

    private final Path dir;
    private final String name;
    private final Spec spec;

    public OneToManyIndex(Path dir, String name, Spec spec) {
        this.dir = dir;
        this.name = name;
        this.spec = spec.withSize(Long.BYTES * 3).concurrency(4);
    }

    public boolean exists() {
        return Files.exists(dir.resolve(name + ".12m"));
    }

    public long size() {
        Path p = dir.resolve(name);
        if (Files.exists(p)) {
            try {
                return Files.size(p) / (Long.BYTES * 2);
            } catch (IOException ex) {
                Logger.getLogger(OneToManyIndex.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return 0L;
    }

    public interface OneToManyIndexWriter extends AutoCloseable {

        void put(int keyIndex, int valueIndex, long key, long value);

        OneToManyIndexWriter buildInvertedIndex();

        void close() throws IOException;
    }

    public OneToManyIndexWriter writer() throws IOException {
        Path path = dir.resolve(name + ".12m");
        FileChannel ch = FileChannel.open(path, CREATE, READ, WRITE, TRUNCATE_EXISTING);
        return new OneToManyIndexWriterImpl(ch, spec, name, dir);
    }

    public OneToManyIndexReader reader() throws IOException {
        return new OneToManyIndexReaderImpl(dir, name, spec.withSize(Long.BYTES * 2));
    }

    private static final class OneToManyIndexWriterImpl implements OneToManyIndexWriter {

        private final FileChannel channel;
        private final Buffers buffers;
        private final Spec spec;
        private boolean buildInvertedIndex;
        private final String name;
        private final Path dir;
        private FileChannel invertedChannel;

        OneToManyIndexWriterImpl(FileChannel channel, Spec spec, String name, Path dir) {
            this.spec = spec.withSize(Long.BYTES * 3).readWrite();
            buffers = this.spec.buffers();
            this.channel = channel;
            this.name = name;
            this.dir = dir;
        }

        public synchronized OneToManyIndexWriter buildInvertedIndex() {
            try {
                this.buildInvertedIndex = true;
                if (channel.position() == 0L) {
                    System.out.println("Will build inverted index as we go");
                    Path inv = dir.resolve(name + ".m21");
                    System.out.println("Dynamically creating inverse index " + inv);
                    invertedChannel = FileChannel.open(inv, READ, WRITE, CREATE, TRUNCATE_EXISTING);
                }
                return this;
            } catch (IOException ex) {
                return Exceptions.chuck(ex);
            }
        }

        @Override
        public void put(int keyIndex, int valueIndex, long key, long value) {
            try {
                ByteBuffer buf = buffers.get(true);
                buf.putInt(keyIndex);
                buf.putInt(valueIndex);
                buf.putLong(key);
                buf.putLong(value);
                buf.flip();
                channel.write(buf);
                if (invertedChannel != null) {
                    buf = buffers.get(false);
                    buf.putInt(valueIndex);
                    buf.putInt(keyIndex);
                    buf.putLong(value);
                    buf.putLong(key);
                    buf.flip();
                    invertedChannel.write(buf);
                }
            } catch (IOException ex) {
                Exceptions.chuck(ex);
            }
        }

        @Override
        public void close() throws IOException {
            Storage stor = null;
            try {
                stor = Storage.create(channel, spec.alwaysMapped());
                /// Need a sort on 128-bit long, so we can sort keys *and* values
                System.out.println("Sorting " + name + " one-to-many of " + stor.size());
                long th = System.currentTimeMillis();
                stor.sort(Integer.BYTES * 2, ValueType.LONG128);
                long el = System.currentTimeMillis() - th;
                System.out.println("  sorting took " + el + " ms");

                Path countFile = dir.resolve(name + ".counts");
                System.out.println("Write countfile " + countFile);
                long then = System.currentTimeMillis();
                Spec spec = this.spec.withSize(Long.BYTES + Integer.BYTES * 2);
                try (FileChannel channel = FileChannel.open(countFile, CREATE,
                        READ, WRITE, TRUNCATE_EXISTING)) {
                    long last = Long.MAX_VALUE;
                    int count = 1;
                    int keyIndex = 0;
                    for (ByteBuffer buf : stor) {
                        keyIndex = buf.getInt();
                        int valueIndex = buf.getInt();
                        long key = buf.getLong();
                        if (key != last) {
                            if (last != Long.MAX_VALUE) {
                                ByteBuffer out = buffers.get(true);
                                out.putInt(keyIndex);
                                out.putLong(last);
                                out.putInt(count);
                                out.flip();
                                channel.write(out);
                            }
                            last = key;
                            count = 1;
                        } else {
                            count++;
                        }
                    }
                    if (last != Long.MAX_VALUE) {
                        ByteBuffer out = buffers.get(true);
                        out.putInt(keyIndex);
                        out.putLong(last);
                        out.putInt(count);
                        out.flip();
                        channel.write(out);
                    }
                }
                if (buildInvertedIndex) {
                    if (invertedChannel != null) {
                        System.out.println("Have existing inverse index channel - sorting it - size " + invertedChannel.size());
                        invertedChannel.force(true);
                        Storage stor2 = Storage.create(invertedChannel, this.spec.alwaysMapped());
                        long then2 = System.currentTimeMillis();
                        stor2.sort(Integer.BYTES * 2, ValueType.LONG128);
                        long elapsed2 = System.currentTimeMillis() - then2;
                        System.out.println(" Sort inv index of " + stor2.size() + " took " + elapsed2 + " ms");
                        invertedChannel.close();
                    } else {
                        channel.force(true);
                        System.out.println("Create inv on close");
                        createInverseIndex(dir, name, new OneToManyIndexReaderImpl(dir, name, spec), spec);
                    }
                }
                long elapsed = System.currentTimeMillis() - then;
                System.out.println("  wrote countfile in " + elapsed + " ms");

            } finally {
                try {
                    if (stor != null && stor instanceof AutoCloseable) {
                        try {
                            ((AutoCloseable) stor).close();
                        } catch (Exception ex) {
                            Exceptions.chuck(ex);
                        }
                    }
                } finally {
                    channel.close();
                }
            }
        }
    }

    public interface OneToManyIndexReader extends AutoCloseable {

        OneToManyIndexReader inverse() throws IOException;

        void close() throws IOException;

        default int closure(long key, LongPredicate pred) {
            class ClosureVisitor implements LongPredicate {

                Set<Long> seen = new TreeSet<>();
                int ct = 0;

                @Override
                public boolean test(long child) {
                    if (seen.contains(child)) {
                        return true;
                    }
                    if (pred.test(child)) {
                        seen.add(child);
                        return true;
                    }
                    values(child, this);
                    return false;
                }
            }
            ClosureVisitor clo = new ClosureVisitor();
            values(key, clo);
            return clo.ct;
        }

        Bits valueIndices(long key);

        Bits valueIndices(int keyIndex);

        int read(long key, LongBiPredicate pred);

        int read(long key, OneToManyIndexPredicate pred);

        int read(int keyIndex, OneToManyIndexPredicate pred);

        int read(long key, OneToManyIndexConsumer pred);

        int read(int keyIndex, OneToManyIndexConsumer pred);

        int values(long key, LongPredicate pred);

        int values(long key, OneToManyValuePredicate pred);

        int values(int keyIndex, OneToManyValuePredicate pred);

        int values(long key, OneToManyValueConsumer pred);

        int values(int keyIndex, OneToManyValueConsumer pred);

        void forEach(LongBiConsumer c);

        int forEach(LongBiPredicate pred);

        void forEach(OneToManyIndexConsumer c);

        int forEach(OneToManyIndexPredicate pred);

        boolean isEmpty();

        PrimitiveIterator.OfLong iterator();

        long max();

        long min();

        long nearestKey(long k, Bias bias);

        Set<Long> valueSet(long key);

        long size();

        public interface OneToManyValueConsumer {

            void accept(int valueIndex, long value);
        }

        public interface OneToManyValuePredicate {

            boolean test(int valueIndex, long value);
        }

        public interface OneToManyIndexConsumer {

            void accept(int keyIndex, int valueIndex, long key, long value);
        }

        public interface OneToManyIndexPredicate {

            boolean test(int keyIndex, int valueIndex, long key, long value);
        }
    }

    private static void createInverseIndex(Path dir, String name, OneToManyIndexReader index, Spec spec) throws IOException {
        Path inv = dir.resolve(name + ".m21");
        if (!Files.exists(inv)) {
            System.out.println("Dynamically creating inverse index " + inv);
            try (FileChannel out = FileChannel.open(inv, READ, WRITE, CREATE)) {
                OneToManyIndexWriterImpl writer
                        = new OneToManyIndexWriterImpl(out, spec, name, dir);
                ByteBuffer buf = ByteBuffer.allocateDirect(Long.BYTES * 3);
                index.forEach((keyIndex, valueIndex, key, val) -> {
                    buf.putInt(valueIndex);
                    buf.putInt(keyIndex);
                    buf.putLong(val);
                    buf.putLong(key);
                    buf.flip();
                    try {
                        out.write(buf);
                    } catch (IOException ex) {
                        Exceptions.chuck(ex);
                    }
                    buf.rewind();
                });
                Storage stor = Storage.create(out, spec.readWrite());
                try {
                    stor.sort(0, ValueType.INT);
                } finally {
                    if (stor instanceof AutoCloseable) {
                        try {
                            ((AutoCloseable) stor).close();
                        } catch (Exception ex) {
                            Exceptions.chuck(ex);
                        }
                    }
                }
            }
            System.out.println("  finished creating " + inv);
        }
    }

    private static final class OneToManyIndexReaderImpl implements OneToManyIndexReader {

        private final Storage ixStorage;
        private final Storage countStorage;
        private final Buffers buffers;
        private final long sz;
        private final FileChannel indexChannel;
        private final FileChannel countsChannel;
        private OneToManyIndexReaderImpl sibling;
        private boolean isInverse;
        private final String name;
        private final Spec spec;
        private final Path dir;
        private volatile boolean closed;

        public OneToManyIndexReaderImpl(Path dir, String name, Spec spec) throws IOException {
            this(dir, name, spec, false);
        }

        public OneToManyIndexReaderImpl(Path dir, String name, Spec spec, boolean isInverse) throws IOException {
            Path index = dir.resolve(name + (isInverse ? ".m21" : ".12m"));
            Path counts = dir.resolve(name + ".counts");
            spec = spec.withSize(Long.BYTES * 3);
            this.dir = dir;
            this.name = name;
            this.spec = spec;
            buffers = spec.buffers();
            indexChannel = FileChannel.open(index, READ);
            countsChannel = FileChannel.open(counts, READ);
            ixStorage = Storage.create(indexChannel, spec.readOnly());
            countStorage = Storage.create(countsChannel, spec.readOnly().withSize(Long.BYTES + Integer.BYTES));
            sz = ixStorage.size();
            if (sz == 0) {
                throw new IllegalStateException("Storage size is 0 for " + index
                        + " with a " + ixStorage.getClass().getSimpleName());
            }
            this.isInverse = isInverse;
        }

        public synchronized OneToManyIndexReader inverse() throws IOException {
            if (sibling != null) {
                return sibling;
            }
            Path inv = dir.resolve(name + (isInverse ? ".12m" : ".m21"));
            if (!Files.exists(inv)) {
                System.out.println("Dynamically creating inverse index " + inv);
                createInverseIndex(dir, name, this, spec);
            } else {
                System.out.println("INV already exists: " + inv);
            }
            OneToManyIndexReaderImpl reader = new OneToManyIndexReaderImpl(dir, name, spec, !isInverse);
            reader.sibling = this;
            return reader;
        }

        public int count(long key) {
            long offset = countStorage.binarySearch(key, OneToManySpec.KEY.byteOffset(), ValueType.LONG);
            if (offset < 0) {
                return -1;
            }
            return (int) countStorage.readValue(offset, Integer.BYTES + Long.BYTES, ValueType.INT);
        }

        public int count(int keyIndex) {
            long offset = countStorage.binarySearch(keyIndex, 0, ValueType.INT);
            if (offset < 0) {
                return -1;
            }
            return (int) countStorage.readValue(offset, Long.BYTES, ValueType.INT);
        }

        @Override
        public Bits valueIndices(long key) {
            MutableBits result = new IntSetBits();
            read(key, (kix, vix, k, v) -> {
                if (k != key) {
                    return false;
                }
                result.set(vix);
                return true;
            });
            if (result.isEmpty()) {
                return Bits.EMPTY;
            }
            return result;
        }

        @Override
        public Bits valueIndices(int keyIndex) {
            MutableBits result = new IntSetBits();
            read(keyIndex, (kix, vix, k, v) -> {
                if (kix != keyIndex) {
                    return false;
                }
                result.set(vix);
                return true;
            });
            if (result.isEmpty()) {
                return Bits.EMPTY;
            }
            return result;
        }

        public Set<Long> valueSet(long key) {
            Set<Long> result = new TreeSet<>();
            values(key, v -> {
                result.add(v);
                return true;
            });
            return result;
        }

        public long size() {
            return sz;
        }

        public boolean isEmpty() {
            return ixStorage.size() < Long.BYTES * 2;
        }

        public void forEach(LongBiConsumer c) {
            for (ByteBuffer buf : ixStorage) {
                buf.position(Integer.BYTES * 2);
                c.accept(buf.getLong(), buf.getLong());
            }
        }

        public int forEach(LongBiPredicate pred) {
            int ct = 0;
            for (ByteBuffer buf : ixStorage) {
                buf.position(Integer.BYTES * 2);
                ct++;
                if (!pred.test(buf.getLong(), buf.getLong())) {
                    break;
                }
            }
            return ct;
        }

        @Override
        public void forEach(OneToManyIndexConsumer c) {
            for (ByteBuffer buf : ixStorage) {
                c.accept(buf.getInt(), buf.getInt(), buf.getLong(), buf.getLong());
            }
        }

        @Override
        public int forEach(OneToManyIndexPredicate pred) {
            int ct = 0;
            for (ByteBuffer buf : ixStorage) {
                ct++;
                if (!pred.test(buf.getInt(), buf.getInt(), buf.getLong(), buf.getLong())) {
                    break;
                }
            }
            return ct;
        }

        public PrimitiveIterator.OfLong iterator() {
            return new StorageIterator(ixStorage, 0).adapted(0);
        }

        public long min() {
            return isEmpty() ? Long.MAX_VALUE
                    : ixStorage.readValue(0, 0, ValueType.LONG);
        }

        public long max() {
            return isEmpty() ? Long.MIN_VALUE
                    : ixStorage.readValue(sz - 1, 0, ValueType.LONG);
        }

        public boolean nearest(long k, Bias bias, LongBiConsumer c) {
            long offset = ixStorage.binarySearch(k, Integer.BYTES * 2, ValueType.LONG, bias);
            if (offset >= 0) {
                ByteBuffer buf = ixStorage.forIndex(offset);
                c.accept(buf.getLong(), buf.getLong());
                return true;
            }
            return false;
        }

        public long nearestKey(long k, Bias bias) {
            long offset = ixStorage.binarySearch(k, Integer.BYTES * 2, ValueType.LONG, bias);
            if (offset >= 0) {
                return ixStorage.readValue(offset, Integer.BYTES * 2, ValueType.LONG);
            }
            return -1;
        }

        @Override
        public int read(long key, LongBiPredicate pred) {
            int result = 0;
            long start = ixStorage.binarySearch(key, Integer.BYTES + Integer.BYTES, ValueType.LONG, Bias.NEAREST);
            if (start < 0) {
                return 0;
            }
            ByteBuffer buf = ixStorage.forIndex(start);
            buf.position(Integer.BYTES * 2);
            long firstKey = buf.getLong();
            for (; start < sz;) {
                long val = buf.getLong();
                result++;
                if (!pred.test(firstKey, val)) {
                    break;
                }
                if (++start >= sz) {
                    break;
                }
                buf = ixStorage.forIndex(start);
                firstKey = buf.getLong();
            }
            return result;
        }

        @Override
        public int read(long key, OneToManyIndexPredicate pred) {
            int result = 0;
            long start = ixStorage.binarySearch(key, Integer.BYTES + Integer.BYTES, ValueType.LONG, Bias.BACKWARD);
            if (start < 0) {
                return 0;
            }
            ByteBuffer buf = ixStorage.forIndex(start);
            int firstKeyIndex = buf.getInt();
            int firstValueIndex = buf.getInt();
            long firstKey = buf.getLong();
            if (firstKey < key) {
                return 0;
            }
            long firstValue = buf.getLong();
            for (; start < sz;) {
                result++;
                if (!pred.test(firstKeyIndex, firstValueIndex, firstKey, firstValue)) {
                    break;
                }
                if (++start >= sz) {
                    break;
                }
                buf = ixStorage.forIndex(start);
                firstKeyIndex = buf.getInt();
                firstValueIndex = buf.getInt();
                firstKey = buf.getLong();
                firstValue = buf.getLong();
            }
            return result;
        }

        @Override
        public int read(int keyIndex, OneToManyIndexPredicate pred) {
            if (keyIndex < 0) {
                return 0;
            }
            int result = 0;
            long start = ixStorage.binarySearch(keyIndex, 0, ValueType.INT, Bias.BACKWARD);
            if (start < 0) {
                return 0;
            }
            ByteBuffer buf = ixStorage.forIndex(start);
            int firstKeyIndex = buf.getInt();
            if (firstKeyIndex < keyIndex || firstKeyIndex < 0) {
                return 0;
            }
            int firstValueIndex = buf.getInt();
            long firstKey = buf.getLong();
            long firstValue = buf.getLong();
            if (firstValueIndex < 0) {
                System.out.println("FVI in " + this.name + " inv " + this.isInverse);
                System.out.println("  Data " + firstKeyIndex + ":" + firstValueIndex
                        + " " + firstKey + " / " + firstValue + " searching for " + keyIndex);
            }
            for (; start < sz;) {
                result++;
                if (!pred.test(firstKeyIndex, firstValueIndex, firstKey, firstValue)) {
                    break;
                }
                if (++start >= sz) {
                    break;
                }
                buf = ixStorage.forIndex(start);
                firstKeyIndex = buf.getInt();
                firstValueIndex = buf.getInt();
                firstKey = buf.getLong();
                firstValue = buf.getLong();
            }
            return result;
        }

        @Override
        public int read(long key, OneToManyIndexConsumer pred) {
            int result = 0;
            long start = ixStorage.binarySearch(key, Integer.BYTES + Integer.BYTES, ValueType.LONG, Bias.NEAREST);
            if (start < 0) {
                return 0;
            }
            ByteBuffer buf = ixStorage.forIndex(start);
            int firstKeyIndex = buf.getInt();
            int firstValueIndex = buf.getInt();
            long firstKey = buf.getLong();
            long firstValue = buf.getLong();
            for (; start < sz;) {
                result++;
                pred.accept(firstKeyIndex, firstValueIndex, firstKey, firstValue);
                if (++start >= sz) {
                    break;
                }
                buf = ixStorage.forIndex(start);
                firstKeyIndex = buf.getInt();
                firstValueIndex = buf.getInt();
                firstKey = buf.getLong();
                firstValue = buf.getLong();
            }
            return result;
        }

        @Override
        public int read(int keyIndex, OneToManyIndexConsumer pred) {
            int result = 0;
            long start = ixStorage.binarySearch(keyIndex, 0, ValueType.INT, Bias.NEAREST);
            if (start < 0) {
                return 0;
            }
            ByteBuffer buf = ixStorage.forIndex(start);
            int firstKeyIndex = buf.getInt();
            int firstValueIndex = buf.getInt();
            long firstKey = buf.getLong();
            long firstValue = buf.getLong();
            for (; start < sz;) {
                result++;
                pred.accept(firstKeyIndex, firstValueIndex, firstKey, firstValue);
                if (++start >= sz) {
                    break;
                }
                buf = ixStorage.forIndex(start);
                firstKeyIndex = buf.getInt();
                if (firstKeyIndex != keyIndex) {
                    break;
                }
                firstValueIndex = buf.getInt();
                firstKey = buf.getLong();
                firstValue = buf.getLong();
            }
            return result;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                if (ixStorage instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) ixStorage).close();
                    } catch (Exception ex) {
                        Exceptions.chuck(ex);
                    }
                }
                OneToManyIndexReader other;
                synchronized (this) {
                    other = sibling;
                }
                if (other != null) {
                    other.close();
                }
            }
        }

        @Override
        public int values(long key, LongPredicate pred) {
            int result = 0;
            long start = ixStorage.binarySearch(key, Integer.BYTES * 2, ValueType.LONG, Bias.BACKWARD);
            if (start < 0) {
                return 0;
            }
            ByteBuffer buf = ixStorage.forIndex(start);
            buf.position(Integer.BYTES * 2);
            long firstKey = buf.getLong();
            if (firstKey != key) {
                return 0;
            }
            System.out.println("vals from " + key + " for " + firstKey + " at " + start);
            for (; start < sz && key == firstKey;) {
                long val = buf.getLong();
                result++;
                if (!pred.test(val)) {
                    break;
                }
                if (++start >= sz) {
                    break;
                }
                buf = ixStorage.forIndex(start);
                buf.position(Integer.BYTES * 2);
                firstKey = buf.getLong();
            }
            System.out.println("  done on " + firstKey);
            return result;
        }

        @Override
        public int values(long key, OneToManyValuePredicate pred) {
            int result = 0;
            long start = ixStorage.binarySearch(key, Integer.BYTES * 2, ValueType.LONG, Bias.BACKWARD);
            if (start < 0) {
                return 0;
            }
            ByteBuffer buf = ixStorage.forIndex(start);
            int firstKeyIndex = buf.getInt();
            int firstValueIndex = buf.getInt();
            long firstKey = buf.getLong();
            long firstValue = buf.getLong();
            if (firstKey != key) {
                return 0;
            }
            for (; start < sz && key == firstKey;) {
                result++;
                if (!pred.test(firstValueIndex, firstValue) || ++start >= sz) {
                    break;
                }
                buf = ixStorage.forIndex(start);
                firstKeyIndex = buf.getInt();
                firstValueIndex = buf.getInt();
                firstKey = buf.getLong();
                firstValue = buf.getLong();
            }
            return result;
        }

        @Override
        public int values(int keyIndex, OneToManyValueConsumer pred) {
            int result = 0;
            long start = ixStorage.binarySearch(keyIndex, 0, ValueType.INT, Bias.BACKWARD);
            if (start < 0) {
                return 0;
            }
            ByteBuffer buf = ixStorage.forIndex(start);
            int firstKeyIndex = buf.getInt();
            int firstValueIndex = buf.getInt();
            long firstKey = buf.getLong();
            long firstValue = buf.getLong();
            if (firstKeyIndex != keyIndex) {
                return 0;
            }
            for (; start < sz && firstKeyIndex == keyIndex;) {
                result++;
                pred.accept(firstValueIndex, firstValue);
                if (++start >= sz) {
                    break;
                }
                buf = ixStorage.forIndex(start);
                firstKeyIndex = buf.getInt();
                firstValueIndex = buf.getInt();
                firstKey = buf.getLong();
                firstValue = buf.getLong();
            }
            return result;
        }

        @Override
        public int values(int keyIndex, OneToManyValuePredicate pred) {
            int result = 0;
            long start = ixStorage.binarySearch(keyIndex, 0, ValueType.INT, Bias.BACKWARD);
            if (start < 0) {
                return 0;
            }
            ByteBuffer buf = ixStorage.forIndex(start);
            int firstKeyIndex = buf.getInt();
            int firstValueIndex = buf.getInt();
            long firstKey = buf.getLong();
            long firstValue = buf.getLong();
            if (firstKeyIndex != keyIndex) {
                return 0;
            }
            for (; start < sz && keyIndex == firstKeyIndex;) {
                result++;
                if (!pred.test(firstValueIndex, firstValue)) {
                    break;
                }
                if (++start >= sz) {
                    break;
                }
                buf = ixStorage.forIndex(start);
                firstKeyIndex = buf.getInt();
                firstValueIndex = buf.getInt();
                firstKey = buf.getLong();
                firstValue = buf.getLong();
            }
            return result;
        }

        @Override
        public int values(long key, OneToManyValueConsumer pred) {
            int result = 0;
            long start = ixStorage.binarySearch(key, Integer.BYTES * 2, ValueType.LONG, Bias.BACKWARD);
            if (start < 0) {
                return 0;
            }
            ByteBuffer buf = ixStorage.forIndex(start);
            int firstKeyIndex = buf.getInt();
            int firstValueIndex = buf.getInt();
            long firstKey = buf.getLong();
            long firstValue = buf.getLong();
            if (firstKey != key) {
                return 0;
            }
            for (; start < sz && key == firstKey;) {
                result++;
                pred.accept(firstValueIndex, firstValue);
                if (++start >= sz) {
                    break;
                }
                buf = ixStorage.forIndex(start);
                firstKeyIndex = buf.getInt();
                firstValueIndex = buf.getInt();
                firstKey = buf.getLong();
                firstValue = buf.getLong();
            }
            return result;
        }

    }

    static enum OneToManySpec implements SchemaItem {
        KEY_OFFSET,
        VALUE_OFFSET,
        KEY,
        VALUE;

        @Override
        public ValueType type() {
            switch (this) {
                case KEY_OFFSET:
                case VALUE_OFFSET:
                    return ValueType.INT;
                default:
                    return ValueType.LONG;
            }
        }

        @Override
        public int byteOffset() {
            switch (this) {
                case KEY_OFFSET:
                    return 0;
                case VALUE_OFFSET:
                    return Integer.BYTES;
                case KEY:
                    return Integer.BYTES * 2;
                case VALUE:
                    return (Integer.BYTES * 2) + (Long.BYTES * 2);
                default:
                    throw new AssertionError(this);
            }
        }

        @Override
        public IndexKind indexKind() {
            if (this == KEY_OFFSET || this == KEY) {
                return IndexKind.CANONICAL_ORDERING;
            }
            return IndexKind.NONE;
        }
    }
}
