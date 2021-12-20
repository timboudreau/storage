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

import static com.mastfrog.storage.Storage.TWO_GB;
import com.mastfrog.function.throwing.ThrowingRunnable;
import com.mastfrog.util.collections.ArrayUtils;
import com.mastfrog.util.file.FileUtils;
import com.mastfrog.util.sort.Swapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests all of the implementations of storage.
 *
 * @author Tim Boudreau
 */
public class StorageTest {

    private Path path;

    @Test
    public void testFileChannelStorage() throws Exception {
        try (Data data = createData(false)) {
            try (FileChannel readWrite = FileChannel.open(data.file, READ, WRITE)) {
                FileChannelStorage stor = new FileChannelStorage(readWrite,
                        Rec.SIZE, Buffers.factory(true, 4, false));
                time(stor.getClass(), () -> testOneStorage(data, stor));
            }
        }
    }

    @Test
    public void testCachingFileChannelStorage() throws Exception {
        try (Data data = createData(false)) {
            try (FileChannel readWrite = FileChannel.open(data.file, READ, WRITE)) {
                CachingFileChannelStorage stor = new CachingFileChannelStorage(readWrite,
                        Rec.SIZE, 96, true);
                time(stor.getClass(), () -> testOneStorage(data, stor));
            }
        }
    }

    @Test
    public void testMappedStorage() throws Exception {
        try (Data data = createData(false)) {
            try (FileChannel readWrite = FileChannel.open(data.file, READ, WRITE)) {
                MappedByteBuffer mapping = readWrite.map(MapMode.READ_WRITE, 0, readWrite.size());
                Storage stor = new SingleMappedStorage(Rec.SIZE, mapping, true,
                        Buffers.factory(true, 4, false));
                time(stor.getClass(), () -> testOneStorage(data, stor));
            }
        }
    }

    @Test
    public void testMultiMappedStorage() throws Exception {
        try (Data data = createData(false)) {
            try (FileChannel readWrite = FileChannel.open(data.file, READ, WRITE)) {
                Storage stor = new MultiMappedStorage(Rec.SIZE, readWrite,
                        MapMode.READ_WRITE, true, 60, Buffers.factory(true, 4, false));
                time(stor.getClass(), () -> testOneStorage(data, stor));
            }
        }
    }

    @Test
    public void testAdaptiveStorage() throws Exception {
        try (Data data = createData(false)) {
            try (FileChannel readWrite = FileChannel.open(data.file, READ, WRITE)) {
                Storage stor = new AdaptiveStorage(Rec.SIZE, readWrite, true, false, 12, true, Buffers.factory(true, 4, false), true);
                time(stor.getClass(), () -> testOneStorage(data, stor));
            }
        }
    }

    private void time(Class<?> what, ThrowingRunnable run) throws Exception {
        long then = System.currentTimeMillis();
        try {
            run.run();
        } finally {
            long elapsed = System.currentTimeMillis() - then;
            System.out.println(what.getSimpleName() + " " + elapsed + " ms");
        }
    }

    private void testOneStorage(Data data, Storage storage) throws Exception {
        String pfx = storage.getClass().getSimpleName() + ": ";
        for (int index = 0; index < data.size(); index++) {
            Rec expect = data.get(index);
            ByteBuffer buf = storage.read(index);
            Rec got = Rec.from(buf);
            assertEquals(expect, got, pfx + "Wrong record for " + index);
        }
        Random rnd = new Random(23);
        // Random access
        for (int index : shuffled(rnd, 0, data.size())) {
            Rec expect = data.get(index);
            ByteBuffer buf = storage.read(index);
            Rec got = Rec.from(buf);
            assertEquals(expect, got, pfx + "Wrong record for " + index);
        }

        Rec nue = new Rec(130, 23, 42);
        storage.write(130, nue.toByteBuffer());
        data.records[130] = nue;

        for (int index = 0; index < data.size(); index++) {
            Rec expect = data.get(index);
            ByteBuffer buf = storage.read(index);
            Rec got = Rec.from(buf);
            assertEquals(expect, got, pfx + "Wrong record for " + index);
        }
        assertEquals((long) data.size(), storage.size(), pfx + " Wrong size");

        data.swap(100, 1000);
        storage.swap(100, 1000);

        data.swap(52, 54);
        storage.swap(52, 54);

        for (int index = 0; index < data.size(); index++) {
            Rec expect = data.get(index);
            ByteBuffer buf = storage.read(index);
            Rec got = Rec.from(buf);
            assertEquals(expect, got, pfx + "Wrong record for " + index);
        }

        data.bulkSwap(200, 500, 100);
        storage.bulkSwap(200, 500, 100);

        for (int index = 0; index < data.size(); index++) {
            Rec expect = data.get(index);
            ByteBuffer buf = storage.read(index);
            Rec got = Rec.from(buf);
            assertEquals(expect, got, pfx + "Wrong record for " + index);
        }

        data.bulkSwap(1, 15, 8);
        storage.bulkSwap(1, 15, 8);

        for (int index = 0; index < data.size(); index++) {
            Rec expect = data.get(index);
            ByteBuffer buf = storage.read(index);
            Rec got = Rec.from(buf);
            assertEquals(expect, got, pfx + "Wrong record for " + index);

            long d1 = expect.data1;
            long d2 = storage.readValue(index, Integer.BYTES, ValueType.LONG);
            assertEquals(d1, d2, "ReadValue on " + index + " did not return the right value");
        }

        data.sortByVal1();
        storage.sort(Integer.BYTES, ValueType.LONG);

        for (int index = 0; index < data.size(); index++) {
            Rec expect = data.get(index);
            ByteBuffer buf = storage.read(index);
            Rec got = Rec.from(buf);
            assertEquals(expect, got, pfx + "Wrong record for " + index);

            long d1 = expect.data1;
            long d2 = storage.readValue(index, Integer.BYTES, ValueType.LONG);
            assertEquals(d1, d2, "ReadValue on " + index + " did not return the right value");

            long ix = storage.binarySearch(expect.data1, Integer.BYTES, ValueType.LONG);
            assertEquals((long) index, ix, "Binary search did not find record " + index);
        }

        for (int index = 0; index < data.size(); index++) {
            Rec expect = data.get(index);
            expect.data2 = 10 * (data.size() - index);

            storage.writeLong(index, expect.data2, Integer.BYTES + Long.BYTES);

            ByteBuffer buf = storage.read(index);
            Rec got = Rec.from(buf);
            assertEquals(expect, got, pfx + "Wrong record for " + index);
        }
    }

    public Data createData(boolean veryLarge) throws IOException {
        Random rnd = new Random(121939420L);
        int countInTwoGb = (int) (TWO_GB / Rec.SIZE);
        int amt = veryLarge ? (countInTwoGb + 1024) : 8192;

        Path path = FileUtils.newTempFile("StorageTest-");
        long[] unique = distinctLongs(rnd, amt);
        try (FileChannel ch = FileChannel.open(path, CREATE, APPEND)) {
            Rec[] result = new Rec[amt];
            ByteBuffer buf = ByteBuffer.allocateDirect(Rec.SIZE);
            for (int i = 0; i < amt; i++) {
                Rec r = new Rec(i, unique[i], rnd.nextLong());
                result[i] = r;
                buf.rewind();
                buf.limit(Rec.SIZE);
                r.write(buf);
                buf.flip();
                ch.write(buf);
            }
            return new Data(path, result);
        }
    }

    private long[] distinctLongs(Random rnd, int count) {
        long[] result = new long[count];
        Set<Long> set = new HashSet<>();
        for (int i = 0; i < count; i++) {
            long l = Math.abs(rnd.nextLong());
            if (set.add(l)) {
                result[i] = l;
            } else {
                i--;
            }
        }
        return result;
    }

    static class Data implements AutoCloseable, Swapper {

        final Path file;
        final Rec[] records;

        public Data(Path file, Rec[] records) {
            this.file = file;
            this.records = records;
        }

        void sortByVal1() {
            Arrays.sort(records, (a, b) -> {
                return Long.compare(a.data1, b.data1);
            });
        }

        int size() {
            return records.length;
        }

        Rec get(int i) {
            return records[i];
        }

        List<Rec> copy() {
            return new ArrayList<>(Arrays.asList(records));
        }

        void delete() throws IOException {
            Files.deleteIfExists(file);
        }

        @Override
        public void close() throws Exception {
            delete();
        }

        @Override
        public void swap(int index1, int index2) {
            Rec a = records[index1];
            records[index1] = records[index2];
            records[index2] = a;
        }
    }

    private int[] shuffled(Random rnd, int start, int end) {
        int[] result = new int[end - start];
        for (int i = 0; i < result.length; i++) {
            result[i] = start + i;
        }
        ArrayUtils.shuffle(rnd, result);
        return result;
    }

    @AfterEach
    public void deleteData() {

    }

    static class Rec implements Comparable<Rec> {

        public int index;
        public long data1;
        public long data2;

        static int SIZE = Integer.BYTES + Long.BYTES * 2;

        public Rec(int index, long data1, long data2) {
            this.index = index;
            this.data1 = data1;
            this.data2 = data2;
        }

        public ByteBuffer toByteBuffer() {
            ByteBuffer result = ByteBuffer.allocate(SIZE);
            write(result);
            return result.flip();
        }

        public void write(ByteBuffer buf) {
            buf.putInt(index);
            buf.putLong(data1);
            buf.putLong(data2);
        }

        public static Rec from(ByteBuffer buf) {
            return new Rec(buf.getInt(), buf.getLong(), buf.getLong());
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 29 * hash + this.index;
            hash = 29 * hash + (int) (this.data1 ^ (this.data1 >>> 32));
            hash = 29 * hash + (int) (this.data2 ^ (this.data2 >>> 32));
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Rec other = (Rec) obj;
            if (this.index != other.index) {
                return false;
            }
            if (this.data1 != other.data1) {
                return false;
            }
            return this.data2 == other.data2;
        }

        @Override
        public String toString() {
            return index + ":" + data1 + ":" + data2;
        }

        @Override
        public int compareTo(Rec o) {
            return Integer.compare(index, o.index);
        }
    }
}
