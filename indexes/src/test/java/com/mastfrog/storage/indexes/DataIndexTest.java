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
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR DATA
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR DATA DEALINGS IN
 * THE SOFTWARE.
 */
package com.mastfrog.storage.indexes;

import com.mastfrog.bits.MutableBits;
import com.mastfrog.bits.collections.IntMatrixMap;
import com.mastfrog.bits.large.LongArray;
import com.mastfrog.storage.ValueType;
import com.mastfrog.util.collections.ArrayUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeAll;

/**
 *
 * @author Tim Boudreau
 */
public class DataIndexTest {

    private static Path DIR;

    @Test
    public void testSomeMethod() throws Exception {
        int count = 1000;
        Random rnd = new Random(12090L);
        List<Item> items = new ArrayList<>(count);
        Index<Schema1> one = new Index<>(DIR, "one", Schema1.class);

        int[] ids = shuffled(rnd, count);
        try (IndexWriter writer = one.writer()) {
            System.out.println("WRITER IS " + writer);
            for (int i = 0; i < count; i++) {
                int offset = rnd.nextInt(22) + i * 23;
                Item item = new Item(i, i * 23, ids[i], rnd.nextLong());
                items.add(item);
                writer.write(item.fileOffset, item.id, item.data);
            }
        }
        Path p = DIR.resolve("idmap");
        Map<Long, Long> stuff = new TreeMap<>();
        try (IndexReader<Schema1> reader = one.reader()) {

            SavableLongMatrixMap map = reader.map(Schema1.ID);

            System.out.println("Map size " + map.size());

            for (int i = 0; i < count; i++) {
                ByteBuffer buf = reader.get(i);
                Item item = Item.from(buf);
                Item exp = items.get(i);
                assertEquals(exp, item);

                long ixo = reader.indexOf(Schema1.ID, exp.id);
                assertEquals((long) i, ixo);

                long ix2 = reader.indexOf(Schema1.FILE_OFFSET, exp.fileOffset);
                assertEquals((long) i, ix2);

                if (i > 20) {
                    long m2o = ids[i - 20];
                    map.put(item.id, m2o);
                    long l = map.getOrDefault(item.id, -1);
//                    assertEquals(m2o, l, "Put " + m2o + " for " + item.id + " at " + i + " but did not get it back");
                    if (l != -1) {
//                        System.out.println("OK " + " " + m2o + " for " + item.id + " at " + i );
                    } else {
                        System.out.println("BAD " + " " + m2o + " for " + item.id + " at " + i);
                    }
                    stuff.put(item.id, m2o);
                }
            }
            map.save(p);

            LongArray arr = LongArray.mappedFileLongArray(p);
            MutableBits bits = arr.toBitSet().toBits();

            IntMatrixMap imm = IntMatrixMap.create(bits, (int) reader.size());
            IntMatrixMap.LongMatrixMap lmap = imm.asLongMap(
                    new IndexReaderMapAdapter<>(reader, Schema1.ID));

            System.out.println("Loaded " + imm);

            System.out.println("\nMap to " + lmap);

            for (int i = 20; i < count; i++) {
                Item it = items.get(i);
                long val = lmap.getOrDefault(it.id, -1);
                long exp = ids[i - 20];
                if (val != exp) {
                    System.out.println("BADRE " + i + " " + it.id + " exp " + exp + " got " + val);
                }
            }

        }
    }

    int[] shuffled(Random rnd, int size) {
        int[] result = new int[size];
        for (int i = 0; i < size; i++) {
            result[i] = i * 10;
        }
        ArrayUtils.shuffle(rnd, result);
        return result;
    }

    enum Schema1 implements SchemaItem {
        FILE_OFFSET,
        ID,
        DATA;

        @Override
        public ValueType type() {
            return ValueType.LONG;
        }

        @Override
        public IndexKind indexKind() {
            switch (this) {
                case DATA:
                    return IndexKind.NONE;
                case FILE_OFFSET:
                    return IndexKind.CANONICAL_ORDERING;
                case ID:
                    return IndexKind.UNIQUE;
                default:
                    throw new AssertionError(this);
            }
        }
    }

    static class Item {

        long fileOffset;
        long id;
        long data;

        public Item(int index, long fileOffset, long id, long data) {
            this.fileOffset = fileOffset;
            this.id = id;
            this.data = data;
        }

        public static Item from(ByteBuffer buf) {
            return new Item(buf.getInt(), buf.getLong(), buf.getLong(), buf.getLong());
        }

        @Override
        public String toString() {
            return "Item{" + "fileOffset=" + fileOffset + ", id=" + id + ", data=" + data + '}';
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 23 * hash + (int) (this.fileOffset ^ (this.fileOffset >>> 32));
            hash = 23 * hash + (int) (this.id ^ (this.id >>> 32));
            hash = 23 * hash + (int) (this.data ^ (this.data >>> 32));
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
            final Item other = (Item) obj;
            if (this.fileOffset != other.fileOffset) {
                return false;
            }
            if (this.id != other.id) {
                return false;
            }
            return this.data == other.data;
        }

    }

    @BeforeAll
    public static void temp() throws IOException {
        Path tmp = Paths.get(System.getProperty("java.io.tmpdir"));
        String proposal = "IndexTest";
        int ix = 1;
        while (Files.exists(tmp.resolve(proposal))) {
            proposal = "IndexTest-" + ix++;
        }
        DIR = tmp.resolve(proposal);
        Files.createDirectories(DIR);
    }

    @AfterAll
    public static void cleanup() throws IOException {
        if (DIR != null) {
            try (Stream<Path> str = Files.list(DIR)) {
                str.forEach(pth -> {
                    try {
                        Files.delete(pth);
                    } catch (IOException ex) {
                        Logger.getLogger(DataIndexTest.class.getName()).log(Level.SEVERE, null, ex);
                    }
                });
            }
            Files.deleteIfExists(DIR);
        }
    }
}
