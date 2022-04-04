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
import com.mastfrog.storage.indexes.OneToManyIndex.OneToManyIndexReader.OneToManyValueConsumer;
import com.mastfrog.storage.indexes.OneToManyIndex.OneToManyIndexReader.OneToManyValuePredicate;
import com.mastfrog.storage.Storage.StorageSpecification;
import com.mastfrog.storage.ValueType;
import com.mastfrog.util.collections.CollectionUtils;
import com.mastfrog.util.search.Bias;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.LongPredicate;
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
public class OneToManyIndexTest {

    private static Path DIR;

    @Test
    public void testBasicSearch() throws Exception {
        OneToManyIndex ix = new OneToManyIndex(DIR, "basic",
                StorageSpecification.defaultSpec().alwaysMapped().readWrite().concurrency(4));
        Map<Long, List<Long>> items = CollectionUtils.supplierMap(() -> new ArrayList<>());
        int index = 0;
        try (OneToManyIndex.OneToManyIndexWriter w = ix.writer()) {
            for (long i = 1000; i < 1012; i += 2) {
                for (long j = 0; j < 23 * (1 + (i % 23)); j += 23) {
                    System.out.println("P " + i + " : " + (j + i * 100));
                    long val = j + i * 100;
                    w.put(++index, index, i, val);
                    items.get(i).add(val);
                }
            }
        }
        System.out.println("-----------------");
        Map<Long, List<Long>> values = CollectionUtils.supplierMap(() -> new ArrayList<>());

        List<Map.Entry<Long, List<Long>>> entries = new ArrayList<>(items.entrySet());
        Collections.sort(entries, (a, b) -> {
            return a.getKey().compareTo(b.getKey());
        });

        try (OneToManyIndex.OneToManyIndexReader r = ix.reader()) {
            r.forEach((k, v) -> {
                System.out.println("G " + k + " : " + v);
                values.get(k).add(v);
            });
            assertEquals(items, values);
            int ct = 0;

            for (Map.Entry<Long, List<Long>> e : entries) {
                long key = e.getKey();
                long noneK = r.nearestKey(key, Bias.NONE);
                long fwdK = r.nearestKey(key, Bias.FORWARD);
                long bwdK = r.nearestKey(key, Bias.BACKWARD);
                long nearK = r.nearestKey(key, Bias.NEAREST);
                System.out.println(ct + ". N/F/B/NR for " + key + ": " + noneK + "/" + fwdK + "/" + bwdK + "/" + nearK);
                assertEquals(key, noneK, "wrong nearest NONE for " + key);
                assertEquals(key, fwdK, "wrong nearest FORWARD for " + key);
                assertEquals(key, bwdK, "wrong nearest BACKWARD for " + key);
                assertEquals(key, nearK, "wrong nearest NEAREST for " + key);

                long mfk = r.nearestKey(key - 1, Bias.FORWARD);
                assertEquals(key, mfk);
                assertEquals(-1L, r.nearestKey(key - 1, Bias.NONE));

                List<Long> nuew = new ArrayList<>();
                r.values(key, v -> {

                    nuew.add(v);
                    return true;
                });
                assertEquals(e.getValue(), nuew, ct + "/" + items.size()
                        + ". wrong read value for "
                        + key + " / " + items.size());

                System.out.println(ct + ". OK " + key + " = " + nuew);

                ct++;
            }
        }
    }

    private Map<Long, Set<Long>> inverse(Map<Long, Set<Long>> map) {
        Map<Long, Set<Long>> result = new TreeMap<>();
        for (Map.Entry<Long, Set<Long>> e : map.entrySet()) {
            for (Long ch : e.getValue()) {
                Set<Long> tgt = result.get(ch);
                if (tgt == null) {
                    tgt = new TreeSet<>();
                    result.put(ch, tgt);
                }
                tgt.add(e.getKey());
            }
        }
        return result;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIndex() throws IOException {
        Random rnd = new Random(239032L);
        Set<Long> itemsSorted = randomLongs(100, rnd);
        Map<Long, Set<Long>> kids = randomChildren(itemsSorted, rnd);
        Long[] all = itemsSorted.toArray(new Long[0]);
        Index<Ixs> index = new Index(DIR, Ixs.class);
        Map<Integer, Set<Integer>> childIndices = new TreeMap();
        List<Long> list = Arrays.asList(all);
        try (IndexWriter w = index.writer()) {
            for (int i = 0; i < all.length; i++) {
                w.write(i * 10, all[i]);
                if (!childIndices.containsKey(i)) {
                    childIndices.put(i, new TreeSet());
                }
                Set<Long> kds = kids.get(all[i]);
                if (kds == null) {
                    kids.put(all[i], kds = new TreeSet<>());
                }
                for (Long kid : kds) {

                    childIndices.get(i).add(list.indexOf(kid));
                }
            }
        }
        OneToManyIndex o2m = new OneToManyIndex(DIR, "ixs-o2m", StorageSpecification.defaultSpec());
        try (OneToManyIndex.OneToManyIndexWriter w = o2m.writer()) {
            for (int i = 0; i < all.length; i++) {
                Set<Long> children = kids.get(all[i]);
                for (Long kid : children) {
                    int cix = list.indexOf(kid);
                    w.put(i, cix, all[i], kid);
                    System.out.println(i + ":" + cix + "  " + all[i] + ":" + kid);
                }
            }
        }
        try (OneToManyIndex.OneToManyIndexReader r = o2m.reader()) {
            for (int i = 0; i < all.length; i++) {
                Set<Long> children = new TreeSet<>();
                Set<Integer> iixen = new TreeSet<>();
                int ix = i;

                r.read(ix, (pix, valueIndex, k, value) -> {
                    System.out.println("READ " + pix + ":" + valueIndex + "  " + k + ":" + value);
                    if (pix != ix) {
                        return false;
                    }
                    iixen.add(valueIndex);
                    children.add(value);
                    return true;
                });

                Bits exp = Bits.fromBitSet(toBitSet(iixen));
                Set<Long> realChildren = kids.get(all[i]);
                Set<Integer> realChildIndices = childIndices.get(i);
                assertEquals(realChildren, children, "Wrong children for " + all[i] + " at " + i);
                assertEquals(realChildIndices, iixen, "Wrong child indices for " + all[i] + " at " + i);
                children.clear();
                iixen.clear();

                r.read(all[ix], (pix, valueIndex, k, value) -> {
                    if (pix != ix) {
                        return false;
                    }
                    iixen.add(valueIndex);
                    children.add(value);
                    return true;
                });

                assertEquals(realChildren, children, "Wrong children for " + all[i] + " at " + i);
                assertEquals(realChildIndices, iixen, "Wrong child indices for " + all[i] + " at " + i);
                children.clear();
                iixen.clear();
                System.out.println("-------------");
                r.values(i, (OneToManyValuePredicate) (int valueIndex, long value) -> {
                    iixen.add(valueIndex);
                    children.add(value);
                    return true;
                });

                assertEquals(realChildren, children, "Wrong children for " + all[i] + " at " + i);
                assertEquals(realChildIndices, iixen, "Wrong child indices for " + all[i] + " at " + i);
                children.clear();
                iixen.clear();

                r.values(all[ix], (OneToManyValuePredicate) (int valueIndex, long value) -> {
                    System.out.println(ix + ". READ " + +valueIndex + "  " + value);
                    iixen.add(valueIndex);
                    children.add(value);
                    return true;
                });
                assertEquals(realChildren, children, "Wrong children for " + all[i] + " at " + i);
                assertEquals(realChildIndices, iixen, "Wrong child indices for " + all[i] + " at " + i);
                children.clear();
                iixen.clear();

                r.values(all[ix], (OneToManyValueConsumer) (int valueIndex, long value) -> {
                    System.out.println(ix + ". READ " + +valueIndex + "  " + value);
                    iixen.add(valueIndex);
                    children.add(value);
                });
                assertEquals(realChildren, children, "Wrong children for " + all[i] + " at " + i);
                assertEquals(realChildIndices, iixen, "Wrong child indices for " + all[i] + " at " + i);
                children.clear();
                iixen.clear();
                r.values(all[ix], (LongPredicate) (long value) -> {
                    children.add(value);
                    return true;
                });
                assertEquals(realChildren, children, "Wrong children for " + all[i] + " at " + i);
                children.clear();

                Bits vals = r.valueIndices(i);
                assertEquals(exp, vals, "Wrong bitset for " + all[i] + " at " + i);
            }
            Map<Long, Set<Long>> inv = inverse(kids);
            OneToManyIndex.OneToManyIndexReader iix = r.inverse();

            System.out.println("INV\n");
            iix.forEach((ki, vi, k, v) -> {
                System.out.println(" * " + ki + ":" + vi + "  " + k + ":" + v);
            });

            System.out.println("END INV\n");

            for (Map.Entry<Long, Set<Long>> e : inv.entrySet()) {
                Set<Long> found = new TreeSet<>();
                iix.values(e.getKey(), val -> {
                    found.add(val);
                    return true;
                });
                assertEquals(e.getValue(), found, "Wrong value for " + e.getKey());
            }
        }
    }

    private static BitSet toBitSet(Set<Integer> ins) {
        BitSet nue = new BitSet();
        for (Integer i : ins) {
            nue.set(i);
        }
        return nue;
    }

    private Set<Long> randomLongs(int sz, Random rnd) {
        Set<Long> result = new TreeSet<>();
        while (result.size() < sz) {
            result.add(Math.abs(rnd.nextLong() % (sz * 2)));
        }
        return result;
    }

    private Map<Long, Set<Long>> randomChildren(Set<Long> ls, Random rnd) {
        Map<Long, Set<Long>> result = new HashMap<>();
        Long[] all = ls.toArray(new Long[0]);
        for (Long ll : ls) {
            int count = rnd.nextInt(ls.size() / 6);
            Set<Long> kids = new TreeSet<>();
            for (int i = 0; i < count; i++) {
                int ix = rnd.nextInt(all.length);
                if (ix != i) {
                    kids.add(all[ix]);
                }
            }
            result.put(ll, kids);
        }
        return result;
    }

    enum Ixs implements SchemaItem {
        FILE_OFFSET,
        VALUE;

        @Override
        public ValueType type() {
            return ValueType.LONG;
        }

        @Override
        public IndexKind indexKind() {
            return ordinal() == 0 ? IndexKind.CANONICAL_ORDERING : IndexKind.UNIQUE;
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
