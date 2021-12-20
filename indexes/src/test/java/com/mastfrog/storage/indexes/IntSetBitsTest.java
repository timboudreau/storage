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

import com.mastfrog.abstractions.list.IndexedResolvable;
import com.mastfrog.bits.MutableBits;
import com.mastfrog.bits.collections.BitSetSet;
import com.mastfrog.util.collections.IntList;
import com.mastfrog.util.collections.IntSet;
import com.mastfrog.util.search.Bias;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author Tim Boudreau
 */
public class IntSetBitsTest {

    @Test
    public void testIntSetBits() {
        IntSetBits bits = new IntSetBits();
        MutableBits bits2 = MutableBits.create(100);
        IntList list = IntList.create(100);
        for (int i = 5; i <= 100; i += 5) {
            bits.set(i);
            bits2.set(i);
            list.add(i);
        }
        IntList found = IntList.create(100);
        bits.forEachSetBitAscending(b -> {
            found.add(b);
            System.out.println("FD " + b);
        });

        System.out.println("BIST " + bits);

        IntList foundRev = IntList.create(100);

        System.out.println("BITS: " + bits);
        assertEquals(list, found);
        for (int i = 0; i < 100; i++) {
            int next = bits.nextSetBit(i);
            int realNext = bits2.nextSetBit(i);
            System.out.println("NEXT SET " + i + " " + realNext + " got " + next);
            assertEquals(realNext, next, "Difference for " + i);
        }

        for (int i = 99; i >=0; i--) {
            int realNext = bits2.previousSetBit(i);
            int next = bits.previousSetBit(i);
            assertEquals(realNext, next, "Descending difference at " + i);
        }
    }
    @Test
    public void testSet() {
        IntSet arr = IntSet.arrayBased(10);
        for (int i = 5; i < 100; i += 5) {
            arr.add(i);
            int ixA = arr.nearestIndexTo(i, Bias.FORWARD);
        }
        IndexedResolvable<String> res = new IndexedResolvable<String>(){
            @Override
            public String forIndex(int index) {
                return Integer.toString(index);
            }

            @Override
            public int size() {
                return arr.size();
            }

            @Override
            public int indexOf(Object obj) {
                return Integer.parseInt(obj.toString());
            }
        };

        BitSetSet<String> bss = new BitSetSet<String>(res, new IntSetBits(arr));
        int ct = 0;
        int expSize = arr.size();
        for (String s : bss) {
            System.out.println(ct + ": " + s);
            ct++;
            if (ct >= expSize + 1) {
                fail("Iterated too many times with " + s + " at " + ct);
            }
        }
    }
}
