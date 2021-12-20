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

import com.mastfrog.bits.large.LongArray;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

/**
 *
 * @author Tim Boudreau
 */
public class LongArrayAtomic implements LongArray {

    private final long MAX = Integer.MAX_VALUE / 2;
    private AtomicLongArray[] partitions;
    private long size;

    LongArrayAtomic(long size) {
        List<AtomicLongArray> all = new ArrayList<>();
        int partitions = (int) (size / MAX);
        System.out.println("Create with " + partitions + " partitions");
        long start = 0;
        for (int i = 0; i < partitions; i++) {
            int sz;
            if (i == partitions - 1) {
                sz = (int) (size - start);
            } else {
                sz = Integer.MAX_VALUE;
            }
            System.out.println("  " + i + ". " + sz);
            AtomicLongArray arr = new AtomicLongArray(sz);
            all.add(arr);
            start += sz;
        }
        this.partitions = all.toArray(new AtomicLongArray[partitions]);
    }

    @Override
    public long size() {
        return size;
    }

    private interface PC {

        long withPartition(int offset, AtomicLongArray arr);
    }

    private interface PCx {

        void withPartition(int offset, AtomicLongArray arr);
    }

    long withPartition(long index, PC pc) {
        int p = (int) (index / MAX);
        int off = (int) (index % MAX);
        return pc.withPartition(off, partitions[p]);
    }

    void withPartition(long index, PCx pc) {
        int p = (int) (index / MAX);
        int off = (int) (index % MAX);
        pc.withPartition(off, partitions[p]);
    }

    @Override
    public long get(long index) {
        return withPartition(index, (ix, arr) -> {
            return arr.get(ix);
        });
    }

    @Override
    public void set(long index, long value) {
        withPartition(index, (ix, arr) -> {
            arr.set(ix, value);
        });
    }

    @Override
    public void update(long index, LongUnaryOperator op) {
        withPartition(index, (ix, arr) -> {
            arr.updateAndGet(ix, op);
        });
    }

    @Override
    public LongFunction<LongArray> factory() {
        return ct -> new LongArrayAtomic(ct);
    }

    @Override
    public boolean isZeroInitialized() {
        return true;
    }

    @Override
    public void resize(long size) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }
}
