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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * Provides a shared ThreadLocal pool of ByteBuffers for reuse, acknowledging
 * that the same thread may need more than one at a time.
 *
 * @author Tim Boudreau
 */
public final class Buffers {

    private final ThreadLocal<ByteBuffer>[] locs;
    private final boolean direct;

    @SuppressWarnings("unchecked")
    public Buffers(int bufferSize, int concurrentUse, boolean direct) {
        this.direct = direct;
        ThreadLocal[] locs = new ThreadLocal[concurrentUse];
        Supplier<ByteBuffer> supp = direct ? new DirectBBSupplier(bufferSize)
                : new HeapBBSupplier(bufferSize);
        for (int i = 0; i < concurrentUse; i++) {
            locs[i] = ThreadLocal.withInitial(supp);
        }
        this.locs = locs;
    }

    public boolean isDirect() {
        return direct;
    }

    public int concurrentMax() {
        return locs.length;
    }

    public ByteBuffer allocate(int size) {
        if (direct) {
            return ByteBuffer.allocateDirect(size);
        } else {
            return ByteBuffer.allocate(size);
        }
    }

    public ByteBuffer get(boolean redBlack) {
        return get(redBlack ? 1 : 0);
    }

    public ByteBuffer get(int usage) {
        int target = usage % locs.length;
        ByteBuffer result = locs[target].get();
        result.rewind();
        result.limit(result.capacity());
        return result;
    }

    public static IntFunction<Buffers> factory(boolean preferDirect, int concurrency, boolean memoizing) {
        IntFunction<Buffers> f = sz -> {
            return new Buffers(sz, concurrency, preferDirect);
        };
        if (memoizing) {
            return new M(f);
        } else {
            return f;
        }
    }

    static final class M implements IntFunction<Buffers> {
        private Map<Integer, Buffers> map = new HashMap<>();
        private final IntFunction<Buffers> factory;

        public M(IntFunction<Buffers> factory) {
            this.factory = factory;
        }


        @Override
        public Buffers apply(int value) {
            return map.computeIfAbsent(value, v -> factory.apply(v));
        }
    }

    static final class HeapBBSupplier implements Supplier<ByteBuffer> {

        private final int size;

        public HeapBBSupplier(int size) {
            this.size = size;
        }

        @Override
        public ByteBuffer get() {
            return ByteBuffer.allocate(size);
        }
    }

    static final class DirectBBSupplier implements Supplier<ByteBuffer> {

        private final int size;

        public DirectBBSupplier(int size) {
            this.size = size;
        }

        @Override
        public ByteBuffer get() {
            return ByteBuffer.allocateDirect(size);
        }
    }
}
