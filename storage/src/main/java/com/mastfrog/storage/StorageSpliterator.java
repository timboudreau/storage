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
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 *
 * @author Tim Boudreau
 */
final class StorageSpliterator implements Spliterator<ByteBuffer> {

    private final Storage storage;
    private long start;
    private long end;

    public StorageSpliterator(Storage storage, long start, long end) {
        this.storage = storage;
        this.start = start;
        this.end = end;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ByteBuffer> action) {
        if (start >= end) {
            return false;
        }
        action.accept(storage.forIndex(start++));
        return true;
    }

    @Override
    public void forEachRemaining(Consumer<? super ByteBuffer> action) {
        for (; start < end; start++) {
            action.accept(storage.forIndex(start));
        }
    }

    @Override
    public Spliterator<ByteBuffer> trySplit() {
        long remaining = end - start;
        if (remaining < 2) {
            return null;
        }
        long mid = start + remaining / 2;
        Spliterator<ByteBuffer> result = new StorageSpliterator(storage, mid, end);
        end = mid;
        return result;
    }

    @Override
    public long estimateSize() {
        return end - start;
    }

    @Override
    public long getExactSizeIfKnown() {
        return end - start;
    }

    @Override
    public int characteristics() {
        return Spliterator.CONCURRENT
                | Spliterator.DISTINCT
                | Spliterator.IMMUTABLE
                | Spliterator.NONNULL
                | Spliterator.ORDERED
                | Spliterator.SIZED
                | Spliterator.ORDERED
                | Spliterator.SUBSIZED;
    }
}
