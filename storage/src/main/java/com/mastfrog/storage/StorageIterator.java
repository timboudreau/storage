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
import java.util.Iterator;
import java.util.PrimitiveIterator;

/**
 *
 * @author Tim Boudreau
 */
public final class StorageIterator implements Iterator<ByteBuffer> {

    private final Storage storage;
    private final long size;
    private long cursor;

    public StorageIterator(Storage storage, long cursor) {
        this.size = storage.size();
        this.storage = storage;
        this.cursor = cursor;
    }

    @Override
    public boolean hasNext() {
        return cursor < size;
    }

    @Override
    public ByteBuffer next() {
        return storage.forIndex(cursor++);
    }

    public PrimitiveIterator.OfLong adapted(int offset) {
        return new Adapted(offset);
    }

    private class Adapted implements PrimitiveIterator.OfLong {
        private final int offset;

        public Adapted(int offset) {
            this.offset = offset;
        }

        @Override
        public long nextLong() {
            return StorageIterator.this.next().getLong(offset);
        }

        @Override
        public boolean hasNext() {
            return StorageIterator.this.hasNext();
        }

    }

}
