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

import com.mastfrog.bits.collections.IntMatrixMap;
import com.mastfrog.bits.large.LongArray;
import com.mastfrog.function.LongBiConsumer;
import com.mastfrog.function.LongBiPredicate;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

/**
 *
 * @author Tim Boudreau
 */
class SavableLongMapWrapper implements SavableLongMatrixMap {

    private final IntMatrixMap base;
    private final IntMatrixMap.LongMatrixMap delegate;

    SavableLongMapWrapper(IntMatrixMap base, IntMatrixMap.LongMatrixMap delegate) {
        this.base = base;
        this.delegate = delegate;
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public void save(Path file) throws IOException {
        LongArray.saveForMapping(base.toLongArray(), file);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public void forEachPair(LongBiConsumer lbc) {
        delegate.forEachPair(lbc);
    }

    @Override
    public int forEachPair(LongBiPredicate lbp) {
        return delegate.forEachPair(lbp);
    }

    @Override
    public long getOrDefault(long l, long l1) {
        return delegate.getOrDefault(l, l1);
    }

    @Override
    public void put(long l, long l1) {
        delegate.put(l, l1);
    }

    @Override
    public boolean containsKey(long l) {
        return delegate.containsKey(l);
    }

    @Override
    public boolean contains(long l, long l1) {
        return delegate.contains(l, l1);
    }

    @Override
    public void remove(long l) {
        delegate.remove(l);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 29 * hash + Objects.hashCode(this.base);
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
        final SavableLongMapWrapper other = (SavableLongMapWrapper) obj;
        return Objects.equals(this.base, other.base);
    }

}
