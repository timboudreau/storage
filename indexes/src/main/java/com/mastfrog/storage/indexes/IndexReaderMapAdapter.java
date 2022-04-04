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
import java.nio.ByteBuffer;

/**
 *
 * @author Tim Boudreau
 */
class IndexReaderMapAdapter<S extends Enum<S> & SchemaItem> implements IntMatrixMap.LongMapAdapter {

    private final IndexReader<S> reader;
    private final S field;

    IndexReaderMapAdapter(IndexReader<S> reader, S field) {
        this.reader = reader;
        this.field = field;
    }

    @Override
    public int indexOfKey(long key) {
        if (field != null) {
            return (int) reader.indexOf(field, key);
        }
        return (int) reader.indexOf(key);
    }

    @Override
    public int indexOfValue(long value) {
        return indexOfKey(value);
    }

    @Override
    public long keyForKeyIndex(int index) {
        if (field != null) {
            return reader.valueFor(index, field);
        }
        ByteBuffer buf = reader.get(index);
        return buf.getLong(Integer.BYTES);
    }

    @Override
    public long valueForValueIndex(int index) {
        return keyForKeyIndex(index);
    }

}
