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
public class IndexReaderBiAdapter <S extends Enum<S> & SchemaItem, A extends Enum<A> & SchemaItem> implements IntMatrixMap.LongMapAdapter {

    private final IndexReader<S> keys;
    private final IndexReader<A> values;
    private final S keyField;
    private final A valueField;

    public IndexReaderBiAdapter(IndexReader<S> keys, IndexReader<A> values, S keyField, A valueField) {
        this.keys = keys;
        this.values = values;
        this.keyField = keyField;
        this.valueField = valueField;
    }

    @Override
    public int indexOfKey(long key) {
        if (keyField != null) {
            return (int) keys.indexOf(keyField, key);
        }
        return (int) keys.indexOf(key);
    }

    @Override
    public int indexOfValue(long value) {
        if (valueField != null) {
            return (int) values.indexOf(valueField, value);
        }
        return (int) values.indexOf(value);
    }

    @Override
    public long keyForKeyIndex(int index) {
        if (keyField != null) {
            return keys.valueFor(index, keyField);
        }
        ByteBuffer buf = keys.get(index);
        return buf.getLong(Integer.BYTES);
    }

    @Override
    public long valueForValueIndex(int index) {
        if (valueField != null) {
            return values.valueFor(index, valueField);
        }
        ByteBuffer buf = values.get(index);
        return buf.getLong(Integer.BYTES);
    }
}
