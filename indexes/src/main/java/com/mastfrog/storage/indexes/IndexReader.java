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

import com.mastfrog.bits.MutableBits;
import com.mastfrog.bits.collections.IntMatrixMap;
import com.mastfrog.bits.large.LongArray;
import com.mastfrog.util.search.Bias;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author Tim Boudreau
 */
public interface IndexReader<S extends Enum<S> & SchemaItem> extends AutoCloseable {

    ByteBuffer get(long recordIndex);

    long search(S index, long matching, Bias bias);

    long search(long matching, Bias bias);

    long size();

    String name();

    @Override
    public void close() throws IOException;

    default ByteBuffer find(long matching, Bias bias) {
        long result = search(matching, bias);
        return result < 0 ? null : get(result);
    }

    default ByteBuffer find(S index, long matching, Bias bias) {
        long result = search(index, matching, bias);
        if (result >= 0) {
            return get(result);
        }
        return null;
    }

    default long indexOf(S index, long matching) {
        return search(index, matching, Bias.NONE);
    }

    default long indexOf(long matching) {
        return search(matching, Bias.NONE);
    }

    default ByteBuffer recordForIndexOf(long matching) {
        long ix = indexOf(matching);
        return ix < 0 ? null : get(ix);
    }

    default ByteBuffer recordForIndexOf(S index, long matching) {
        long ix = indexOf(index, matching);
        return ix < 0 ? null : get(ix);
    }

    default long valueFor(long record, S index) {
        ByteBuffer buf = get(record);
        return index.type().read(index.byteOffset(), buf);
    }

    default SavableLongMatrixMap map() {
        IndexReaderMapAdapter<S> adap = new IndexReaderMapAdapter<>(this, null);
        long size = this.size();
        long sz = this.size();
        sz *= sz;
        long longCount = sz / 64;

//        LongArrayAtomic laa = new LongArrayAtomic(longCount);
//        LongArray laa = UnsafeLongArray.unsafeLongArray(longCount);
//        AtomicBits bits = AtomicBits.
        LongArray lng = LongArray.javaLongArray((int) longCount);
        MutableBits bts = lng.toBitSet().toBits();

        IntMatrixMap ato = IntMatrixMap.create(bts, (int) size);
        System.out.println("Cap " + ato.capacity());
        return new SavableLongMapWrapper(ato, ato.asLongMap(adap));
    }

    default SavableLongMatrixMap map(S key) {
        IndexReaderMapAdapter<S> adap = new IndexReaderMapAdapter<>(this, key);
        IntMatrixMap ato = IntMatrixMap.atomic((int) this.size());
        System.out.println("Cap " + ato.capacity());
        return new SavableLongMapWrapper(ato, ato.asLongMap(adap));
    }

    default <A extends Enum<A> & SchemaItem> SavableLongMatrixMap map(IndexReader<A> other, S keyField, A valField) {
        IndexReaderBiAdapter<S, A> adap = new IndexReaderBiAdapter<>(this, other, null, null);
        IntMatrixMap ato = IntMatrixMap.atomic((int) this.size());
        return new SavableLongMapWrapper(ato, ato.asLongMap(adap));
    }
}
