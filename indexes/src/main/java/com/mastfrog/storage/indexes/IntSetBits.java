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
import com.mastfrog.bits.DoubleLongFunction;
import com.mastfrog.bits.MutableBits;
import com.mastfrog.util.collections.IntSet;
import com.mastfrog.util.search.Bias;
import java.io.Serializable;
import java.util.PrimitiveIterator;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.function.IntSupplier;
import java.util.function.IntToDoubleFunction;

/**
 * To minimize memory size building potentially enormous graphs, we need an
 * array-based IntSet rather than anything based on BitSet, which would require
 * several million unused bits to be allocated.
 *
 * @author Tim Boudreau
 */
public class IntSetBits implements MutableBits, Serializable {

    private final IntSet intSet;

    public static IntSetBits EMPTY = new IntSetBits(IntSet.EMPTY);

    public IntSetBits(IntSet intSet) {
        this.intSet = intSet;
    }

    public IntSetBits() {
        this(IntSet.arrayBased(4));
    }

    public IntSetBits(int size) {
        this(IntSet.arrayBased(Math.min(8, size)));
    }

    @Override
    public void set(int bitIndex, boolean value) {
        if (bitIndex < 0) {
            throw new IllegalArgumentException("Invalid bit " + bitIndex);
        }
        if (value) {
            intSet.add(bitIndex);
        } else {
            intSet.remove(bitIndex);
        }
    }

    @Override
    public int cardinality() {
        return intSet.size();
    }

    @Override
    public Bits copy() {
        return new IntSetBits(intSet.copy());
    }

    @Override
    public MutableBits mutableCopy() {
        return new IntSetBits(intSet.copy());
    }

    @Override
    public boolean get(int bitIndex) {
        return intSet.contains(bitIndex);
    }

    @Override
    public int nextClearBit(int fromIndex) {
        if (isEmpty()) {
            return fromIndex;
        }
        while (intSet.contains(fromIndex)) {
            fromIndex++;
        }
        return fromIndex;
    }

    @Override
    public boolean isEmpty() {
        return intSet.isEmpty();
    }

    @Override
    public int nextSetBit(int fromIndex) {
        if (isEmpty() || fromIndex < 0) {
            return -1;
        }
        int ix = intSet.nearestIndexTo(fromIndex, Bias.FORWARD);
        if (ix < 0) {
            return -1;
        }
        int val = intSet.valueAt(ix);
        return val;
    }

    @Override
    public int previousClearBit(int fromIndex) {
        if (isEmpty()) {
            return fromIndex;
        }
        while (intSet.contains(fromIndex)) {
            fromIndex--;
        }
        return fromIndex;
    }

    @Override
    public int previousSetBit(int fromIndex) {
        if (isEmpty()) {
            return fromIndex - 1;
        }
        int ix = intSet.nearestIndexTo(fromIndex, Bias.BACKWARD);
        if (ix < 0) {
            return -1;
        }
        int result = intSet.valueAt(ix);
        if (result > fromIndex) {
            result = -1;
        }
        return result;
    }

    @Override
    public void clear() {
        intSet.clear();
    }

    @Override
    public void clear(int bitIndex) {
        intSet.remove(bitIndex);
    }

    @Override
    public boolean contentEquals(Bits other) {
        if (other == Bits.EMPTY) {
            return this.isEmpty();
        }
        if (other instanceof IntSetBits) {
            IntSetBits isb = (IntSetBits) other;
            return isb.intSet.equals(intSet);
        }
        return MutableBits.super.contentEquals(other);
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (o == null || !(o instanceof Bits)) {
            return false;
        }
        Bits b = (Bits) o;
        return contentEquals(b);
    }

    public int hashCode() {
        return bitsHashCode();
    }

    @Override
    public void clear(int fromIndex, int toIndex) {
        if (isEmpty()) {
            return;
        }
        intSet.removeRange(fromIndex, toIndex);
    }

    @Override
    public MutableBits andWith(Bits other) {
        if (other.isEmpty() || this.isEmpty()) {
            return new IntSetBits(1);
        }
        return MutableBits.super.andWith(other);
    }

    @Override
    public void and(Bits other) {
        if (other instanceof IntSetBits) {
            intSet.retainAll(((IntSetBits) other).intSet);
            return;
        } else {
            IntSet toRemove = IntSet.arrayBased(intSet.size());
            for (int i=0; i < intSet.size(); i++) {
                int val = intSet.valueAt(i);
                if (!other.get(val)) {
                    toRemove.add(val);
                }
            }
            if (!toRemove.isEmpty()) {
                intSet.removeAll(toRemove);
            }
        }
        MutableBits.super.and(other);
    }

    @Override
    public void or(Bits other) {
        if (other.isEmpty()) {
            return;
        }
        if (other instanceof IntSetBits) {
            intSet.addAll(((IntSetBits) other).intSet);
            return;
        } else {
            other.forEachSetBitAscending((int bit) -> intSet.add(bit));
            return;
        }
    }

    @Override
    public MutableBits orWith(Bits other) {
        IntSet nue = intSet.copy();
        if (other instanceof IntSetBits) {
            nue.addAll(((IntSetBits) other).intSet);
        } else {
            other.forEachSetBitAscending(bit -> {
                nue.add(bit);
            });
        }
        return new IntSetBits(nue);
    }

    @Override
    public void flip(int bitIndex) {
        if (intSet.contains(bitIndex)) {
            intSet.remove(bitIndex);
        } else {
            intSet.add(bitIndex);
        }
    }

    @Override
    public void set(int bitIndex) {
        set(bitIndex, true);
    }

    @Override
    public void set(long bitIndex, boolean value) {
        if (value) {
            set((int) bitIndex, value);
        } else {
            intSet.remove((int) bitIndex);
        }
    }

    @Override
    public IntSupplier asIntSupplier() {
        PrimitiveIterator.OfInt iter = intSet.iterator();
        return () -> {
            if (iter.hasNext()) {
                return iter.nextInt();
            }
            return -1;
        };
    }

    @Override
    public int forEachSetBitAscending(IntConsumer consumer) {
        if (isEmpty()) {
            return 0;
        }
        intSet.iterator().forEachRemaining(consumer);
        return cardinality();
    }

    @Override
    public int forEachSetBitAscending(IntPredicate consumer) {
        int result = 0;
        for (int i = 0; i < intSet.size(); i++) {
            int val = intSet.valueAt(i);
            result++;
            if (!consumer.test(val)) {
                break;
            }
        }
        return result;
    }

    @Override
    public int forEachSetBitAscending(int from, IntConsumer consumer) {
        if (isEmpty()) {
            return 0;
        }
        int result = 0;
        int nv = intSet.nearestIndexTo(from, Bias.FORWARD);
        for (int i = 0; i < intSet.size(); i++) {
            consumer.accept(intSet.valueAt(i));
            result++;
        }
        return result;
    }

    @Override
    public int forEachSetBitAscending(int from, IntPredicate consumer) {
        if (isEmpty()) {
            return 0;
        }
        int result = 0;
        int nv = intSet.nearestIndexTo(from, Bias.FORWARD);
        for (int i = 0; i < intSet.size(); i++) {
            result++;
            if (!consumer.test(intSet.valueAt(i))) {
                break;
            }
        }
        return result;
    }

    @Override
    public Bits filter(IntPredicate pred) {
        IntSet nue = IntSet.arrayBased(intSet.size());
        for (int i = 0; i < intSet.size(); i++) {
            int val = intSet.valueAt(i);
            if (pred.test(intSet.valueAt(i))) {
                nue.add(val);
            }
        }
        return new IntSetBits(nue);
    }

    @Override
    public void andNot(Bits set) {
        if (set.isEmpty()) {
            return;
        }
        int length = Math.max(length(), set.length());
        for (int i = 0; i < length; i++) {
            set(i, get(i) && !set.get(i));
        }
    }

    @Override
    public int length() {
        if (isEmpty()) {
            return 0;
        }
        return intSet.last() + 1;
    }

    @Override
    public MutableBits get(int fromIndex, int toIndex) {
        IntSet result = IntSet.arrayBased(toIndex - fromIndex);
        this.forEachSetBitAscending(fromIndex, toIndex,  (int bit) -> result.add(bit));
        return new IntSetBits(result);
    }

    @Override
    public double sum(IntToDoubleFunction f) {
        double result = 0.0;
        int sz = intSet.size();
        for (int i = 0; i < sz; i++) {
            int val = intSet.valueAt(i);
            result += f.applyAsDouble(val);
        }
        return result;
    }

    @Override
    public double sum(double[] values, int ifNot) {
        double result = 0.0;
        int sz = intSet.size();
        for (int i = 0; i < sz; i++) {
            int val = intSet.valueAt(i);
            result += values[val];
        }
        return result;
    }

    @Override
    public double sum(DoubleLongFunction f) {
        double result = 0.0;
        int sz = intSet.size();
        for (int i = 0; i < sz; i++) {
            int val = intSet.valueAt(i);
            result += f.apply(val);
        }
        return result;
    }

    @Override
    public String stringValue() {
        return intSet.toString();
    }
}
