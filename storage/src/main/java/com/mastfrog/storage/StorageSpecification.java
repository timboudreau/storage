/*
 * The MIT License
 *
 * Copyright 2022 Mastfrog Technologies.
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

/**
 * Describes the desired characteristics of a desired Storage instance
 * without tying it to a specific implementation type. Setter methods mutate
 * the instance.
 */
public class StorageSpecification {

    boolean preferMapped = true;
    boolean preferDirect = true;
    boolean alwaysMapped = false;
    boolean writable = true;
    int concurrency = 4;
    final int size;

    public StorageSpecification(int size) {
        this.size = size;
    }

    public Buffers buffers() {
        return new Buffers(size, concurrency, preferDirect);
    }

    /**
     * Creates a new instance with defaults and a size of zero for use as a
     * template.
     *
     * @return A spec
     */
    public static StorageSpecification defaultSpec() {
        return new StorageSpecification(0);
    }

    public boolean isReadWrite() {
        return writable;
    }

    public boolean isDirect() {
        return preferDirect;
    }

    public boolean isMapped() {
        return alwaysMapped || preferMapped;
    }

    public StorageSpecification copy() {
        StorageSpecification result = new StorageSpecification(size);
        result.preferMapped = preferMapped;
        result.preferDirect = preferDirect;
        result.alwaysMapped = alwaysMapped;
        result.writable = writable;
        result.concurrency = concurrency;
        return result;
    }

    public StorageSpecification withSize(int size) {
        StorageSpecification result = new StorageSpecification(size);
        result.preferMapped = preferMapped;
        result.preferDirect = preferDirect;
        result.alwaysMapped = alwaysMapped;
        result.writable = writable;
        result.concurrency = concurrency;
        return result;
    }

    public StorageSpecification readOnly() {
        writable = false;
        return this;
    }

    public StorageSpecification readWrite() {
        writable = true;
        return this;
    }

    public StorageSpecification direct() {
        preferDirect = true;
        return this;
    }

    public StorageSpecification heap() {
        preferDirect = false;
        return this;
    }

    public StorageSpecification initiallyMapped() {
        preferMapped = true;
        return this;
    }

    public StorageSpecification initiallyUnmapped() {
        preferMapped = false;
        return this;
    }

    public StorageSpecification alwaysMapped() {
        preferMapped = true;
        alwaysMapped = true;
        return this;
    }

    /**
     * This is not the usual meaning of "concurrency" - it refers to how
     * many buffers may be used by a <i>single</i> thread such that none of
     * them should be recycled - i.e. when sorting, one needs at least two
     * buffers if you want to compare their contents - recycling the same
     * buffer and changing its contents would be counterproductive.
     * <p>
     * Storage uses a thread local pool of buffers.
     *
     * @param val The number of buffers in each thread's pool
     * @return this
     */
    public StorageSpecification concurrency(int val) {
        concurrency = val;
        return this;
    }

}
