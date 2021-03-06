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

/**
 * Specification for an element of a record sufficient to read it given a buffer
 * containing a record.
 *
 * @author Tim Boudreau
 */
public interface RecordElement {

    /**
     * The type of value at the position of this element within a record.
     *
     * @return A value type
     */
    ValueType type();

    /**
     * The byte offset from the start of a record at which this element occurs.
     *
     * @return A byte offset, &gt;= 0
     */
    int byteOffset();

    /**
     * Read a value at a given offset, returning it as a long.
     *
     * @param buf A buffer
     * @return A long
     */
    default long read(ByteBuffer buf) {
        return type().read(byteOffset(), buf);
    }

}
