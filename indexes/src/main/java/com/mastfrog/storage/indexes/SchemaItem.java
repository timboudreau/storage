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

import com.mastfrog.storage.RecordElement;

/**
 * Intended to be implemented by enums to provide schema info to a DataIndex.
 *
 * @author Tim Boudreau
 */
public interface SchemaItem extends RecordElement {

    String name();

    @Override
    default int byteOffset() {
        SchemaItem[] all = getClass().getEnumConstants();
        int base = Integer.BYTES;
        for (SchemaItem all1 : all) {
            if (all1 == this) {
                break;
            } else {
                base += all1.type().size();
            }
        }
        return base;
    }

    default IndexKind indexKind() {
        if (name().equals("OFFSET") || name().equals("FILE_OFFSET")) {
            return IndexKind.CANONICAL_ORDERING;
        }
        return IndexKind.NONE;
    }

    default String fileName(String baseName) {
        return baseName + '.' + name().toLowerCase() + 's';
    }

    public enum IndexKind {
        NONE,
        CANONICAL_ORDERING,
        UNIQUE, // ONE_TO_MANY
        ;

        boolean isUnique() {
            switch (this) {
                case UNIQUE:
                case CANONICAL_ORDERING:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public String toString() {
            return name().toLowerCase();
        }

        boolean isCanonicalOrdering() {
            return this == CANONICAL_ORDERING;
        }
    }
}
