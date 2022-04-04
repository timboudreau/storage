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

import com.mastfrog.storage.Storage;
import com.mastfrog.storage.Storage.StorageSpecification;
import com.mastfrog.storage.ValueType;
import com.mastfrog.util.preconditions.Exceptions;
import com.mastfrog.util.search.Bias;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author Tim Boudreau
 */
final class IndexReaderImpl<S extends Enum<S> & SchemaItem> implements IndexReader<S> {

    private final String name;
    private final Path dir;
    private final Set<S> indexed;
    private final Storage primaryStorage;
    private final Map<S, Storage> secondary = new ConcurrentHashMap<>();
    private final int recordSize;
    private final StorageSpecification spec;

    IndexReaderImpl(String name, Path dir, Set<S> indexed, int recordSize) throws IOException {
        this(name, dir, indexed, recordSize, StorageSpecification.defaultSpec().readOnly());
    }

    IndexReaderImpl(String name, Path dir, Set<S> indexed, int recordSize, StorageSpecification spec) throws IOException {
        this.name = name;
        this.dir = dir;
        this.indexed = indexed;
        this.recordSize = recordSize;
        this.spec = spec.withSize(recordSize);
        Path base = dir.resolve(name + ".offsets");
        FileChannel channel = spec.isReadWrite()
                ? FileChannel.open(base, READ, WRITE)
                : FileChannel.open(base, READ);
        primaryStorage = Storage.create(channel, this.spec);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("IndexReader for ");
        sb.append(name);
        if (!indexed.isEmpty()) {
            sb.append(" with ");
            int ix = 0;
            for (S s : indexed) {
                if (ix++ > 0) {
                    sb.append(", ");
                }
                sb.append(s.name());
                sb.append(" @ ").append(s.byteOffset())
                        .append(' ').append(s.type());
            }
        }
        sb.append(" in ").append(dir);
        if (!secondary.isEmpty()) {
            sb.append(" with open ");
            int ix = 0;
            for (Map.Entry<S, Storage> e : secondary.entrySet()) {
                if (ix++ > 0) {
                    sb.append(", ");
                }
                sb.append(e.getKey()).append(":").append(e.getValue());
            }
        }
        return sb.toString();
    }

    public String name() {
        return name;
    }

    private Storage secondaryStorage(S key) {
        if (!key.indexKind().isUnique()) {
            throw new IllegalArgumentException(key + " is not indexed");
        }
        return secondary.computeIfAbsent(key, k -> {
            try {
                Path p = dir.resolve(key.fileName(name));
                System.out.println("Open " + p);
                FileChannel channel = spec.isReadWrite()
                        ? FileChannel.open(p, READ, WRITE)
                        : FileChannel.open(p, READ);
                return Storage.create(channel, spec);
            } catch (IOException ex) {
                return Exceptions.chuck(ex);
            }
        });
    }

    public long size() {
        return primaryStorage.size();
    }

    @Override
    public ByteBuffer get(long recordIndex) {
        return primaryStorage.forIndex(recordIndex);
    }

    @Override
    public long search(S index, long matching, Bias bias) {
        if (index.indexKind().isCanonicalOrdering()) {
            return indexOf(matching);
        }
        Storage sec = secondaryStorage(index);
        long locIx = sec.binarySearch(matching, index.byteOffset(), index.type(), bias);
        if (locIx < 0) {
            return -1;
        }
        ByteBuffer buf = sec.forIndex(locIx);
        return buf.getInt();
    }

    @Override
    public long search(long matching, Bias bias) {
        return primaryStorage.binarySearch(matching, Integer.BYTES, ValueType.LONG, bias);
    }

    @Override
    public void close() throws IOException {
        try {
            if (primaryStorage instanceof AutoCloseable) {
                ((AutoCloseable) primaryStorage).close();
            }
            for (Storage v : secondary.values()) {
                if (v instanceof AutoCloseable) {
                    ((AutoCloseable) v).close();
                }
            }
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw ((IOException) e);
            } else {
                throw new IOException(e);
            }
        } finally {
            secondary.clear();
        }
    }
}
