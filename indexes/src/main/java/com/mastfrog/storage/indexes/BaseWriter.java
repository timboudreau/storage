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

import com.mastfrog.storage.indexes.SchemaItem.IndexKind;
import com.mastfrog.storage.Storage;
import com.mastfrog.storage.Storage.StorageSpecification;
import com.mastfrog.function.throwing.io.IORunnable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author Tim Boudreau
 */
class BaseWriter<S extends Enum<S> & SchemaItem> implements IndexWriter {

    protected final FileChannel channel;
    final Path dir;
    final Set<S> indices;
    final String name;
    final int recordSize;
    protected final ThreadLocal<ByteBuffer> buf;
    private int index;
    private final StorageSpecification spec;
    private final S canonicalOrderingField;
    private final AtomicLong writeThread = new AtomicLong();
    private final AtomicBoolean hasMultiThreadedWrites = new AtomicBoolean();

    public BaseWriter(Path dir, String name, Set<S> indices, int recordSize, StorageSpecification spec) throws IOException {
        buf = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(recordSize));
        this.indices = indices;
        this.dir = dir;
        this.name = name;
        Path file = dir.resolve(name + ".offsets");
        channel = FileChannel.open(file, CREATE, READ, WRITE);
        this.recordSize = recordSize;
        this.spec = spec.withSize(recordSize).readWrite().initiallyMapped();
        canonicalOrderingField = findCanonicalOrderingField(indices);
    }

    static <S extends Enum<S> & SchemaItem> S findCanonicalOrderingField(Set<S> indices) {
        S canon = null;
        for (S s : indices) {
            if (s.indexKind() == IndexKind.CANONICAL_ORDERING) {
                if (canon != null) {
                    throw new IllegalArgumentException(
                            "More than one field has index type CANONICAL_ORDERING in "
                                    + indices + ". Have both " + canon + " and " + s);
                }
                canon = s;
            }
        }
        return canon;
    }

    public boolean supportsMultiThreadedWrites() {
        return canonicalOrderingField != null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("IndexWriter for ");
        sb.append(name);
        if (!indices.isEmpty()) {
            sb.append(" with ");
            int ix = 0;
            for (S s : indices) {
                if (ix++ > 0) {
                    sb.append(", ");
                }
                sb.append(s.name());
                sb.append(" @ ").append(s.byteOffset())
                        .append(' ').append(s.type());
            }
        }
        sb.append(" in ").append(dir);
        return sb.toString();
    }

    protected ByteBuffer buffer() {
        if (!hasMultiThreadedWrites.get()) {
            long tid = Thread.currentThread().getId();
            writeThread.getAndUpdate(old -> {
                if (old != 0 && old != tid) {
                    if (canonicalOrderingField == null) {
                        throw new IllegalStateException("Cannot write to an index from more than one "
                                + " thread if the index has no field of kind CANONICAL_ORDERING");
                    }
                    hasMultiThreadedWrites.set(true);
                }
                return tid;
            });
        }
        ByteBuffer b = buf.get();
        b.rewind();
        b.limit(recordSize);
        b.putInt(index++);
        return b;
    }

    private void quietly(IORunnable run) {
        run.toRunnable().run();
    }

    @Override
    public void write(ByteBuffer buf) {
        if (buf.remaining() == 0) {
            buf.flip();
        }
        quietly(() -> channel.write(buf));
    }

    @Override
    public void close() throws IOException {
        try {
            if (hasMultiThreadedWrites.get()) {
                sortByCanonicalOrderingAndRenumber();
            }
            writeSubsidiaryIndexes();
        } finally {
            channel.close();
        }
    }

    private void sortByCanonicalOrderingAndRenumber() throws IOException {
        System.out.println("Have multithreaded writes - sorting base");
        Storage.StorageSpecification spec = this.spec.copy().alwaysMapped().readWrite().concurrency(4);
        Storage stor = Storage.create(channel, spec);
        stor.sort(canonicalOrderingField.byteOffset(), canonicalOrderingField.type());

        System.out.println("Renumbering base");
        for (long i=0; i < stor.size(); i++) {
            stor.writeInt(i, (int) i, 0);
        }
    }

    private Map<S, Path> writeSubsidiaryIndexes() throws IOException {
        Map<S, Path> result = null;
        for (S s : indices) {
            if (s.indexKind().isCanonicalOrdering()) {
                System.out.println("Skip canonical ordering " + s + " in " + s.getClass().getSimpleName());
                continue;
            }
            channel.position(0);
            if (result == null) {
                result = new EnumMap<>(s.getDeclaringClass());
            }
            Path nue = dir.resolve(s.fileName(name));
            System.err.println("Create index " + nue);
            long then = System.currentTimeMillis();
            try (final FileChannel ch = FileChannel.open(nue, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                ch.transferFrom(channel, 0, channel.size());
                Storage.StorageSpecification spec = this.spec.copy().alwaysMapped().readWrite().concurrency(2);
                Storage stor = Storage.create(ch, spec);
                stor.sort(s.byteOffset(), s.type());
            }
            long elapsed = System.currentTimeMillis() - then;
            System.err.println("Wrote and sorted " + nue + " in " + elapsed + " ms");
            result.put(s, nue);
        }
        return result == null ? Collections.emptyMap() : result;
    }

    @Override
    public void write(long offset) {
        throw new UnsupportedOperationException("Wrong number of arguments for a " + getClass().getSimpleName());
    }

    @Override
    public void write(long offset, long id) {
        throw new UnsupportedOperationException("Wrong number of arguments for a " + getClass().getSimpleName());
    }

    @Override
    public void write(long offset, long id, long type) {
        throw new UnsupportedOperationException("Wrong number of arguments for a " + getClass().getSimpleName());
    }

    @Override
    public void write(long offset, long id, long type, int ser) {
        throw new UnsupportedOperationException("Wrong number of arguments for a " + getClass().getSimpleName());
    }

    @Override
    public void write(long offset, long id, long type, long x, long y) {
        throw new UnsupportedOperationException("Wrong number of arguments for a " + getClass().getSimpleName());
    }
}
