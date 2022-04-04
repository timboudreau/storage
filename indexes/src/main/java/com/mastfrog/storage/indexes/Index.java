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

import com.mastfrog.storage.Storage.StorageSpecification;
import com.mastfrog.storage.ValueType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A flexible, multi-field index over some numeric data. Typically these are
 * used to provide an indexed list of tags of some type in a heap dump, and also
 * associate the unique id for that item with that index, such that the index
 * can be binary searched for. Additional non-indexed fields can be associated
 * with a record.
 * <p>
 * Under the hood, creates a primary fixed-record-length file in the form [int
 * index][long file offset] followed by [optional id][optional other_data]. For
 * each indexable field, upon closing the writer, creates a copy of that file,
 * and sorts it in-place by the index field.
 * </p><p>
 * Indexable fields are defined by an enum which implements SchemaItem, which
 * determines the offset into a record of a given field.
 *
 * @author Tim Boudreau
 */
public final class Index<S extends Enum<S> & SchemaItem> {

    private final Path dir;
    private final S[] consts;
    private final int recordSize;
    private Set<S> unique;
    private final String name;
    private StorageSpecification spec;

    public Index(Path dir, Class<S> type) {
        this(dir, type.getSimpleName(), type);
    }

    public Index(Path dir, String name, Class<S> type) {
        this(dir, name, type, new StorageSpecification(0)
                .concurrency(4).readOnly().direct().alwaysMapped());
    }

    public Index(Path dir, String name, Class<S> type, StorageSpecification template) {
        this.dir = dir;
        this.name = name;
        unique = EnumSet.noneOf(type);
        consts = type.getEnumConstants();
        int size = Integer.BYTES;
        for (int i = 0; i < consts.length; i++) {
            size += consts[i].type().size();
            if (consts[i].indexKind().isUnique()) {
                unique.add(consts[i]);
            }
        }
        this.recordSize = size;
        this.spec = template.withSize(size);
    }

    @SuppressWarnings("unchecked")
    public <T extends Enum<T> & SchemaItem> Index<T> cast(Class<T> type) {
        if (consts.getClass().getComponentType() != type) {
            throw new ClassCastException(type + " is not " + consts.getClass().getComponentType());
        }
        return (Index) this;
    }

    public long size() {
        Path file = dir.resolve(name + ".offsets");
        if (Files.exists(file)) {
            try {
                return Files.size(file) / recordSize;
            } catch (IOException ex) {
                Logger.getLogger(Index.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return 0L;
    }

    private static final List<ValueType> THREE_LONG_INT_PATTERN
            = Arrays.asList(ValueType.LONG, ValueType.LONG, ValueType.LONG, ValueType.INT);

    private List<ValueType> indexPattern() {
        List<ValueType> vals = new ArrayList<>(consts.length);
        for (S s : consts) {
            vals.add(s.type());
        }
        return vals;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DataIndex ");
        sb.append(name);
        if (consts.length > 0) {
            sb.append(" with ");
            int ix = 0;
            for (S s : consts) {
                if (ix++ > 0) {
                    sb.append(", ");
                }
                sb.append(s.name());
                sb.append(" @ ").append(s.byteOffset())
                        .append(' ').append(s.type());
                sb.append(s.indexKind());
                if (s.indexKind().isUnique()) {
                    sb.append("+indexed");
                }
            }
        }
        sb.append(" in ").append(dir);
        return sb.toString();
    }

    public boolean exists() {
        Path base = dir.resolve(name + ".offsets");
        if (Files.exists(base)) {
            for (S uq : unique) {
                Path sub = dir.resolve(uq.fileName(name));
                if (!Files.exists(sub)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public enum None implements SchemaItem {
        FILE_OFFSET;

        ;

        @Override
        public ValueType type() {
            return ValueType.LONG;
        }

        @Override
        public IndexKind indexKind() {
            return IndexKind.NONE;
        }
    }

    public String name() {
        return name;
    }

    public IndexReader<S> reader() throws IOException {
        return new IndexReaderImpl<>(name, dir, unique, recordSize);
    }

    public IndexWriter writer() throws IOException {
        List<ValueType> pattern = indexPattern();
        if (THREE_LONG_INT_PATTERN.equals(pattern)) {
            return new Writer4<>(dir, name, unique, recordSize, spec);
        }
        int longCount = 0;
        for (S s : consts) {
            if (s.type() == ValueType.LONG) {
                longCount++;
            } else {
                throw new UnsupportedOperationException(
                        "Non-long values not supported yet");
            }
        }
        switch (longCount) {
            case 1:
                return new Writer1<>(dir, name, unique, recordSize, spec);
            case 2:
                return new Writer2<>(dir, name, unique, recordSize, spec);
            case 3:
                return new Writer3<>(dir, name, unique, recordSize, spec);
            case 5:
                return new Writer5<>(dir, name, unique, recordSize, spec);
            default:
                System.err.println("4-item schemas have no interface - "
                        + "write byte buffers only");
                return new BaseWriter<>(dir, name, unique, recordSize, spec);
        }
    }

    static class Writer1<S extends Enum<S> & SchemaItem> extends BaseWriter<S> {

        Writer1(Path dir, String name, Set<S> indices, int recordSize,
                StorageSpecification spec) throws IOException {
            super(dir, name, indices, recordSize, spec);
        }

        public void write(long offset) {
            ByteBuffer buf = buffer();
            buf.putLong(offset);
            buf.flip();
            write(buf);
        }
    }

    static class Writer2<S extends Enum<S> & SchemaItem> extends BaseWriter<S> {

        Writer2(Path dir, String name, Set<S> indices, int recordSize,
                StorageSpecification spec) throws IOException {
            super(dir, name, indices, recordSize, spec);
        }

        public void write(long offset, long id) {
            ByteBuffer buf = buffer();
            buf.putLong(offset);
            buf.putLong(id);
            buf.flip();
            write(buf);
        }
    }

    static class Writer3<S extends Enum<S> & SchemaItem> extends BaseWriter<S> {

        Writer3(Path dir, String name, Set<S> indices, int recordSize,
                StorageSpecification spec) throws IOException {
            super(dir, name, indices, recordSize, spec);
        }

        public void write(long offset, long id, long data) {
            ByteBuffer buf = buffer();
            buf.putLong(offset);
            buf.putLong(id);
            buf.putLong(data);
            buf.flip();
            write(buf);
        }
    }

    static class Writer4<S extends Enum<S> & SchemaItem> extends BaseWriter<S> {

        Writer4(Path dir, String name, Set<S> indices, int recordSize,
                StorageSpecification spec) throws IOException {
            super(dir, name, indices, recordSize, spec);
        }

        public void write(long offset, long id, long data, int x) {
            ByteBuffer buf = buffer();
            buf.putLong(offset);
            buf.putLong(id);
            buf.putLong(data);
            buf.putInt(x);
            buf.flip();
            write(buf);
        }
    }

    static class Writer5<S extends Enum<S> & SchemaItem> extends BaseWriter<S> {

        Writer5(Path dir, String name, Set<S> indices, int recordSize,
                StorageSpecification spec) throws IOException {
            super(dir, name, indices, recordSize, spec);
        }

        public void write(long offset, long id, long data, long x, long y) {
            ByteBuffer buf = buffer();
            buf.putLong(offset);
            buf.putLong(id);
            buf.putLong(data);
            buf.putLong(x);
            buf.putLong(y);
            buf.flip();
            write(buf);
        }
    }
}
