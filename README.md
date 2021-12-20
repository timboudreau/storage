Storage
=======

A set of libraries for working with memory-mapped data and fixed-record-length storage.

Originally written for building efficient indexes into Java heap dumps, this library provides a general-purpose
set of tools for building such indexes.

These allow you to build single-purpose high-performance persistent micro-databases.


The Storage Library
-------------------

A `Storage` gives read- and (optionally) write- access to some bytes.  It has a record-size in bytes, which
is the number of bytes in one record, and may contain many records.

A number of implementations with different characteristics are included - `FileChannel`-based, a single
memory-mapped implementation for files where you can guarantee the file size will remain below the
maximum size the operating system or available memory will be able to accomodate in a single memory-mapping.
A multiple-memory-mapping version maintains as many memory-mappings as are needed to have the entire storage
memory-mapped.

The data in the backing file of a `Storage` is entirely record-based - there are no headers or other
data, so the position of a record is a simple function of the number of records from the start of the file
it exists at.

Records can be written and/or rewritten or appended, looked up by index, and a Storage can be treated as an
`Iterable<ByteBuffer>`.

The two less prosaic features of `Storage` are:

  * The contents can be sorted in-place using whatever sort function you choose
  * Once sorted, the contents can be efficiently binary-searched

That makes possible...


The Indexes Library
-------------------

An `Index` utilizes the above feature to allow fast lookup of records.  The typical pattern is that you
write some data you want to look up by multiple sortable fields;  either as you write it or thereafter
you create an index that allows you to look up elements by field.

An index has a schema; the schema is expressed as a Java `enum`, where the fields appear in the order
they occur in the enum.  Each has a (primitive) type and a byte-offset into the record, and an
`IndexKind` that describes whether that field is unique or not, and should be treated as the canonical
ordering - equivalent to a _primary key_ in SQL.

Unique and many-to-many indexes are supported.

Typically the canonical ordering is the position of the referenced record in the original data you're
creating an index over - that way, your index only need contain the file offset and the one field
you want to index, and gets stored in that order.

Example:

```java
public enum Classes implements SchemaItem {
    // offset, classId, nameId, superclassId, instanceSize
    FILE_OFFSET, CLASS_ID, NAME_ID, SUPERCLASS_ID, INSTANCE_SIZE;

    @Override
    public ValueType type() {
        return ValueType.LONG;
    }

    @Override
    public int byteOffset() {
        switch (this) {
            case FILE_OFFSET:
                return Integer.BYTES;
            case CLASS_ID:
                return Integer.BYTES + Long.BYTES;
            case NAME_ID:
                return Integer.BYTES + Long.BYTES * 2;
            case SUPERCLASS_ID:
                return Integer.BYTES + Long.BYTES * 3;
            case INSTANCE_SIZE:
                return Integer.BYTES + Long.BYTES * 4;
            default:
                throw new AssertionError(this);
        }
    }

    @Override
    public IndexKind indexKind() {
        switch (this) {
            case FILE_OFFSET:
                return IndexKind.CANONICAL_ORDERING;
            case CLASS_ID:
                return IndexKind.UNIQUE;
            default:
                return IndexKind.NONE;
        }
    }
}
```
