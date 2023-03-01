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

import com.mastfrog.function.throwing.io.IORunnable;
import static com.mastfrog.util.preconditions.Checks.greaterThanZero;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

/**
 * Slower but non-memory-intensive storage that uses a FileChannel under the
 * hood.
 *
 * @author Tim Boudreau
 */
public class FileChannelStorage implements Storage, AutoCloseable {

    private final FileChannel channel;
    private final int recordSize;
    private final Buffers buffers;
    private final AtomicInteger usageCount = new AtomicInteger();
    private final long byteSize;

    public FileChannelStorage(FileChannel channel, int recordSize, IntFunction<Buffers> bufs) {
        this.channel = channel;
        this.recordSize = greaterThanZero("recordSize", recordSize);
        this.buffers = bufs.apply(recordSize);
        try {
            byteSize = channel.size();
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public int recordSize() {
        return recordSize;
    }

    @Override
    public ByteBuffer forIndex(long index) {
        ByteBuffer buf = buffers.get(usageCount.getAndIncrement());
        quietly(() -> {
            int readCount = channel.read(buf, offsetOf(index));
            assert readCount == recordSize() : "Only read " + readCount + " / " + recordSize();
        }
        );
        buf.flip();
        return buf;
    }

    @Override
    public ByteBuffer read(int record) {
        ByteBuffer buf = buffers.get(usageCount.getAndIncrement());
        quietly(() -> {
            int readCount = channel.read(buf, offsetOf(record));
            assert readCount == recordSize() : "Only read " + readCount + " / " + recordSize();
        }
        );
        buf.flip();
        return buf;
    }

    @Override
    public void writeAtBytePosition(long position, ByteBuffer buffer) {
        assert position % recordSize == 0 : "Not a record boundary: " + position;
        if (buffer.remaining() == 0) {
            buffer.flip();
        }
        quietly(() -> channel.write(buffer, position));
    }

    @Override
    public long sizeInBytes() {
        return byteSize;
//        try {
//            return channel.size();
//        } catch (IOException ex) {
//            return Exceptions.chuck(ex);
//        }
    }

    @Override
    public void close() throws Exception {
        if (channel.isOpen()) {
            channel.close();
        }
    }

    static void quietly(IORunnable r) {
        r.toNonThrowing().run();
    }

    @Override
    public void bulkSwap(int a, int b, int count) {
        if (count == 1) {
            swap(a, b);
            return;
        }
        quietly(() -> {
            int bytes = recordSize * count;
            ByteBuffer buf1 = buffers.allocate(bytes);
            long offset1 = offsetOf(a);
            channel.read(buf1, offset1);
            buf1.flip();

            ByteBuffer buf2 = buffers.allocate(bytes);
            long offset2 = offsetOf(b);
            channel.read(buf2, offset2);
            buf2.flip();

            channel.write(buf1, offset2);
            channel.write(buf2, offset1);
        });
    }

    @Override
    public void swap(int a, int b) {
        if (a == b) {
            return;
        }
        quietly(() -> {
            ByteBuffer buf1 = buffers.get(false);
            long offset1 = offsetOf(a);
            channel.read(buf1, offset1);
            buf1.flip();
            ByteBuffer buf2 = buffers.get(true);
            long offset2 = offsetOf(b);
            channel.read(buf2, offset2);
            buf2.flip();
            channel.write(buf1, offset2);
            channel.write(buf2, offset1);
        });
    }
}
