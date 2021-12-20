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

import com.mastfrog.storage.CachingFileChannelStorage.CacheBuffers.BufferHolder;
import static com.mastfrog.util.preconditions.Checks.greaterThanZero;
import com.mastfrog.util.preconditions.Exceptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A variant on simple file storage which reads some number of records when
 * needed and tries to reuse them.
 *
 * @author Tim Boudreau
 */
public class CachingFileChannelStorage implements Storage, AutoCloseable {

    private final FileChannel channel;
    private final int recordSize;
    private final int recordsToCache;
    private final AtomicInteger mutations = new AtomicInteger();
    private final ThreadLocal<CacheBuffers> buffers;

    public CachingFileChannelStorage(FileChannel channel, int recordSize,
            int recordsToCache, boolean direct) {
        this.channel = channel;
        this.recordSize = greaterThanZero("recordSize", recordSize);
        this.recordsToCache = recordsToCache;
        buffers = ThreadLocal.withInitial(() -> new CacheBuffers(direct));
    }

    @Override
    public int recordSize() {
        return recordSize;
    }

    @Override
    public void writeAtBytePosition(long bytes, ByteBuffer buffer) {
        touch((int) (bytes / recordSize));
        try {
            channel.write(buffer, bytes);
        } catch (IOException ex) {
            Exceptions.chuck(ex);
        }
    }

    @Override
    public long sizeInBytes() {
        try {
            return channel.size();
        } catch (IOException ex) {
            return Exceptions.chuck(ex);
        }
    }

    @Override
    public void swap(int index1, int index2) {
        try {
            ByteBuffer a = read(index1);
            ByteBuffer b = read(index2);
            channel.write(a, offsetOf(index2));
            channel.write(b, offsetOf(index1));
            touch(index1, index2);
        } catch (IOException ex) {
            Exceptions.chuck(ex);
        }
    }

    @Override
    public ByteBuffer forIndex(long index) {
        return read((int) index);
    }

    @Override
    public ByteBuffer read(int record) {
        return buffers.get().get(record);
    }

    private void touch(int ix) {
//        for (BufferHolder h : all) {
//            h.invalidate(ix);
//        }
        mutations.getAndIncrement();
    }

    private void touch(int ix, int ix2) {
        mutations.getAndIncrement();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    /**
     * A pool of buffers which attempts to keep sections of the file cached.
     */
    class CacheBuffers {

        private final BufferHolder head1;
        private final BufferHolder head2;
        private final BufferHolder mid1;
        private final BufferHolder mid2;
        private final BufferHolder tail1;
        private final BufferHolder tail2;

        private int headUsages, midUsages, tailUsages;
        private final long sz;

        CacheBuffers(boolean direct) {
            sz = size();
            head1 = new BufferHolder(direct);
            head2 = new BufferHolder(direct);

            mid1 = new BufferHolder(direct);
            mid2 = new BufferHolder(direct);

            tail1 = new BufferHolder(direct);
            tail2 = new BufferHolder(direct);
        }

        ByteBuffer get(int record) {
            ByteBuffer result = null;
            int r = rng(record);
            if (r == 0 || r == 1) {
                result = head1.get(record, false);
                if (result != null) {
                    return result;
                }
                result = head2.get(record, false);
                if (result != null) {
                    return result;
                }
            }
            result = mid1.get(record, false);
            if (result != null) {
                return result;
            }
            result = mid2.get(record, false);
            if (result != null) {
                return result;
            }
            if (r == 1 || r == 2) {
                result = tail1.get(record, false);
                if (result != null) {
                    return result;
                }
                result = tail2.get(record, false);
                if (result != null) {
                    return result;
                }
            }
            BufferHolder tgt = target(record);
            return tgt.get(record, true);
        }

        int rng(int record) {
            long rangeSize = sz / 3;
            if (record >= rangeSize * 2) {
                return 2;
            } else if (record >= rangeSize) {
                return 1;
            }
            return 0;
        }

        BufferHolder target(int record) {
            switch (rng(record)) {
                case 2:
                    return endBuffer();
                case 1:
                    return midBuffer();
                case 0:
                    return headBuffer();
                default:
                    throw new AssertionError(record);
            }
        }

        BufferHolder headBuffer() {
            return ++headUsages % 2 == 0 ? head2 : head1;
        }

        BufferHolder midBuffer() {
            return ++midUsages % 2 == 0 ? mid2 : mid1;
        }

        BufferHolder endBuffer() {
            return ++tailUsages % 2 == 0 ? tail2 : tail1;
        }

        class BufferHolder {

            private final ByteBuffer buf;
            private int currentStart;
            private int currentCount;
            private int mut;

            BufferHolder(boolean direct) {
                buf = direct ? ByteBuffer.allocateDirect(recordSize * recordsToCache)
                        : ByteBuffer.allocate(recordSize * recordsToCache);
            }

            void invalidate(int buf) {
                if (currentStart >= buf && buf < currentStart + currentCount) {
                    currentStart = 0;
                    currentCount = 0;
                }
            }

            ByteBuffer get(int record, boolean load) {
                if (canSatisfy(record)) {
                    int offset = (int) (offsetOf(record) - offsetOf(currentStart));
                    return buf.slice(offset, recordSize);
                } else {
                    if (load) {
                        currentStart = record;
                        currentCount = (int) Math.min(recordsToCache, sz - record);
                        load();
                        int offset = (int) (offsetOf(record) - offsetOf(currentStart));
                        return buf.slice(offset, recordSize);
                    }
                }
                return null;
            }

            boolean canSatisfy(int record) {
                int currMut = mutations.get();
                return currMut == mut
                        && record >= currentStart
                        && record < currentStart + currentCount;
            }

            void load() {
                buf.rewind();
                buf.limit(buf.capacity());
                try {
                    channel.read(buf, offsetOf(currentStart));
                    buf.flip();
                } catch (IOException ex) {
                    Exceptions.chuck(ex);
                } finally {
                    mut = mutations.get();
                }
            }
        }
    }
}
