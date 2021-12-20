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

import static com.mastfrog.util.preconditions.Checks.greaterThanZero;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

/**
 * Starts out using FileChannelStorage, which is slow, but does not use
 * a great deal of memory; if used heavily more than a threshold number of
 * times in a second, flips to using mapped storage;  a background thread
 * should periodically call <code>maybeFlipBack()</code>, which, if this
 * storage has become disused, will flip it back - to CachingFileChannelStorage,
 * which has some of the speed benefits (since this storage has been heavily used
 * at one point, it may be again); if heavy use is encountered, will flip back
 * to memory mapping.
 *
 * @author Tim Boudreau
 */
public class AdaptiveStorage implements Storage {

    private final AtomicReference<Storage> storage;
    private final int recordSize;
    private final MapMode mapMode;
    private final Stats stats = new Stats(128);
    private final IntFunction<Buffers> buffersFactory;
    private boolean memLimited;
    private FileChannel channel;
    private final boolean preferDirectBuffers;
    private final long byteSize;

    AdaptiveStorage(int recordSize, FileChannel channel, boolean readWrite, 
            boolean preferMapped, int flipThreshold, boolean writable,
            IntFunction<Buffers> buffersFactory, boolean preferDirectBuffers) throws IOException {
        if (readWrite) {
            mapMode = MapMode.READ_WRITE;
        } else {
            mapMode = MapMode.READ_ONLY;
        }
        this.channel = channel;
        this.recordSize = greaterThanZero("recordSize", recordSize);
        this.buffersFactory = buffersFactory;
        this.preferDirectBuffers = preferDirectBuffers;
        Storage initial;
        if (preferMapped) {
            try {
                initial = createMappedStorage(channel, recordSize, preferDirectBuffers, buffersFactory);
            } catch (OutOfMemoryError e) {
                e.printStackTrace();
                memLimited = true;
                initial = new FileChannelStorage(channel, recordSize, buffersFactory);
            }
        } else {
            initial = new FileChannelStorage(channel, recordSize, buffersFactory);
        }
        storage = new AtomicReference<>(initial);
        byteSize = channel.size();
    }

    public Storage createMappedStorage(FileChannel channel1, int recordSize1, boolean preferDirectBuffers, IntFunction<Buffers> buffersFactory) throws IOException {
        Storage initial;
        if (channel1.size() < TWO_GB) {
            initial = new SingleMappedStorage(recordSize1, channel1.map(mapMode, 0, channel1.size()), preferDirectBuffers, buffersFactory);
        } else {
            initial = new MultiMappedStorage(recordSize1, channel1, mapMode, preferDirectBuffers, buffersFactory);
        }
        return initial;
    }

    private Storage delegate() {
        touch();
        return storage.get();
    }

    @Override
    public int recordSize() {
        return recordSize;
    }

    @Override
    public ByteBuffer read(int record) {
        return delegate().read(record);
    }

    @Override
    public ByteBuffer forIndex(long index) {
        return delegate().forIndex(index);
    }

    @Override
    public void writeAtBytePosition(long bytes, ByteBuffer buffer) {
        delegate().writeAtBytePosition(bytes, buffer);
    }

    @Override
    public long sizeInBytes() {
        return byteSize;
//        try {
//            return channel.size();
//        } catch (IOException ex) {
//            ex.printStackTrace();
//            return 0;
//        }
    }

    @Override
    public void swap(int index1, int index2) {
        delegate().swap(index1, index2);
    }

    @Override
    public void bulkSwap(int index1, int index2, int count) {
        delegate().bulkSwap(index1, index2, count);
    }

    void maybeFlipBack() {
        if (stats.isUntouched()) {
            storage.getAndUpdate(old -> {
                if (!(old instanceof FileChannelStorage)) {
                    return new CachingFileChannelStorage(channel, recordSize, 64, preferDirectBuffers);
                }
                return old;
            });
        }
    }

    void touch() {
        Storage sto = storage.get();
        if (!memLimited && stats.touched() && sto instanceof FileChannelStorage || sto instanceof CachingFileChannelStorage) {
            storage.getAndUpdate(old -> {
                if (old instanceof FileChannelStorage) {
                    try {
                        System.out.println("FLIP TO MAPPED");
                        return createMappedStorage(channel, recordSize, preferDirectBuffers, buffersFactory);
                    } catch (Exception | Error ex) {
                        ex.printStackTrace();
                        memLimited = true;
                    }
                }
                return old;
            });
        }
    }
}
