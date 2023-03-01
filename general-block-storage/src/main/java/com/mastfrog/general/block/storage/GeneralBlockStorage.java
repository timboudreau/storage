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
package com.mastfrog.general.block.storage;

import com.mastfrog.concurrent.lock.SlottedLock;
import com.mastfrog.function.throwing.io.IOFunction;
import com.mastfrog.storage.Storage;
import com.mastfrog.storage.StorageSpecification;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.BitSet;

/**
 * A way to use a Storage implementation as a simple block memory manager with a
 * block size using the record size, region locking to avoid needing a giant IO
 * lock over the entire storage. The lock is a 64-bit SlottedLock which divides
 * the storage into 64 independently lockable regions of the multiple of the
 * block size and the blocksPerSlot variable passed to the constructor.
 *
 * @author Tim Boudreau
 */
public class GeneralBlockStorage {

    private final Storage storage;
    private final SlottedLock lock = SlottedLock.create();
    private final int blocksPerSlot;

    public GeneralBlockStorage(FileChannel channel, StorageSpecification spec, int blocksPerSlot) throws IOException {
        storage = Storage.create(channel, spec);
        this.blocksPerSlot = blocksPerSlot;
    }

    public int bytesPerBlock() {
        return storage.recordSize();
    }

    public int bytesPerLockableSlot() {
        return bytesPerBlock() * blocksPerSlot;
    }

    private BitSet slotsForByteRange(int start, int length) {
        int len = bytesPerLockableSlot();
        int first = start / len;
        int slotCount = (length / len) + 1;
        int last = first + slotCount;
        BitSet result = new BitSet(64);
        for (int i = first; i <= last; i++) {
            result.set(i);
        }
        return result;
    }

    public <T> T enterByteRange(int startByte, int lengthInBytes, IOFunction<Storage, T> io)
            throws IOException, InterruptedException {
        return lock.getLockingReentrantlyIO(() -> {
            return io.apply(storage);
        }, slotsForByteRange(startByte, lengthInBytes));
    }
}
