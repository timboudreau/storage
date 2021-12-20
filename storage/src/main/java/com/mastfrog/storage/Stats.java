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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 *
 * @author Tim Boudreau
 */
class Stats {

    private final AtomicInteger cursor = new AtomicInteger();
    private final AtomicLongArray stats;

    Stats(int count) {
        stats = new AtomicLongArray(count);
    }

    int countBelowThreshold(long threshold) {
        int cur = cursor.get();
        int len = stats.length();
        if (cur < len * 2) {
            return 0;
        }
        int count = 0;
        long last = stats.get(0);
        for (int i = 1; i < len; i++) {
            long next = stats.get(i);
            long diff = next - last;
            if (diff >= 0 && diff <= threshold) {
                count++;
            }
            last = next;
        }
        return count;
    }

    boolean isBelowThreshold(int count, long threshold) {
        int ct = countBelowThreshold(threshold);
        return ct >= count;
    }

    boolean isUntouched() {
        return isBelowThreshold(stats.length() / 2, 2000);
    }

    boolean touched() {
        int t = touch();
        if (t > 0 && t % stats.length() == 0) {
            return isBelowThreshold(stats.length() / 2, 1000);
        }
        return false;
    }

    int touch() {
        int len = stats.length();
        int now = Math.abs(cursor.getAndIncrement());
        int item = now % len;
        long when = System.currentTimeMillis();
        stats.getAndUpdate(item, old -> System.currentTimeMillis());
        return now;
    }

}
