/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/21/25, 7:43 PM The Polypheny Project
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.polypheny.simpleclient.scenario.lockingBench;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class NumberTracker {

    public static final int CATEGORY_COUNT = 4;

    private final LockingBenchConfig config;
    private final AtomicLong idCounter;


    public NumberTracker( LockingBenchConfig config, Mode mode ) {
        this.config = config;
        this.idCounter = switch ( mode ) {
            case SETUP -> new AtomicLong();
            case WARMUP -> new AtomicLong( (long) config.namespaceCount * config.entityCount * config.numberOfEntriesPerTable );
            case RUN -> new AtomicLong( ((long) config.namespaceCount * config.entityCount * config.numberOfEntriesPerTable) + config.numberOfWarmUpIterations );
        };
    }


    public long getNextId() {
        return idCounter.incrementAndGet();
    }


    public long getMaxId() {
        return idCounter.get();
    }


    public long getRandomId() {
        return ThreadLocalRandom.current().nextLong( getMaxId() );
    }


    public int getRandomCategoryId() {
        return ThreadLocalRandom.current().nextInt( CATEGORY_COUNT );
    }

    enum Mode {
        SETUP,
        WARMUP,
        RUN
    }

}
