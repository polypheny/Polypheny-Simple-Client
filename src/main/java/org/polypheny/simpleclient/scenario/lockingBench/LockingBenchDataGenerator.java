/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/21/25, 2:12 PM The Polypheny Project
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

import java.util.LinkedList;
import java.util.List;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.scenario.lockingBench.queryBuilder.SingleInsert;

public class LockingBenchDataGenerator {

    private final LockingBenchConfig config;
    private final Executor executor;
    private final ProgressReporter progressReporter;
    private final List<BatchableInsert> batchList;
    private final NumberTracker numberTracker;

    private boolean aborted;

    public LockingBenchDataGenerator( LockingBenchConfig config, Executor executor, ProgressReporter progressReporter, NumberTracker numberTracker ) {
        this.config = config;
        this.executor = executor;
        this.progressReporter = progressReporter;
        this.aborted = false;
        this.numberTracker = numberTracker;
        this.batchList = new LinkedList<>();
    }

    public void generateEntries( List<Entity> entities) throws ExecutorException {
        int mod = entities.size() / progressReporter.base;
        if ( mod == 0 ) {
            mod++; // Avoid division by zero
        }
        for (Entity entity : entities) {
            SingleInsert entryBuilder = new SingleInsert( entity.getFullName(), numberTracker );
            for (int i = 0; i < config.numberOfEntriesPerTable; i++) {
                if (aborted) {
                    break;
                }
                addToInsertList(entryBuilder.getNewQuery());
            }
        }
        if ( !batchList.isEmpty() ) {
            executeInsertList();
        }
    }

    private void addToInsertList( BatchableInsert query) throws ExecutorException {
        batchList.add(query);
        if (batchList.size() >= config.maxBatchSize) {
            executeInsertList();
        }
    }

    private void executeInsertList() throws ExecutorException {
        executor.executeInsertList( batchList, config );
        executor.executeCommit();
        batchList.clear();
    }


    public void abort() {
        aborted = true;
    }

}
