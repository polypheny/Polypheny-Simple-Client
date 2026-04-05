/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 The Polypheny Project
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
 *
 */

package org.polypheny.simpleclient.scenario.knnbench;

import java.util.LinkedList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.InsertIntFeature;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.InsertMetadata;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.InsertRealFeature;


@Slf4j
public class DataGenerator {

    private final Executor theExecutor;
    private final KnnBenchConfig config;
    private final ProgressReporter progressReporter;

    private final List<BatchableInsert> batchList;

    private boolean aborted;


    DataGenerator( Executor executor, KnnBenchConfig config, ProgressReporter progressReporter ) {
        theExecutor = executor;
        this.config = config;
        this.progressReporter = progressReporter;
        batchList = new LinkedList<>();

        aborted = false;
    }


    void generateMetadata() throws ExecutorException {
        InsertMetadata queryBuilder = new InsertMetadata();
        for ( int i = 0; i < config.numberOfEntries; i++ ) {
            if ( aborted ) {
                break;
            }

            addToInsertList( queryBuilder.getNewQuery() );
        }
        executeInsertList();
    }


    void generateIntFeatures() throws ExecutorException {
        InsertIntFeature queryBuilder = new InsertIntFeature( config.randomSeedInsert, config.dimensionFeatureVectors );
        for ( int i = 0; i < config.numberOfEntries; i++ ) {
            if ( aborted ) {
                break;
            }

            addToInsertList( queryBuilder.getNewQuery() );
        }
        executeInsertList();
    }


    void generateRealFeatures() throws ExecutorException {
        InsertRealFeature queryBuilder = new InsertRealFeature( config.randomSeedInsert, config.dimensionFeatureVectors );
        for ( int i = 0; i < config.numberOfEntries; i++ ) {
            if ( aborted ) {
                break;
            }

            addToInsertList( queryBuilder.getNewQuery() );
        }
        executeInsertList();
    }


    private void addToInsertList( BatchableInsert query ) throws ExecutorException {
        batchList.add( query );
        if ( batchList.size() >= config.batchSizeInserts ) {
            executeInsertList();
        }
    }


    private void executeInsertList() throws ExecutorException {
        theExecutor.executeInsertList( batchList, config );
        theExecutor.executeCommit();
        batchList.clear();
    }


    public void abort() {
        aborted = true;
    }

}
