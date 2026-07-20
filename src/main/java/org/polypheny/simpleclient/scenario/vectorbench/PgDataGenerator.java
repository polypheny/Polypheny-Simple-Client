/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2026 The Polypheny Project
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

package org.polypheny.simpleclient.scenario.vectorbench;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.postgres.dml.PgInsertBooleanFeature;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.postgres.dml.PgInsertRealFeature;
import java.util.LinkedList;
import java.util.List;


@Slf4j
public class PgDataGenerator {

    private final Executor executor;
    private final VectorBenchConfig config;
    private final ProgressReporter progressReporter;
    private final List<BatchableInsert> batch;
    private boolean aborted;


    PgDataGenerator( Executor executor, VectorBenchConfig config, ProgressReporter progressReporter ) {
        this.executor = executor;
        this.config = config;
        this.progressReporter = progressReporter;
        this.batch = new LinkedList<>();
        this.aborted = false;
    }


    void generateRealFeatures() throws ExecutorException {
        PgInsertRealFeature builder = new PgInsertRealFeature( config.randomSeedInsert, config.dimensionFeatureVectors );
        for ( int i = 0; i < config.numberOfEntries; i++ ) {
            if ( aborted ) break;
            addToBatch( builder.getNewQuery() );
            progressReporter.update( 1 );
        }
        flushBatch();
    }


    void generateBooleanFeatures() throws ExecutorException {
        PgInsertBooleanFeature builder = new PgInsertBooleanFeature( config.randomSeedInsert, config.dimensionFeatureVectors );
        for ( int i = 0; i < config.numberOfEntries; i++ ) {
            if ( aborted ) break;
            addToBatch( builder.getNewQuery() );
            progressReporter.update( 1 );
        }
        flushBatch();
    }


    private void addToBatch( BatchableInsert query ) throws ExecutorException {
        batch.add( query );
        if ( batch.size() >= config.batchSizeInserts ) {
            flushBatch();
        }
    }


    private void flushBatch() throws ExecutorException {
        if ( batch.isEmpty() ) return;
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for ( BatchableInsert insert : batch ) {
            if ( first ) {
                sb.append( insert.getSql() );
                first = false;
            } else {
                sb.append( "," ).append( insert.getSqlRowExpression() );
            }
        }
        executor.executeQuery( RawQuery.builder().sql( sb.toString() ).expectResultSet( false ).build() );
        executor.executeCommit();
        batch.clear();
    }



    public void abort() {
        aborted = true;
    }
}

