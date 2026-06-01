/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-5/26/26, 5:48 PM The Polypheny Project
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
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.PolyphenyScenario;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.postgres.ddl.PgCreateRealFeature;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.postgres.ddl.PgCreateRealFeatureIndex;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.postgres.dql.PgSimpleKnnRealFeature;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.postgres.dql.PgSimpleKnnRealFeatureFiltered;
import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Vector;


@Slf4j
public class PgVectorBench extends PolyphenyScenario {

    private final VectorBenchConfig config;


    public PgVectorBench( Executor.ExecutorFactory executorFactory, VectorBenchConfig config,
            boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList, QueryMode.TABLE );
        this.config = config;
    }


    @Override
    public void createSchema( DatabaseInstance databaseInstance, boolean includingKeys ) {
        log.info( "Creating schema..." );
        Executor executor = null;
        try {
            executor = executorFactory.createExecutorInstance();
            executor.executeQuery( new RawQuery( "CREATE EXTENSION IF NOT EXISTS vector", null, false ) );
            executor.executeQuery( new PgCreateRealFeature( config.dimensionFeatureVectors ).getNewQuery() );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    public void createIndex() {
        if ( !config.useIndex ) {
            return;
        }
       Executor executor = null;
        try {
            executor = executorFactory.createExecutorInstance();
            long start = System.nanoTime();
            executor.executeQuery( new PgCreateRealFeatureIndex( config.indexMethod, config.distanceNorm, config.indexM, config.indexEfConstruction, config.indexLists ).getNewQuery() );
            executor.executeCommit();
            long durationMillis = ( System.nanoTime() - start ) / 1_000_000L;
            log.info( "Vector index built in {} ms", durationMillis );

              String conf = config.indexMethod.equals( "hnsw" )
                    ? "hnsw.ef_search = " + config.queryEfSearch
                    : "ivfflat.probes = " + config.queryProbes;
            executor.executeQuery( new RawQuery( "ALTER DATABASE postgres SET " + conf, null, false ) );
            executor.executeCommit();
            log.info( "Query-time index parameter set: {}", conf );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating vector index", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    public void generateData( DatabaseInstance databaseInstance, ProgressReporter progressReporter ) {
        log.info( "Generating data..." );
        Executor executor = executorFactory.createExecutorInstance();
        PgDataGenerator dataGenerator = new PgDataGenerator( executor, config, progressReporter );
        try {
            dataGenerator.generateRealFeatures();
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while generating data", e );
        } finally {
            commitAndCloseExecutor( executor );
        }

        // Build the index for the benchmark run (Chronos has no separate index task).
        if ( databaseInstance != null && config.useIndex ) {
            createIndex();
        }
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        log.info( "Preparing query list..." );
        List<QueryListEntry> queryList = new Vector<>();
        addNumberOfTimes( queryList, new PgSimpleKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfSimpleKnnRealFeatureQueries );
        addNumberOfTimes( queryList, new PgSimpleKnnRealFeatureFiltered( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm, "cat_A" ), config.numberOfSimpleKnnRealFeatureFilteredQueries );
        return commonExecute( queryList, progressReporter, outputDirectory, numberOfThreads,
                Query::getSql, () -> executorFactory.createExecutorInstance( csvWriter ), new Random() );
    }


    @Override
    public void warmUp( ProgressReporter progressReporter ) {
        log.info( "Warm-up..." );
        PgSimpleKnnRealFeature knnBuilder = new PgSimpleKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        Executor executor = null;
        for ( int i = 0; i < config.numberOfWarmUpIterations; i++ ) {
            try {
                executor = executorFactory.createExecutorInstance();
                if ( config.numberOfSimpleKnnRealFeatureQueries > 0 ) {
                    executor.executeQuery( knnBuilder.getNewQuery() );
                }
            } catch ( ExecutorException e ) {
                throw new RuntimeException( "Error during warm-up", e );
            } finally {
                commitAndCloseExecutor( executor );
            }
            try {
                Thread.sleep( 10000 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Interrupted during warm-up", e );
            }
        }
    }


    @Override
    public int getNumberOfInsertThreads() {
        return 1;
    }


    private void addNumberOfTimes( List<QueryListEntry> list, QueryBuilder builder, int count ) {
        int id = queryTypes.size() + 1;
        queryTypes.put( id, builder.getNewQuery().getSql() );
        measuredTimePerQueryType.put( id, Collections.synchronizedList( new LinkedList<>() ) );
        for ( int i = 0; i < count; i++ ) {
            list.add( new QueryListEntry( builder.getNewQuery(), id ) );
        }
    }
}

