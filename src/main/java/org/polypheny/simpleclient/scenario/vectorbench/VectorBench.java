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

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
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
import org.polypheny.simpleclient.scenario.PolyphenyScenario;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dql.SimpleKnnBooleanFeature;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dql.SimpleKnnBooleanFeatureFiltered;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dql.SimpleKnnIdIntFeature;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dql.SimpleKnnRealFeatureFiltered;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.ddl.CreateBooleanFeature;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.ddl.CreateIntFeature;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.ddl.CreateBooleanFeatureIndex;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.ddl.CreateMetadata;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.ddl.CreateRealFeature;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.ddl.CreateRealFeatureIndex;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dql.MetadataKnnIntFeature;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dql.MetadataKnnRealCrossJoin;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dql.MetadataKnnRealFeature;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dql.SimpleKnnIdRealFeature;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dql.SimpleKnnIntFeature;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dql.SimpleKnnRealCrossJoin;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dql.SimpleKnnRealFeature;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dql.SimpleMetadata;


@Slf4j
public class VectorBench extends PolyphenyScenario {

    private final VectorBenchConfig config;
    private String featureStore;
    private String metadataStore;

    public VectorBench(Executor.ExecutorFactory executorFactory, VectorBenchConfig config, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList, QueryMode.TABLE );
        this.config = config;
        this.featureStore = config.dataStoreFeature;
        this.metadataStore = config.dataStoreMetadata;

    }


    @Override
    public void createSchema( DatabaseInstance databaseInstance, boolean includingKeys ) {
        if ( queryMode != QueryMode.TABLE ) {
            throw new UnsupportedOperationException( "Unsupported query mode: " + queryMode.name() );
        }

        if ( config.newTablePlacementStrategy.equalsIgnoreCase( "Optimized" ) && config.dataStores.size() > 1 ) {
            if ( config.dataStoreMetadata == null ) {
                throw new RuntimeException( "Optimized placements is selected but 'dataStoreMetadata' is null!" );
            }
            if ( config.dataStoreFeature == null ) {
                throw new RuntimeException( "Optimized placements is selected but 'dataStoreFeature' is null!" );
            }
        }

        resolveStores( databaseInstance );

        log.info( "Creating schema..." );
        Executor executor = null;
        try {
            executor = executorFactory.createExecutorInstance();
            executor.executeQuery( (new CreateMetadata(  metadataStore  )).getNewQuery() );
            executor.executeQuery( (new CreateIntFeature(  featureStore , config.dimensionFeatureVectors, config.supportsNotNullArray )).getNewQuery() );
            executor.executeQuery( (new CreateRealFeature(  featureStore , config.dimensionFeatureVectors, config.supportsNotNullArray )).getNewQuery() );
            executor.executeQuery( (new CreateBooleanFeature( featureStore, config.dimensionFeatureVectors, config.supportsNotNullArray )).getNewQuery() );
        } catch (ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    public void createIndex() {
        if ( !config.useIndex ) {
            return;
        }
        // Polypheny vector-maps array columns only when declared with element NOT NULL (REAL NOT NULL ARRAY).
        // A plain ARRAY column is not vector-mapped, so no vector index can be built.
        if ( !config.supportsNotNullArray ) {
            log.info( "Skipping index creation: plain ARRAY columns are not vector-mapped in Polypheny (requires NOT NULL ARRAY)." );
            return;
        }
       Executor executor = null;
        try {
            executor = executorFactory.createExecutorInstance();
            long start = System.nanoTime();
            if ( indexSupportsMetric( config.indexMethod, config.distanceNorm ) ) {
                executor.executeQuery( new CreateRealFeatureIndex( featureStore, config.indexMethod, config.distanceNorm, config.indexM, config.indexEfConstruction, config.indexLists ).getNewQuery() );
            } else {
                log.info( "Skipping real index: {} does not support metric '{}'.", config.indexMethod, config.distanceNorm );
            }
            if ( indexSupportsMetric( config.indexMethod, config.booleanDistanceNorm ) ) {
                executor.executeQuery( new CreateBooleanFeatureIndex( featureStore, config.indexMethod, config.booleanDistanceNorm, config.indexM, config.indexEfConstruction, config.indexLists ).getNewQuery() );
            } else {
                log.info( "Skipping boolean index: {} does not support metric '{}'.", config.indexMethod, config.booleanDistanceNorm );
            }
            executor.executeCommit();
            long durationMillis = ( System.nanoTime() - start ) / 1_000_000L;
            log.info( "Vector indexes built in {} ms", durationMillis );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating vector index", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    private static boolean indexSupportsMetric( String method, String metric ) {
        // HNSW supports all metrics, IVFFlat does not support L1 or JACCARD.
        if ( method.equalsIgnoreCase( "ivfflat" ) ) {
            return !( metric.equalsIgnoreCase( "L1" ) || metric.equalsIgnoreCase( "JACCARD" ) );
        }
        return true;
    }


    @Override
    public void generateData( DatabaseInstance databaseInstance, ProgressReporter progressReporter ) {
        log.info( "Generating data..." );
        resolveStores( databaseInstance );
        Executor executor1 = executorFactory.createExecutorInstance();
        DataGenerator dataGenerator = new DataGenerator( executor1, config, progressReporter );

        try {
            dataGenerator.generateMetadata();
            dataGenerator.generateIntFeatures();
            dataGenerator.generateRealFeatures();
            dataGenerator.generateBooleanFeatures();
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while generating data", e );
        } finally {
            commitAndCloseExecutor( executor1 );
        }

        // Build the index for the benchmark run (Chronos has no separate index task).
        if ( databaseInstance != null && config.useIndex ) {
            createIndex();
        }
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        log.info( "Preparing query list for the benchmark..." );
        List<QueryListEntry> queryList = new Vector<>();
        addNumberOfTimes( queryList, new SimpleKnnIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfSimpleKnnIntFeatureQueries );
        addNumberOfTimes( queryList, new SimpleKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfSimpleKnnRealFeatureQueries );
        addNumberOfTimes( queryList, new SimpleMetadata( config.randomSeedQuery, config.numberOfEntries ), config.numberOfSimpleMetadataQueries );
        addNumberOfTimes( queryList, new SimpleKnnIdRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfSimpleKnnIdRealFeatureQueries );
        addNumberOfTimes( queryList, new MetadataKnnIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfMetadataKnnIntFeatureQueries );
        addNumberOfTimes( queryList, new MetadataKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfMetadataKnnRealFeatureQueries );
        addNumberOfTimes( queryList, new SimpleKnnRealCrossJoin( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfSimpleKnnRealCrossJoinQueries );
        addNumberOfTimes( queryList, new SimpleKnnRealFeatureFiltered( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm, "cat_A" ), config.numberOfSimpleKnnRealFeatureFilteredQueries );
        addNumberOfTimes( queryList, new SimpleKnnBooleanFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.booleanDistanceNorm ), config.numberOfSimpleKnnBooleanFeatureQueries );
        addNumberOfTimes( queryList, new SimpleKnnBooleanFeatureFiltered( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.booleanDistanceNorm, "cat_A" ), config.numberOfSimpleKnnBooleanFeatureFilteredQueries );
        addNumberOfTimes( queryList, new MetadataKnnRealCrossJoin( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfMetadataKnnRealCrossJoinQueries );
        addNumberOfTimes( queryList, new SimpleKnnIdIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfSimpleKnnIdIntFeatureQueries );



        return commonExecute( queryList, progressReporter, outputDirectory, numberOfThreads, Query::getSql, () -> executorFactory.createExecutorInstance( csvWriter ), new Random() );
    }


    @Override
    public void warmUp( ProgressReporter progressReporter ) {
        log.info( "Warm-up..." );

        Executor executor = null;
        SimpleKnnIntFeature simpleKnnIntFeatureBuilder = new SimpleKnnIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        SimpleKnnRealFeature simpleKnnRealFeatureBuilder = new SimpleKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        SimpleMetadata simpleMetadataBuilder = new SimpleMetadata( config.randomSeedQuery, config.numberOfEntries );
        SimpleKnnIdRealFeature simpleKnnIdRealFeatureBuilder = new SimpleKnnIdRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        MetadataKnnIntFeature metadataKnnIntFeature = new MetadataKnnIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        MetadataKnnRealFeature metadataKnnRealFeature = new MetadataKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        MetadataKnnRealCrossJoin metadataKnnCrossJoin = new MetadataKnnRealCrossJoin( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        SimpleKnnRealCrossJoin simpleKnnCrossJoin = new SimpleKnnRealCrossJoin( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        SimpleKnnRealFeatureFiltered simpleKnnRealFeatureFiltered = new SimpleKnnRealFeatureFiltered( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm, "cat_A" );
        SimpleKnnBooleanFeature simpleKnnBooleanFeature = new SimpleKnnBooleanFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.booleanDistanceNorm );
        SimpleKnnBooleanFeatureFiltered simpleKnnBooleanFeatureFiltered = new SimpleKnnBooleanFeatureFiltered( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.booleanDistanceNorm, "cat_A" );
        SimpleKnnIdIntFeature simpleKnnIdIntFeature = new SimpleKnnIdIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors,config.limitKnnQueries, config.distanceNorm );

        for ( int i = 0; i < config.numberOfWarmUpIterations; i++ ) {
            try {
                executor = executorFactory.createExecutorInstance();
                if ( config.numberOfSimpleKnnIntFeatureQueries > 0 ) {
                    executor.executeQuery( simpleKnnIntFeatureBuilder.getNewQuery() );
                }
                if ( config.numberOfSimpleKnnRealFeatureQueries > 0 ) {
                    executor.executeQuery( simpleKnnRealFeatureBuilder.getNewQuery() );
                }
                if ( config.numberOfSimpleMetadataQueries > 0 ) {
                    executor.executeQuery( simpleMetadataBuilder.getNewQuery() );
                }
                if ( config.numberOfSimpleKnnIdRealFeatureQueries > 0 ) {
                    executor.executeQuery( simpleKnnIdRealFeatureBuilder.getNewQuery() );
                }
                if ( config.numberOfMetadataKnnIntFeatureQueries > 0 ) {
                    executor.executeQuery( metadataKnnIntFeature.getNewQuery() );
                }
                if ( config.numberOfSimpleKnnIdIntFeatureQueries > 0 ) {
                    executor.executeQuery( simpleKnnIdIntFeature.getNewQuery() );
                }
                if ( config.numberOfMetadataKnnRealFeatureQueries > 0 ) {
                    executor.executeQuery( metadataKnnRealFeature.getNewQuery() );
                }
                if ( config.numberOfMetadataKnnRealCrossJoinQueries > 0 ) {
                    executor.executeQuery( metadataKnnCrossJoin.getNewQuery() );
                }
                if ( config.numberOfSimpleKnnRealCrossJoinQueries > 0 ) {
                    executor.executeQuery( simpleKnnCrossJoin.getNewQuery() );
                }
                if ( config.numberOfSimpleKnnRealFeatureFilteredQueries > 0 ) {
                    executor.executeQuery( simpleKnnRealFeatureFiltered.getNewQuery() );
                }
                if ( config.numberOfSimpleKnnBooleanFeatureQueries > 0 ) {
                    executor.executeQuery( simpleKnnBooleanFeature.getNewQuery() );
                }
                if ( config.numberOfSimpleKnnBooleanFeatureFilteredQueries > 0 ) {
                    executor.executeQuery( simpleKnnBooleanFeatureFiltered.getNewQuery() );
                }
            } catch ( ExecutorException e ) {
                throw new RuntimeException( "Error while executing warm-up queries", e );
            } finally {
                commitAndCloseExecutor( executor );
            }
            try {
                Thread.sleep( 10000 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }
        }
    }


    @Override
    public int getNumberOfInsertThreads() {
        return 1;
    }


    private void addNumberOfTimes( List<QueryListEntry> list, QueryBuilder queryBuilder, int numberOfTimes ) {
        int id = queryTypes.size() + 1;
        queryTypes.put( id, queryBuilder.getNewQuery().getSql() );
        measuredTimePerQueryType.put( id, Collections.synchronizedList( new LinkedList<>() ) );
        for ( int i = 0; i < numberOfTimes; i++ ) {
            list.add( new QueryListEntry( queryBuilder.getNewQuery(), id ) );
        }
    }


    private void resolveStores( DatabaseInstance databaseInstance ) {
        if ( databaseInstance != null ) {
            featureStore = findMatchingDataStoreName( config.dataStoreFeature );
            metadataStore = findMatchingDataStoreName( config.dataStoreMetadata );
        }
    }


}
