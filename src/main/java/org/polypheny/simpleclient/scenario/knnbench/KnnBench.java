/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2023 The Polypheny Project
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

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
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
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.CreateIntFeature;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.CreateMetadata;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.CreateRealFeature;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.MetadataKnnIntFeature;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.MetadataKnnRealFeature;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.SimpleKnnIdRealFeature;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.SimpleKnnIntFeature;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.SimpleKnnRealFeature;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.SimpleMetadata;


@Slf4j
public class KnnBench extends PolyphenyScenario {

    private final KnnBenchConfig config;

    private final List<Long> measuredTimes;
    private long executeRuntime;
    private final Map<Integer, String> queryTypes;
    private final Map<Integer, List<Long>> measuredTimePerQueryType;


    public KnnBench( Executor.ExecutorFactory executorFactory, KnnBenchConfig config, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList, QueryMode.TABLE );
        this.config = config;

        measuredTimes = Collections.synchronizedList( new LinkedList<>() );
        queryTypes = new HashMap<>();
        measuredTimePerQueryType = new ConcurrentHashMap<>();
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

        log.info( "Creating schema..." );
        Executor executor = null;
        try {
            executor = executorFactory.createExecutorInstance();
            executor.executeQuery( (new CreateMetadata( findMatchingDataStoreName( config.dataStoreMetadata ) )).getNewQuery() );
            executor.executeQuery( (new CreateIntFeature( findMatchingDataStoreName( config.dataStoreFeature ), config.dimensionFeatureVectors )).getNewQuery() );
            executor.executeQuery( (new CreateRealFeature( findMatchingDataStoreName( config.dataStoreFeature ), config.dimensionFeatureVectors )).getNewQuery() );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    public void generateData( DatabaseInstance databaseInstance, ProgressReporter progressReporter ) {
        log.info( "Generating data..." );
        Executor executor1 = executorFactory.createExecutorInstance();
        DataGenerator dataGenerator = new DataGenerator( executor1, config, progressReporter );

        try {
            dataGenerator.generateMetadata();
            dataGenerator.generateIntFeatures();
            dataGenerator.generateRealFeatures();
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while generating data", e );
        } finally {
            commitAndCloseExecutor( executor1 );
        }
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        log.info( "Preparing query list for the benchmark..." );
        List<QueryListEntry> queryList = new Vector<>();
        addNumberOfTimes( queryList, new SimpleKnnIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfSimpleKnnIntFeatureQueries );
        addNumberOfTimes( queryList, new SimpleKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfSimpleKnnRealFeatureQueries );
        addNumberOfTimes( queryList, new SimpleMetadata( config.randomSeedQuery, config.numberOfEntries ), config.numberOfSimpleMetadataQueries );
//        addNumberOfTimes( queryList, new SimpleKnnIdIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfSimpleKnnIdIntFeatureQueries );
        addNumberOfTimes( queryList, new SimpleKnnIdRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfSimpleKnnIdRealFeatureQueries );
        addNumberOfTimes( queryList, new MetadataKnnIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfMetadataKnnIntFeatureQueries );
        addNumberOfTimes( queryList, new MetadataKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm ), config.numberOfMetadataKnnRealFeatureQueries );

        return commonExecute( queryList, progressReporter, outputDirectory, numberOfThreads, Query::getSql, () -> executorFactory.createExecutorInstance( csvWriter ), new Random() );
    }


    @Override
    public void warmUp( ProgressReporter progressReporter ) {
        log.info( "Warm-up..." );

        Executor executor = null;
        SimpleKnnIntFeature simpleKnnIntFeatureBuilder = new SimpleKnnIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        SimpleKnnRealFeature simpleKnnRealFeatureBuilder = new SimpleKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        SimpleMetadata simpleMetadataBuilder = new SimpleMetadata( config.randomSeedQuery, config.numberOfEntries );
//        SimpleKnnIdIntFeature simpleKnnIdIntFeatureBuilder = new SimpleKnnIdIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        SimpleKnnIdRealFeature simpleKnnIdRealFeatureBuilder = new SimpleKnnIdRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        MetadataKnnIntFeature metadataKnnIntFeature = new MetadataKnnIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        MetadataKnnRealFeature metadataKnnRealFeature = new MetadataKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );

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

//                if ( config.numberOfSimpleKnnIdIntFeatureQueries > 0 ) {
//                    executor.executeQuery( simpleKnnIdIntFeatureBuilder.getNewQuery() );
//                }
                if ( config.numberOfSimpleKnnIdRealFeatureQueries > 0 ) {
                    executor.executeQuery( simpleKnnIdRealFeatureBuilder.getNewQuery() );
                }
                if ( config.numberOfMetadataKnnIntFeatureQueries > 0 ) {
                    executor.executeQuery( metadataKnnIntFeature.getNewQuery() );
                }
                if ( config.numberOfMetadataKnnRealFeatureQueries > 0 ) {
                    executor.executeQuery( metadataKnnRealFeature.getNewQuery() );
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

}
