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
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.scenario.Scenario;
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
public class KnnBench extends Scenario {

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

        Collections.shuffle( queryList );

        // This dumps the sql queries independent of the selected interface
        if ( outputDirectory != null && dumpQueryList ) {
            log.info( "Dump query list..." );
            try {
                FileWriter fw = new FileWriter( outputDirectory.getPath() + File.separator + "queryList" );
                queryList.forEach( query -> {
                    try {
                        fw.append( query.query.getSql() ).append( "\n" );
                    } catch ( IOException e ) {
                        log.error( "Error while dumping query list", e );
                    }
                } );
                fw.close();
            } catch ( IOException e ) {
                log.error( "Error while dumping query list", e );
            }
        }

        log.info( "Executing benchmark..." );
        (new Thread( new ProgressReporter.ReportQueryListProgress( queryList, progressReporter ) )).start();
        long startTime = System.nanoTime();

        ArrayList<EvaluationThread> threads = new ArrayList<>();
        for ( int i = 0; i < numberOfThreads; i++ ) {
            threads.add( new EvaluationThread( queryList, executorFactory.createExecutorInstance( csvWriter ) ) );
        }

        EvaluationThreadMonitor threadMonitor = new EvaluationThreadMonitor( threads );
        threads.forEach( t -> t.setThreadMonitor( threadMonitor ) );

        for ( EvaluationThread thread : threads ) {
            thread.start();
        }

        for ( Thread thread : threads ) {
            try {
                thread.join();
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }
        }

        executeRuntime = System.nanoTime() - startTime;

        for ( EvaluationThread thread : threads ) {
            thread.closeExecutor();
        }

        if ( threadMonitor.aborted ) {
            throw new RuntimeException( "Exception while executing benchmark", threadMonitor.exception );
        }

        log.info( "run time: {} s", executeRuntime / 1000000000 );

        return executeRuntime;
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


    private class EvaluationThread extends Thread {

        private final Executor executor;
        private final List<QueryListEntry> theQueryList;
        private boolean abort = false;
        @Setter
        private EvaluationThreadMonitor threadMonitor;


        EvaluationThread( List<QueryListEntry> queryList, Executor executor ) {
            super( "EvaluationThread" );
            this.executor = executor;
            theQueryList = queryList;
        }


        @Override
        public void run() {
            long measuredTimeStart;
            long measuredTime;
            QueryListEntry queryListEntry;

            while ( !theQueryList.isEmpty() && !abort ) {
                measuredTimeStart = System.nanoTime();
                try {
                    queryListEntry = theQueryList.remove( 0 );
                } catch ( IndexOutOfBoundsException e ) { // This is neither nice nor efficient...
                    // This can happen due to concurrency if two threads enter the while-loop and there is only one thread left
                    // Simply leaf the loop
                    break;
                }
                try {
                    executor.executeQuery( queryListEntry.query );
                } catch ( ExecutorException e ) {
                    log.error( "Caught exception while executing queries", e );
                    threadMonitor.notifyAboutError( e );
                    try {
                        executor.executeRollback();
                    } catch ( ExecutorException ex ) {
                        log.error( "Error while rollback", e );
                    }
                    throw new RuntimeException( e );
                }
                measuredTime = System.nanoTime() - measuredTimeStart;
                measuredTimes.add( measuredTime );
                measuredTimePerQueryType.get( queryListEntry.templateId ).add( measuredTime );
                if ( commitAfterEveryQuery ) {
                    try {
                        executor.executeCommit();
                    } catch ( ExecutorException e ) {
                        log.error( "Caught exception while committing", e );
                        threadMonitor.notifyAboutError( e );
                        try {
                            executor.executeRollback();
                        } catch ( ExecutorException ex ) {
                            log.error( "Error while rollback", e );
                        }
                        throw new RuntimeException( e );
                    }
                }
            }

            try {
                executor.executeCommit();
            } catch ( ExecutorException e ) {
                log.error( "Caught exception while committing", e );
                threadMonitor.notifyAboutError( e );
                try {
                    executor.executeRollback();
                } catch ( ExecutorException ex ) {
                    log.error( "Error while rollback", e );
                }
                throw new RuntimeException( e );
            }

            executor.flushCsvWriter();
        }


        public void abort() {
            this.abort = true;
        }


        public void closeExecutor() {
            commitAndCloseExecutor( executor );
        }

    }


    private class EvaluationThreadMonitor {

        private final List<EvaluationThread> threads;
        @Getter
        private Exception exception;
        @Getter
        private boolean aborted;


        public EvaluationThreadMonitor( List<EvaluationThread> threads ) {
            this.threads = threads;
            this.aborted = false;
        }


        public void abortAll() {
            this.aborted = true;
            threads.forEach( EvaluationThread::abort );
        }


        public void notifyAboutError( Exception e ) {
            exception = e;
            abortAll();
        }

    }


    @Override
    public void analyze( Properties properties, File outputDirectory ) {
        properties.put( "measuredTime", calculateMean( measuredTimes ) );

        measuredTimePerQueryType.forEach( ( templateId, time ) -> {
            calculateResults( queryTypes, properties, templateId, time );
        } );
        properties.put( "queryTypes_maxId", queryTypes.size() );
        properties.put( "executeRuntime", executeRuntime / 1000000000.0 );
        properties.put( "numberOfQueries", measuredTimes.size() );
        properties.put( "throughput", measuredTimes.size() / (executeRuntime / 1000000000.0) );
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
