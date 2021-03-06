package org.polypheny.simpleclient.scenario.knnbench;


import com.google.common.base.Joiner;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.ChronosAgent;
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
    private final Map<Integer, String> queryTypes;
    private final Map<Integer, List<Long>> measuredTimePerQueryType;


    public KnnBench( Executor.ExecutorFactory executorFactory, KnnBenchConfig config, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList );
        this.config = config;

        measuredTimes = Collections.synchronizedList( new LinkedList<>() );
        queryTypes = new HashMap<>();
        measuredTimePerQueryType = new ConcurrentHashMap<>();
    }


    @Override
    public void createSchema( boolean includingKeys ) {
        log.info( "Creating schema..." );
        Executor executor = null;

        try {
            executor = executorFactory.createExecutorInstance();
            executor.executeQuery( (new CreateMetadata( config.dataStoreMetadata )).getNewQuery() );
            executor.executeQuery( (new CreateIntFeature( config.dataStoreFeature, config.dimensionFeatureVectors )).getNewQuery() );
            executor.executeQuery( (new CreateRealFeature( config.dataStoreFeature, config.dimensionFeatureVectors )).getNewQuery() );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    public void generateData( ProgressReporter progressReporter ) {
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

        long runTime = System.nanoTime() - startTime;

        for ( EvaluationThread thread : threads ) {
            thread.closeExecutor();
        }

        if ( threadMonitor.aborted ) {
            throw new RuntimeException( "Exception while executing benchmark", threadMonitor.exception );
        }

        log.info( "run time: {} s", runTime / 1000000000 );

        return runTime;
    }


    @Override
    public void warmUp( ProgressReporter progressReporter, int iterations ) {
        log.info( "Warm-up..." );

        Executor executor = null;
        SimpleKnnIntFeature simpleKnnIntFeatureBuilder = new SimpleKnnIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        SimpleKnnRealFeature simpleKnnRealFeatureBuilder = new SimpleKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        SimpleMetadata simpleMetadataBuilder = new SimpleMetadata( config.randomSeedQuery, config.numberOfEntries );
//        SimpleKnnIdIntFeature simpleKnnIdIntFeatureBuilder = new SimpleKnnIdIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        SimpleKnnIdRealFeature simpleKnnIdRealFeatureBuilder = new SimpleKnnIdRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        MetadataKnnIntFeature metadataKnnIntFeature = new MetadataKnnIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        MetadataKnnRealFeature metadataKnnRealFeature = new MetadataKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );

        for ( int i = 0; i < iterations; i++ ) {
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
    public void analyze( Properties properties ) {
        properties.put( "measuredTime", calculateMean( measuredTimes ) );

        measuredTimePerQueryType.forEach( ( templateId, time ) -> {
            calculateResults( properties, templateId, time );
        } );
        properties.put( "queryTypes_maxId", queryTypes.size() );
    }


    private void calculateResults( Properties properties, int templateId, List<Long> time ) {
        LongSummaryStatistics summaryStatistics = time.stream().mapToLong( Long::longValue ).summaryStatistics();
        double mean = summaryStatistics.getAverage();
        long max = summaryStatistics.getMax();
        long min = summaryStatistics.getMin();
        double stddev = calculateSampleStandardDeviation( time, mean );

        properties.put( "queryTypes_" + templateId + "_mean", processDoubleValue( mean ) );
        if ( ChronosAgent.STORE_INDIVIDUAL_QUERY_TIMES ) {
            properties.put( "queryTypes_" + templateId + "_all", Joiner.on( ',' ).join( time ) );
        }
        properties.put( "queryTypes_" + templateId + "_stddev", processDoubleValue( stddev ) );
        properties.put( "queryTypes_" + templateId + "_min", min / 1_000_000L );
        properties.put( "queryTypes_" + templateId + "_max", max / 1_000_000L );
        properties.put( "queryTypes_" + templateId + "_example", queryTypes.get( templateId ) );
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


    private void commitAndCloseExecutor( Executor executor ) {
        if ( executor != null ) {
            try {
                executor.executeCommit();
            } catch ( ExecutorException e ) {
                try {
                    executor.executeRollback();
                } catch ( ExecutorException ex ) {
                    log.error( "Error while rollback connection", e );
                }
            }
            try {
                executor.closeConnection();
            } catch ( ExecutorException e ) {
                log.error( "Error while closing connection", e );
            }
        }
    }

}
