/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2022 The Polypheny Project
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

package org.polypheny.simpleclient.scenario.graph;

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
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.PolyphenyDbCypherExecutor;
import org.polypheny.simpleclient.executor.PolyphenyDbCypherExecutor.PolyphenyDbCypherExecutorFactory;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.scenario.Scenario;
import org.polypheny.simpleclient.scenario.graph.queryBuilder.CountNodePropertyBuilder;
import org.polypheny.simpleclient.scenario.graph.queryBuilder.CreateGraphDatabase;
import org.polypheny.simpleclient.scenario.graph.queryBuilder.DeleteNodeBuilder;
import org.polypheny.simpleclient.scenario.graph.queryBuilder.DifferentPathsBuilder;
import org.polypheny.simpleclient.scenario.graph.queryBuilder.EdgeLabelMatchBuilder;
import org.polypheny.simpleclient.scenario.graph.queryBuilder.FindNeighborsBuilder;
import org.polypheny.simpleclient.scenario.graph.queryBuilder.NodeFilterBuilder;
import org.polypheny.simpleclient.scenario.graph.queryBuilder.RelatedInsertBuilder;
import org.polypheny.simpleclient.scenario.graph.queryBuilder.SetPropertyBuilder;
import org.polypheny.simpleclient.scenario.graph.queryBuilder.ShortestPathBuilder;
import org.polypheny.simpleclient.scenario.graph.queryBuilder.UnwindBuilder;


@Slf4j
public class GraphBench extends Scenario {

    public static final String GRAPH_NAMESPACE = "test";
    public static boolean EXPECTED_RESULT = true;
    public static final AtomicLong idBuilder = new AtomicLong();

    private final GraphBenchConfig config;

    private final List<Long> measuredTimes;
    private final Map<Integer, String> queryTypes;
    private final Map<Integer, List<Long>> measuredTimePerQueryType;


    public GraphBench( Executor.ExecutorFactory executorFactory, GraphBenchConfig config, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList, QueryMode.TABLE );

        if ( !(executorFactory instanceof PolyphenyDbCypherExecutorFactory) ) {
            throw new RuntimeException( "GraphBench is only supported by the PolyphenyDbCypherExecutor!" );
        }

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

        log.info( "Creating schema..." );
        Executor executor = null;
        try {
            executor = executorFactory.createExecutorInstance( null, GRAPH_NAMESPACE );
            executor.executeQuery( new CreateGraphDatabase().getNewQuery() );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    public void generateData( DatabaseInstance databaseInstance, ProgressReporter progressReporter ) {
        log.info( "Generating data..." );
        Executor executor1 = executorFactory.createExecutorInstance( null, GRAPH_NAMESPACE );
        assert executor1 instanceof PolyphenyDbCypherExecutor;
        DataGenerator dataGenerator = new DataGenerator( executor1, config, progressReporter );

        try {
            dataGenerator.generatePaths( config.paths, config.minPathLength, config.maxPathLength );
            dataGenerator.generateClusters( config.clusters, config.minClusterSize, config.maxClusterSize );
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
        addNumberOfTimes( queryList, new CountNodePropertyBuilder( config ), config.numberOfPropertyCountQueries );
        addNumberOfTimes( queryList, new EdgeLabelMatchBuilder( config ), config.numberOfEdgeMatchQueries );
        addNumberOfTimes( queryList, new FindNeighborsBuilder( config ), config.numberOfFindNeighborsQueries );
        addNumberOfTimes( queryList, new UnwindBuilder( config ), config.numberOfUnwindQueries );
        addNumberOfTimes( queryList, new NodeFilterBuilder( config ), config.numberOfNodeFilterQueries );
        addNumberOfTimes( queryList, new DifferentPathsBuilder( config ), config.numberOfDifferentLengthQueries );
        addNumberOfTimes( queryList, new ShortestPathBuilder( config ), config.numberOfShortestPathQueries );
        addNumberOfTimes( queryList, new SetPropertyBuilder( config ), config.numberOfSetPropertyQueries );
        addNumberOfTimes( queryList, new RelatedInsertBuilder( config ), config.numberOfInsertQueries );
        addNumberOfTimes( queryList, new DeleteNodeBuilder( config ), config.numberOfDeleteQueries );

        Collections.shuffle( queryList, new Random( config.seed ) );

        // This dumps the cypher queries independent of the selected interface
        if ( outputDirectory != null && dumpQueryList ) {
            log.info( "Dump query list..." );
            try {
                FileWriter fw = new FileWriter( outputDirectory.getPath() + File.separator + "queryList" );
                queryList.forEach( query -> {
                    try {
                        fw.append( query.query.getCypher() ).append( "\n" );
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
            threads.add( new EvaluationThread( queryList, executorFactory.createExecutorInstance( csvWriter, GRAPH_NAMESPACE ) ) );
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
    public void warmUp( ProgressReporter progressReporter ) {
        log.info( "Warm-up..." );

        Executor executor = null;
        EdgeLabelMatchBuilder edgeMatch = new EdgeLabelMatchBuilder( config );
        CountNodePropertyBuilder propertyCount = new CountNodePropertyBuilder( config );
        FindNeighborsBuilder findNeighbors = new FindNeighborsBuilder( config );
        UnwindBuilder unwind = new UnwindBuilder( config );
        NodeFilterBuilder nodeFilter = new NodeFilterBuilder( config );
        DifferentPathsBuilder differentPaths = new DifferentPathsBuilder( config );
        ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder( config );
        DeleteNodeBuilder deleteNodeBuilder = new DeleteNodeBuilder( config );
        SetPropertyBuilder setPropertyBuilder = new SetPropertyBuilder( config );
        RelatedInsertBuilder relatedInsertBuilder = new RelatedInsertBuilder( config );

        for ( int i = 0; i < config.numberOfWarmUpIterations; i++ ) {
            try {
                executor = executorFactory.createExecutorInstance( null, GRAPH_NAMESPACE );
                if ( config.numberOfEdgeMatchQueries > 0 ) {
                    executor.executeQuery( edgeMatch.getNewQuery() );
                }
                if ( config.numberOfPropertyCountQueries > 0 ) {
                    executor.executeQuery( propertyCount.getNewQuery() );
                }
                if ( config.numberOfFindNeighborsQueries > 0 ) {
                    executor.executeQuery( findNeighbors.getNewQuery() );
                }
                if ( config.numberOfUnwindQueries > 0 ) {
                    executor.executeQuery( unwind.getNewQuery() );
                }
                if ( config.numberOfNodeFilterQueries > 0 ) {
                    executor.executeQuery( nodeFilter.getNewQuery() );
                }
                if ( config.numberOfDifferentLengthQueries > 0 ) {
                    executor.executeQuery( differentPaths.getNewQuery() );
                }
                if ( config.numberOfShortestPathQueries > 0 ) {
                    executor.executeQuery( shortestPathBuilder.getNewQuery() );
                }
                if ( config.numberOfInsertQueries > 0 ) {
                    executor.executeQuery( relatedInsertBuilder.getNewQuery() );
                }
                if ( config.numberOfSetPropertyQueries > 0 ) {
                    executor.executeQuery( setPropertyBuilder.getNewQuery() );
                }
                if ( config.numberOfDeleteQueries > 0 ) {
                    executor.executeQuery( deleteNodeBuilder.getNewQuery() );
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
    }


    @Override
    public int getNumberOfInsertThreads() {
        return 1;
    }


    private void addNumberOfTimes( List<QueryListEntry> list, QueryBuilder queryBuilder, int numberOfTimes ) {
        int id = queryTypes.size() + 1;
        queryTypes.put( id, queryBuilder.getNewQuery().getCypher() );
        measuredTimePerQueryType.put( id, Collections.synchronizedList( new LinkedList<>() ) );
        for ( int i = 0; i < numberOfTimes; i++ ) {
            list.add( new QueryListEntry( queryBuilder.getNewQuery(), id ) );
        }
    }


    public synchronized static String getUniqueIdentifier() {
        return "id_" + GraphBench.idBuilder.getAndIncrement();
    }

}
