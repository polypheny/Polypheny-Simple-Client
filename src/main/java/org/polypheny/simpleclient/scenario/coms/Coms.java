/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/11/23, 11:17 AM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.coms;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.main.ProgressReporter.ReportQueryListProgress;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.scenario.Scenario;
import org.polypheny.simpleclient.scenario.graph.GraphBench.EvaluationThread;
import org.polypheny.simpleclient.scenario.graph.GraphBench.EvaluationThreadMonitor;

@Slf4j
public class Coms extends Scenario {

    public static final String NAMESPACE = "coms";
    public static final double EPSILLON = 0.000001;

    private final Random random;
    private final ComsConfig config;
    private final List<Long> measuredTimes;
    private final HashMap<Integer, String> queryTypes;
    private final ConcurrentHashMap<Integer, List<Long>> measuredTimePerQueryType;
    private long executeRuntime;


    public Coms( ExecutorFactory executorFactory, ComsConfig config, boolean commitAfterEveryQuery, boolean dumpQueryList, QueryMode queryMode ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList, queryMode );
        this.random = new Random( config.seed );
        this.config = config;

        this.measuredTimes = Collections.synchronizedList( new LinkedList<>() );
        this.queryTypes = new HashMap<>();
        this.measuredTimePerQueryType = new ConcurrentHashMap<>();
    }


    @Override
    public void createSchema( DatabaseInstance databaseInstance, boolean includingKeys ) {
        if ( queryMode != QueryMode.TABLE ) {
            throw new UnsupportedOperationException( "Unsupported query mode: " + queryMode.name() );
        }

        String onStore = null;
        if ( config.newTablePlacementStrategy.equalsIgnoreCase( "Optimized" ) && config.dataStores.size() > 1 ) {
            for ( String storeName : PolyphenyDbExecutor.storeNames ) {
                if ( storeName.toLowerCase().startsWith( "neo4j" ) ) {
                    onStore = storeName;
                    break;
                }
            }
            if ( onStore == null ) {
                throw new RuntimeException( "No suitable data store found for optimized placing of the GraphBench graph." );
            }
        }

        log.info( "Creating schema..." );
        Executor executor = null;
        try {
            executor = executorFactory.createExecutorInstance( null, NAMESPACE );
            SchemaGenerator generator = new SchemaGenerator();
            generator.generateSchema( config, executor, NAMESPACE, onStore );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    public void generateData( DatabaseInstance databaseInstance, ProgressReporter progressReporter ) {
        log.info( "Generating data..." );
        Executor executor = executorFactory.createExecutorInstance( null, NAMESPACE );
        DataGenerator dataGenerator = new DataGenerator();
        try {
            dataGenerator.generateData( config, executor );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while generating data", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        DataGenerator generator = new DataGenerator();
        List<Query> queries = generator.generateWorkload( config );

        log.info( "Preparing query list for the benchmark..." );
        List<QueryListEntry> relQueryList = getRelQueries( queries );
        List<QueryListEntry> docQueryList = getDocQueries( queries );
        List<QueryListEntry> graphQueryList = getGraphQueries( queries );

        dumpQueries( outputDirectory, relQueryList, q -> q.query.getSql() );
        dumpQueries( outputDirectory, docQueryList, q -> q.query.getMongoQl() );
        dumpQueries( outputDirectory, graphQueryList, q -> q.query.getCypher() );
        startEvaluation( progressReporter, csvWriter, numberOfThreads, config.threadDistribution, relQueryList, docQueryList, graphQueryList );

        log.info( "run time: {} s", executeRuntime / 1000000000 );

        return executeRuntime;
    }


    private List<QueryListEntry> getGraphQueries( List<Query> queries ) {
        return queries.stream().filter( q -> q.getCypher() != null ).map( q -> new QueryListEntry( q, 0 ) ).collect( Collectors.toList() );
    }


    private List<QueryListEntry> getDocQueries( List<Query> queries ) {
        return queries.stream().filter( q -> q.getMongoQl() != null ).map( q -> new QueryListEntry( q, 0 ) ).collect( Collectors.toList() );
    }


    private List<QueryListEntry> getRelQueries( List<Query> queries ) {
        return queries.stream().filter( q -> q.getSql() != null ).map( q -> new QueryListEntry( q, 0 ) ).collect( Collectors.toList() );
    }


    @SafeVarargs
    private final void startEvaluation( ProgressReporter progressReporter, CsvWriter csvWriter, int numberOfThreads, List<Integer> threadDistribution, List<QueryListEntry>... queryLists ) {
        log.info( "Executing benchmark..." );
        if ( threadDistribution.size() != queryLists.length ) {
            throw new RuntimeException( "ThreadDistribution needs to define an number for each data model" );
        }
        float part = ((float) numberOfThreads) / threadDistribution.stream().reduce( Integer::sum ).orElse( 1 );

        List<List<QueryListEntry>> organized = new ArrayList<>();

        int i = 0;
        float amount = 0;
        for ( int t : threadDistribution ) {
            if ( (amount + (part * t) > (1 - EPSILLON)) && (1 - amount > (part * t) / 2) ) {
                // execute in new Thread, "significantly" bigger than 1
                organized.add( queryLists[i] );
                amount += (part * t);
                amount -= 1;
            } else {
                // add to last
                List<QueryListEntry> old = organized.remove( organized.size() - 1 );

                List<QueryListEntry> list = Stream.concat( old.stream(), queryLists[i].stream() ).collect( Collectors.toList() );
                Collections.shuffle( list );
                organized.add( list );
                amount += (part * t);
            }
            i++;
        }

        List<QueryListEntry> mergedList = Arrays.stream( queryLists ).flatMap( Collection::stream ).collect( Collectors.toList() );
        (new Thread( new ReportQueryListProgress( mergedList, progressReporter ) )).start();
        long startTime = System.nanoTime();

        ArrayList<EvaluationThread> threads = new ArrayList<>();
        for ( List<QueryListEntry> queryList : queryLists ) {
            for ( int j = 0; j < numberOfThreads; j++ ) {
                threads.add( new EvaluationThread( queryList, executorFactory.createExecutorInstance( csvWriter, NAMESPACE ), commitAfterEveryQuery ) );
            }
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

        if ( threadMonitor.isAborted() ) {
            throw new RuntimeException( "Exception while executing benchmark", threadMonitor.getException() );
        }
    }


    private void dumpQueries( File outputDirectory, List<QueryListEntry> relQueryList, Function<QueryListEntry, String> dumper ) {
        // This dumps the queries independent of the selected interface
        if ( outputDirectory != null && dumpQueryList ) {
            log.info( "Dump query list..." );
            try {
                FileWriter fw = new FileWriter( outputDirectory.getPath() + File.separator + "queryList" );
                relQueryList.forEach( query -> {
                    try {
                        fw.append( dumper.apply( query ) ).append( "\n" );
                    } catch ( IOException e ) {
                        log.error( "Error while dumping query list", e );
                    }
                } );
                fw.close();
            } catch ( IOException e ) {
                log.error( "Error while dumping query list", e );
            }
        }
    }


    private void addNumberOfTimes( List<QueryListEntry> list, QueryBuilder queryBuilder, int numberOfTimes ) {
        int id = queryTypes.size() + 1;
        queryTypes.put( id, queryBuilder.getNewQuery().getSql() );
        measuredTimePerQueryType.put( id, Collections.synchronizedList( new LinkedList<>() ) );
        for ( int i = 0; i < numberOfTimes; i++ ) {
            list.add( new QueryListEntry( queryBuilder.getNewQuery(), id ) );
        }
    }


    @Override
    public void warmUp( ProgressReporter progressReporter ) {

    }


    @Override
    public void analyze( Properties properties, File outputDirectory ) {

    }


    @Override
    public int getNumberOfInsertThreads() {
        return 0;
    }

}
