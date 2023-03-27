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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.cli.Mode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.executor.PolyphenyDbMultiExecutorFactory.MultiExecutor;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.main.ProgressReporter.ReportMultiQueryListProgress;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.scenario.EvaluationThread;
import org.polypheny.simpleclient.scenario.Scenario;
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
    private final Mode mode;
    private final int multiplier;
    private long executeRuntime = 0;
    private PolyphenyAdapters adapters;


    public Coms( ExecutorFactory executorFactory, ComsConfig config, boolean commitAfterEveryQuery, boolean dumpQueryList, QueryMode queryMode, int multiplier ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList, queryMode );
        this.random = new Random( config.seed );
        this.config = config;
        this.mode = config.mode;
        this.multiplier = multiplier == -1 ? config.cycles : multiplier;

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

            if ( mode == Mode.POLYPHENY ) {
                log.info( "Deploying adapters..." );
                adapters = deployAdapters( (MultiExecutor) executor );
            } else {
                adapters = deployAdapters( null );
            }

            SchemaGenerator generator = new SchemaGenerator();
            generator.generateSchema( config, executor, NAMESPACE, onStore, adapters );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    private PolyphenyAdapters deployAdapters( MultiExecutor executor ) {
        if ( executor == null ) {
            return new PolyphenyAdapters( null, null, null );
        }

        try {
            executor.jdbc.setNewDeploySyntax( true );
            return new PolyphenyAdapters(
                    executor.deployAdapter( config.graphStore ),
                    executor.deployAdapter( config.docStore ),
                    executor.deployAdapter( config.relStore ) );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( e );
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
        generator.updateNetworkGenerator( config );

        if ( config.numberOfWarmUpIterations > 0 ) {
            log.warn( "Pre-Warm-Up Configuration:\n" + generator.generator.network );
        }

        for ( int i = 0; i < config.numberOfWarmUpIterations; i++ ) {
            // we advance the network to a state, which is at the point of after the warmup workload
            generator.generateWorkload();
        }

        log.warn( "Start Configuration:\n" + generator.generator.network );
        for ( int i = 0; i < multiplier; i++ ) {
            List<Query> queries = generator.generateWorkload();

            log.info( "Preparing query list for the benchmark..." );

            List<QueryListEntry> relQueries = getRelQueries( queries );
            List<QueryListEntry> docQueries = getDocQueries( queries );
            List<QueryListEntry> graphQueries = getGraphQueries( queries );

            dumpQueries( outputDirectory, relQueries, q -> q.query.getSql() );
            dumpQueries( outputDirectory, docQueries, q -> q.query.getMongoQl() );
            dumpQueries( outputDirectory, graphQueries, q -> q.query.getCypher() );

            // this could be extended to allow observations of changes over a single run

            log.info( String.format( "Starting benchmark cycle %d of %d...", i, multiplier ) );
            startEvaluation( progressReporter, csvWriter, numberOfThreads, config.threadDistribution, relQueries, docQueries, graphQueries );

        }

        log.warn( "End Configuration:\n" + generator.generator.network );
        log.info( "run time: {} s", executeRuntime / 1000000000 );

        if ( adapters != null && adapters.isSet() ) {
            tearDownAdapters( adapters, executorFactory );
        }

        return executeRuntime;
    }


    private void tearDownAdapters( PolyphenyAdapters adapters, ExecutorFactory executorFactory ) {
        PolyphenyDbExecutor executor = (PolyphenyDbExecutor) executorFactory.createExecutorInstance( null, "coms" );
        SchemaGenerator generator = new SchemaGenerator();

        try {
            generator.tearDown( "coms", executor );
            executor.dropStore( adapters.docAdapter );
            executor.dropStore( adapters.relAdapter );
            executor.dropStore( adapters.graphAdapter );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( e );
        }
    }


    private List<QueryListEntry> getGraphQueries( List<Query> queries ) {
        queryTypes.put( 1, "graph" );
        return queries.stream().filter( q -> q.getCypher() != null ).map( q -> new QueryListEntry( q, 1 ) ).collect( Collectors.toList() );
    }


    private List<QueryListEntry> getDocQueries( List<Query> queries ) {
        queryTypes.put( 2, "doc" );
        return queries.stream().filter( q -> q.getMongoQl() != null ).map( q -> new QueryListEntry( q, 2 ) ).collect( Collectors.toList() );
    }


    private List<QueryListEntry> getRelQueries( List<Query> queries ) {
        queryTypes.put( 3, "relational" );
        return queries.stream().filter( q -> q.getSql() != null ).map( q -> new QueryListEntry( q, 3 ) ).collect( Collectors.toList() );
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
                if ( organized.isEmpty() ) {
                    organized.add( new ArrayList<>() );
                }
                List<QueryListEntry> old = organized.remove( organized.size() - 1 );

                List<QueryListEntry> list = randomlyMergeInOrder( old, queryLists[i] );
                organized.add( list );
                amount += (part * t);
            }
            i++;
        }

        new Thread( new ReportMultiQueryListProgress( organized, progressReporter ) ).start();
        long startTime = System.nanoTime();

        ArrayList<EvaluationThread> threads = new ArrayList<>();
        for ( List<QueryListEntry> queryList : organized ) {
            threads.add( new EvaluationThread( queryList, executorFactory.createExecutorInstance( csvWriter, NAMESPACE ), queryTypes.keySet(), commitAfterEveryQuery ) );
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

        executeRuntime += System.nanoTime() - startTime;

        collectResultsOfThreads( threads );

        for ( EvaluationThread thread : threads ) {
            thread.closeExecutor();
        }

        if ( threadMonitor.isAborted() ) {
            throw new RuntimeException( "Exception while executing benchmark", threadMonitor.getException() );
        }
    }


    private void collectResultsOfThreads( ArrayList<EvaluationThread> threads ) {
        for ( EvaluationThread thread : threads ) {
            thread.getMeasuredTimePerQueryType().forEach( ( k, v ) -> {
                if ( !measuredTimePerQueryType.containsKey( k ) ) {
                    measuredTimePerQueryType.put( k, new ArrayList<>() );
                }
                measuredTimePerQueryType.get( k ).addAll( v );
            } );
            measuredTimes.addAll( thread.getMeasuredTimes() );
        }
    }


    @SafeVarargs
    private final List<QueryListEntry> randomlyMergeInOrder( final List<QueryListEntry>... lists ) {
        List<QueryListEntry> merged = new ArrayList<>();

        List<List<QueryListEntry>> bucket = new ArrayList<>( Arrays.asList( lists ) );

        while ( !bucket.isEmpty() ) {
            int i = random.nextInt( bucket.size() );

            if ( bucket.get( i ).isEmpty() ) {
                bucket.remove( i );
                continue;
            }

            merged.add( bucket.get( i ).remove( 0 ) );
            if ( bucket.get( i ).isEmpty() ) {
                bucket.remove( i );
            }

        }

        return merged;
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


    @Override
    public void warmUp( ProgressReporter progressReporter ) {
        log.warn( "Warm-up..." );
        DataGenerator generator = new DataGenerator();
        generator.updateNetworkGenerator( config );
        Executor executor = null;
        for ( int i = 0; i < config.numberOfWarmUpIterations; i++ ) {
            log.warn( "Iteration " + i + "..." );
            try {
                executor = executorFactory.createExecutorInstance( null, NAMESPACE );
                List<Query> queries = generator.generateWorkload();
                for ( Query query : queries ) {
                    executor.executeQuery( query );
                }
            } catch ( ExecutorException e ) {
                throw new RuntimeException( e );
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
        return 0;
    }


    @Value
    @AllArgsConstructor
    public static class PolyphenyAdapters {

        String relAdapter;
        String docAdapter;
        String graphAdapter;


        public boolean isSet() {
            return relAdapter != null || docAdapter != null || graphAdapter != null;
        }

    }

}
