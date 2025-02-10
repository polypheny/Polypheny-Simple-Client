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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.PolyphenyDbCypherExecutor;
import org.polypheny.simpleclient.executor.PolyphenyDbCypherExecutor.PolyphenyDbCypherExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.scenario.PolyphenyScenario;
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
public class GraphBench extends PolyphenyScenario {

    public static final String GRAPH_NAMESPACE = "test";
    public static boolean EXPECTED_RESULT = true;
    public static final AtomicLong idBuilder = new AtomicLong();

    private final GraphBenchConfig config;

    private long executeRuntime;
    private final Map<Integer, String> queryTypes;

    private final List<Long> measuredTimes = Collections.synchronizedList( new LinkedList<>() );

    private final Map<Integer, List<Long>> measuredTimePerQueryType = new ConcurrentHashMap<>();


    public GraphBench( Executor.ExecutorFactory executorFactory, GraphBenchConfig config, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList, QueryMode.TABLE );

        if ( !(executorFactory instanceof PolyphenyDbCypherExecutorFactory) ) {
            throw new RuntimeException( "GraphBench is only supported by the PolyphenyDbCypherExecutor!" );
        }

        this.config = config;

        queryTypes = new HashMap<>();
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
            executor = executorFactory.createExecutorInstance( null, GRAPH_NAMESPACE );
            executor.executeQuery( new CreateGraphDatabase( onStore ).getNewQuery() );
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

        return commonExecute( queryList, progressReporter, outputDirectory, numberOfThreads, Query::getCypher, () -> executorFactory.createExecutorInstance( csvWriter, GRAPH_NAMESPACE ), new Random( config.seed ) );
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
