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

package org.polypheny.simpleclient.scenario.docbench;

import java.io.File;
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
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.PolyphenyScenario;
import org.polypheny.simpleclient.scenario.docbench.queryBuilder.PutProductQueryBuilder;
import org.polypheny.simpleclient.scenario.docbench.queryBuilder.SearchProductQueryBuilder;
import org.polypheny.simpleclient.scenario.docbench.queryBuilder.UpdateProductQueryBuilder;


@Slf4j
public class DocBench extends PolyphenyScenario {

    private final DocBenchConfig config;
    private final List<Long> measuredTimes;
    private long executeRuntime;
    private final Map<Integer, String> queryTypes;
    private final Map<Integer, List<Long>> measuredTimePerQueryType;
    private final Random random;
    public final List<String> valuesPool = new ArrayList<>();

    public static final String NAMESPACE = "docbench";


    public DocBench( Executor.ExecutorFactory executorFactory, DocBenchConfig config, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList, QueryMode.TABLE );
        this.config = config;
        measuredTimes = Collections.synchronizedList( new LinkedList<>() );
        queryTypes = new HashMap<>();
        measuredTimePerQueryType = new ConcurrentHashMap<>();
        random = new Random( config.seed );

        // Build attribute values pool
        for ( int i = 0; i < config.sizeOfValuesPool; i++ ) {
            int stringLength = DataGenerator.boundedRandom( random, config.valuesStringMinLength, config.valuesStringMaxLength );
            valuesPool.add( DataGenerator.randomString( random, stringLength ) );
        }
    }


    @Override
    public void createSchema( DatabaseInstance databaseInstance, boolean includingKeys ) {
        log.info( "Creating schema..." );
        Executor executor = null;

        String onStore = "";
        if ( config.newTablePlacementStrategy.equalsIgnoreCase( "Optimized" ) && config.dataStores.size() > 1 ) {
            for ( String storeName : PolyphenyDbExecutor.storeNames ) {
                if ( storeName.toLowerCase().startsWith( "mongodb" ) ) {
                    onStore = storeName;
                    break;
                }
            }
            if ( onStore.isEmpty() ) {
                throw new RuntimeException( "No suitable data store found for optimized placing of the DocBench collection." );
            } else {
                onStore = ".store(\"" + onStore + "\")";
            }
        }

        try {
            executor = executorFactory.createExecutorInstance( null, NAMESPACE );
            executor.executeQuery( RawQuery.builder().mongoQl( "use " + NAMESPACE ).build() );
            executor.executeQuery( RawQuery.builder().mongoQl( "db.createCollection(product)" + onStore ).build() );
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
        DataGenerator dataGenerator = new DataGenerator( random, executor, config, progressReporter, valuesPool );
        try {
            dataGenerator.generateData();
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while generating data", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        log.info( "Preparing query list for the benchmark..." );
        List<QueryListEntry> queryList = new Vector<>();
        addNumberOfTimes( queryList, new SearchProductQueryBuilder( random, valuesPool, config ), config.numberOfFindQueries );
        addNumberOfTimes( queryList, new UpdateProductQueryBuilder( random, valuesPool, config ), config.numberOfUpdateQueries );
        addNumberOfTimes( queryList, new PutProductQueryBuilder( random, valuesPool, config ), config.numberOfPutQueries );
        return commonExecute( queryList, progressReporter, outputDirectory, numberOfThreads, Query::getMongoQl, () -> executorFactory.createExecutorInstance( csvWriter, NAMESPACE ), random );
    }


    @Override
    public void warmUp( ProgressReporter progressReporter ) {
        log.info( "Warm-up..." );
        Executor executor = null;
        SearchProductQueryBuilder searchProduct = new SearchProductQueryBuilder( random, valuesPool, config );
        UpdateProductQueryBuilder updateProduct = new UpdateProductQueryBuilder( random, valuesPool, config );
        PutProductQueryBuilder putProduct = new PutProductQueryBuilder( random, valuesPool, config );
        for ( int i = 0; i < config.numberOfWarmUpIterations; i++ ) {
            try {
                executor = executorFactory.createExecutorInstance( null, NAMESPACE );
                if ( config.numberOfFindQueries > 0 ) {
                    executor.executeQuery( searchProduct.getNewQuery() );
                }
                if ( config.numberOfUpdateQueries > 0 ) {
                    executor.executeQuery( updateProduct.getNewQuery() );
                }
                if ( config.numberOfPutQueries > 0 ) {
                    executor.executeQuery( putProduct.getNewQuery() );
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
    public void analyze( Properties properties, File outputDirectory ) {
        super.analyze( properties, outputDirectory );
        properties.put( "numberOfFindQueries", measuredTimePerQueryType.get( 1 ).size() );
        properties.put( "numberOfUpdateQueries", measuredTimePerQueryType.get( 2 ).size() );
        properties.put( "numberOfPutQueries", measuredTimePerQueryType.get( 3 ).size() );
    }


    @Override
    public int getNumberOfInsertThreads() {
        return 1;
    }


    private void addNumberOfTimes( List<QueryListEntry> list, QueryBuilder queryBuilder, int numberOfTimes ) {
        int id = queryTypes.size() + 1;
        queryTypes.put( id, queryBuilder.getNewQuery().getMongoQl() );
        measuredTimePerQueryType.put( id, Collections.synchronizedList( new LinkedList<>() ) );
        for ( int i = 0; i < numberOfTimes; i++ ) {
            list.add( new QueryListEntry( queryBuilder.getNewQuery(), id ) );
        }
    }


}
