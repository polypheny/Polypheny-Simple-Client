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

package org.polypheny.simpleclient.scenario.gavel;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.JdbcExecutor;
import org.polypheny.simpleclient.executor.MonetdbExecutor.MonetdbExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbMongoQlExecutor.PolyphenyDbMongoQlExecutorFactory;
import org.polypheny.simpleclient.executor.PostgresExecutor.PostgresExecutorFactory;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.EvaluationThread;
import org.polypheny.simpleclient.scenario.EvaluationThreadMonitor;
import org.polypheny.simpleclient.scenario.PolyphenyScenario;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.ChangePasswordOfRandomUser;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.ChangeRandomAuction;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.CountAuction;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.CountBid;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.CountCategory;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.CountUser;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.InsertRandomAuction;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.InsertRandomBid;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.InsertUser;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SearchAuction;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectAllBidsOnRandomAuction;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectHighestBidOnRandomAuction;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectHighestOverallBid;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectPriceBetweenAndNotInCategory;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectRandomAuction;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectRandomBid;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectRandomUser;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectTheHundredNextEndingAuctionsOfRandomCategory;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectTopHundredSellerByNumberOfAuctions;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectTopTenCitiesByNumberOfCustomers;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.CreateTable;


@Slf4j
public class Gavel extends PolyphenyScenario {

    private final GavelConfig config;


    public Gavel( JdbcExecutor.ExecutorFactory executorFactory, GavelConfig config, boolean commitAfterEveryQuery, boolean dumpQueryList, QueryMode queryMode ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList, queryMode );
        this.config = config;
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        log.info( "Analyzing currently stored data..." );
        Map<String, Integer> numbers = getNumbers();

        InsertRandomAuction.setNextId( numbers.get( "auctions" ) + 1 );
        InsertRandomBid.setNextId( numbers.get( "bids" ) + 1 );

        log.info( "Preparing query list for the benchmark..." );
        List<QueryListEntry> queryList = new Vector<>();
        addNumberOfTimes( queryList, new InsertUser(), config.numberOfAddUserQueries );
        addNumberOfTimes( queryList, new ChangePasswordOfRandomUser( numbers.get( "users" ) ), config.numberOfChangePasswordQueries );
        addNumberOfTimes( queryList, new InsertRandomAuction( numbers.get( "users" ), numbers.get( "categories" ), config ), config.numberOfAddAuctionQueries );
        addNumberOfTimes( queryList, new InsertRandomBid( numbers.get( "auctions" ), numbers.get( "users" ) ), config.numberOfAddBidQueries );
        addNumberOfTimes( queryList, new ChangeRandomAuction( numbers.get( "auctions" ), config ), config.numberOfChangeAuctionQueries );
        addNumberOfTimes( queryList, new SelectRandomAuction( numbers.get( "auctions" ), queryMode ), config.numberOfGetAuctionQueries );
        addNumberOfTimes( queryList, new SelectTheHundredNextEndingAuctionsOfRandomCategory( numbers.get( "categories" ), config, queryMode ), config.numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries );
        addNumberOfTimes( queryList, new SearchAuction( queryMode ), config.numberOfSearchAuctionQueries );
        addNumberOfTimes( queryList, new CountAuction( queryMode ), config.numberOfCountAuctionsQueries );
        addNumberOfTimes( queryList, new SelectTopTenCitiesByNumberOfCustomers( queryMode ), config.numberOfTopTenCitiesByNumberOfCustomersQueries );
        addNumberOfTimes( queryList, new CountBid( queryMode ), config.numberOfCountBidsQueries );
        addNumberOfTimes( queryList, new SelectRandomBid( numbers.get( "bids" ), queryMode ), config.numberOfGetBidQueries );
        addNumberOfTimes( queryList, new SelectRandomUser( numbers.get( "users" ), queryMode ), config.numberOfGetUserQueries );
        addNumberOfTimes( queryList, new SelectAllBidsOnRandomAuction( numbers.get( "auctions" ), queryMode ), config.numberOfGetAllBidsOnAuctionQueries );
        addNumberOfTimes( queryList, new SelectHighestBidOnRandomAuction( numbers.get( "auctions" ), queryMode ), config.numberOfGetCurrentlyHighestBidOnAuctionQueries );
        addNumberOfTimes( queryList, new SelectHighestOverallBid( queryMode ), config.totalNumOfHighestOverallBidQueries );
        addNumberOfTimes( queryList, new SelectTopHundredSellerByNumberOfAuctions( queryMode ), config.totalNumOfTopHundredSellerByNumberOfAuctionsQueries );
        addNumberOfTimes( queryList, new SelectPriceBetweenAndNotInCategory( queryMode ), config.totalNumOfPriceBetweenAndNotInCategoryQueries );

        Collections.shuffle( queryList );

        // This dumps the sql queries independent of the selected interface
        dumpQueryList( outputDirectory, queryList, Query::getSql );

        log.info( "Executing benchmark..." );
        (new Thread( new ProgressReporter.ReportQueryListProgress( queryList, progressReporter ) )).start();
        long startTime = System.nanoTime();

        ArrayList<EvaluationThread> threads = new ArrayList<>();
        for ( int i = 0; i < numberOfThreads; i++ ) {
            threads.add( new EvaluationThread( queryList, executorFactory.createExecutorInstance( csvWriter ), queryTypes.keySet(), commitAfterEveryQuery ) );
        }

        EvaluationThreadMonitor threadMonitor = new EvaluationThreadMonitor( threads );
        threads.forEach( t -> t.setThreadMonitor( threadMonitor ) );

        for ( EvaluationThread thread : threads ) {
            thread.start();
        }

        for ( EvaluationThread thread : threads ) {
            try {
                thread.join();
                this.measuredTimes.addAll( thread.getMeasuredTimes() );
                thread.getMeasuredTimePerQueryType().forEach( ( k, v ) -> {
                    if ( !this.measuredTimePerQueryType.containsKey( k ) ) {
                        this.measuredTimePerQueryType.put( k, new ArrayList<>() );
                    }
                    this.measuredTimePerQueryType.get( k ).addAll( v );
                } );
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

        log.info( "run time: {} s", executeRuntime / 1000000000 );

        return executeRuntime;
    }


    @Override
    public void warmUp( ProgressReporter progressReporter ) {
        log.info( "Analyzing currently stored data..." );
        Map<String, Integer> numbers = getNumbers();

        InsertRandomAuction.setNextId( numbers.get( "auctions" ) + 2 );
        InsertRandomBid.setNextId( numbers.get( "bids" ) + 2 );

        log.info( "Warm-up..." );
        Executor executor = null;
        for ( int i = 0; i < config.numberOfWarmUpIterations; i++ ) {
            try {
                executor = executorFactory.createExecutorInstance();
                if ( config.numberOfAddUserQueries > 0 ) {
                    executor.executeQuery( new InsertUser().getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.numberOfChangePasswordQueries > 0 ) {
                    executor.executeQuery( new ChangePasswordOfRandomUser( numbers.get( "users" ) ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.numberOfAddAuctionQueries > 0 ) {
                    executor.executeQuery( new InsertRandomAuction( numbers.get( "users" ), numbers.get( "categories" ), config ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.numberOfAddBidQueries > 0 ) {
                    executor.executeQuery( new InsertRandomBid( numbers.get( "auctions" ), numbers.get( "users" ) ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.numberOfChangeAuctionQueries > 0 ) {
                    executor.executeQuery( new ChangeRandomAuction( numbers.get( "auctions" ), config ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.numberOfGetAuctionQueries > 0 ) {
                    executor.executeQuery( new SelectRandomAuction( numbers.get( "auctions" ), queryMode ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries > 0 ) {
                    executor.executeQuery( new SelectTheHundredNextEndingAuctionsOfRandomCategory( numbers.get( "categories" ), config, queryMode ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.numberOfSearchAuctionQueries > 0 ) {
                    executor.executeQuery( new SearchAuction( queryMode ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.numberOfCountAuctionsQueries > 0 ) {
                    executor.executeQuery( new CountAuction( queryMode ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.numberOfTopTenCitiesByNumberOfCustomersQueries > 0 ) {
                    executor.executeQuery( new SelectTopTenCitiesByNumberOfCustomers( queryMode ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.numberOfCountBidsQueries > 0 ) {
                    executor.executeQuery( new CountBid( queryMode ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.numberOfGetBidQueries > 0 ) {
                    executor.executeQuery( new SelectRandomBid( numbers.get( "bids" ), queryMode ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.numberOfGetUserQueries > 0 ) {
                    executor.executeQuery( new SelectRandomUser( numbers.get( "users" ), queryMode ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.numberOfGetAllBidsOnAuctionQueries > 0 ) {
                    executor.executeQuery( new SelectAllBidsOnRandomAuction( numbers.get( "auctions" ), queryMode ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.numberOfGetCurrentlyHighestBidOnAuctionQueries > 0 ) {
                    executor.executeQuery( new SelectHighestBidOnRandomAuction( numbers.get( "auctions" ), queryMode ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.totalNumOfPriceBetweenAndNotInCategoryQueries > 0 ) {
                    executor.executeQuery( new SelectPriceBetweenAndNotInCategory( queryMode ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.totalNumOfTopHundredSellerByNumberOfAuctionsQueries > 0 ) {
                    executor.executeQuery( new SelectTopHundredSellerByNumberOfAuctions( queryMode ).getNewQuery() );
                    executor.executeCommit();
                }
                if ( config.totalNumOfHighestOverallBidQueries > 0 ) {
                    executor.executeQuery( new SelectHighestOverallBid( queryMode ).getNewQuery() );
                    executor.executeCommit();
                }
            } catch ( ExecutorException e ) {
                throw new RuntimeException( "Error while executing warm-up queries", e );
            } catch ( Exception e ) {
                log.error( "Exception while executing warmup queries", e );
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


    private long countNumberOfRecords( Executor executor, QueryBuilder queryBuilder ) throws ExecutorException {
        return executor.executeQueryAndGetNumber( queryBuilder.getNewQuery() );
    }


    @Override
    public void createSchema( DatabaseInstance databaseInstance, boolean includingKeys ) {
        log.info( "Creating schema..." );
        InputStream file;
        if ( executorFactory instanceof PolyphenyDbMongoQlExecutorFactory ) {
            file = ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavel/schema.mongoql" );
            executeMongoQlSchema( file );
        } else if ( executorFactory instanceof PolyphenyDbJdbcExecutorFactory || executorFactory instanceof PostgresExecutorFactory ||
                executorFactory instanceof MonetdbExecutorFactory ) {
            if ( includingKeys ) {
                file = ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavel/schema.sql" );
            } else {
                file = ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavel/schema-without-keys-and-constraints.sql" );
            }

            String onStore = "";
            if ( config.newTablePlacementStrategy.equalsIgnoreCase( "Optimized" ) && config.dataStores.size() > 1 ) {
                for ( String storeName : PolyphenyDbExecutor.storeNames ) {
                    if ( storeName.toLowerCase().startsWith( "postgres" ) || storeName.toLowerCase().startsWith( "hsqldb" ) ||
                            storeName.toLowerCase().startsWith( "monetdb" ) ) {
                        onStore = storeName;
                        break;
                    }
                }
                if ( onStore.isEmpty() ) {
                    throw new RuntimeException( "No suitable data store found for optimized placing of Gavel tables." );
                } else {
                    onStore = " ON STORE " + onStore;
                }
            }

            // Check if file != null
            executeSchema( file, onStore );

            // Create Views / Materialized Views
            if ( queryMode == QueryMode.VIEW ) {
                log.info( "Creating Views ..." );
                file = ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavel/view.sql" );
                executeSchema( file, "" );
            } else if ( queryMode == QueryMode.MATERIALIZED ) {
                log.info( "Creating Materialized Views ..." );
                file = ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavel/materialized.sql" );
                executeSchema( file, "" );
            }
        } else {
            throw new RuntimeException( "Unsupported executor factory: " + executorFactory.getClass().getName() );
        }
    }


    private void executeMongoQlSchema( InputStream file ) {
        Executor executor = null;
        if ( file == null ) {
            throw new RuntimeException( "Unable to load schema definition file" );
        }
        try ( BufferedReader bf = new BufferedReader( new InputStreamReader( file ) ) ) {
            executor = executorFactory.createExecutorInstance();
            String line = bf.readLine();
            executor.executeQuery( RawQuery.builder().mongoQl( "use test" ).build() );
            while ( line != null ) {
                executor.executeQuery( RawQuery.builder().mongoQl( "db.createCollection(" + line + ")" ).build() );
                line = bf.readLine();
            }
        } catch ( IOException | ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    private void executeSchema( InputStream file, String onStore ) {
        Executor executor = null;
        if ( file == null ) {
            throw new RuntimeException( "Unable to load schema definition file" );
        }

        try ( BufferedReader bf = new BufferedReader( new InputStreamReader( file ) ) ) {
            executor = executorFactory.createExecutorInstance();
            String line = bf.readLine();
            while ( line != null ) {
                if ( line.toLowerCase().startsWith( "create table" ) ) {
                    line = line + onStore;
                }
                executor.executeQuery( RawQuery.builder().sql( line ).expectResultSet( false ).build() );
                line = bf.readLine();
            }
        } catch ( IOException | ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    public void generateData( DatabaseInstance databaseInstance, ProgressReporter progressReporter ) {
        log.info( "Generating data..." );

        DataGenerationThreadMonitor threadMonitor = new DataGenerationThreadMonitor();

        Executor executor1 = executorFactory.createExecutorInstance();
        DataGenerator dataGenerator = new DataGenerator( executor1, config, progressReporter, threadMonitor );
        try {
            //dataGenerator.truncateTables();
            dataGenerator.generateCategories();
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while generating data", e );
        } finally {
            commitAndCloseExecutor( executor1 );
        }

        ArrayList<Thread> threads = new ArrayList<>();
        int numberOfUserGenerationThreads;
        if ( executorFactory.getMaxNumberOfThreads() > 0 && config.numberOfUserGenerationThreads > executorFactory.getMaxNumberOfThreads() ) {
            numberOfUserGenerationThreads = executorFactory.getMaxNumberOfThreads();
            log.warn( "Limiting number of executor threads to {} threads (instead of {} as specified by the job)", numberOfUserGenerationThreads, config.numberOfUserGenerationThreads );
        } else {
            numberOfUserGenerationThreads = config.numberOfUserGenerationThreads;
        }
        for ( int i = 0; i < numberOfUserGenerationThreads; i++ ) {
            Runnable task = () -> {
                Executor executor = executorFactory.createExecutorInstance();
                try {
                    DataGenerator dg = new DataGenerator( executor, config, progressReporter, threadMonitor );
                    dg.generateUsers( config.numberOfUsers / numberOfUserGenerationThreads );
                } catch ( ExecutorException e ) {
                    threadMonitor.notifyAboutError( e );
                    try {
                        executor.executeRollback();
                    } catch ( ExecutorException ex ) {
                        log.error( "Error while rollback", e );
                    }
                    log.error( "Exception while generating data", e );
                } catch ( Throwable e ) {
                    log.error( "Exception while inserting users", e );
                } finally {
                    try {
                        executor.closeConnection();
                    } catch ( ExecutorException e ) {
                        log.error( "Error while closing connection", e );
                    }
                }
            };
            Thread thread = new Thread( task );
            threads.add( thread );
            thread.start();
        }

        if ( !config.parallelizeUserGenerationAndAuctionGeneration ) {
            for ( Thread t : threads ) {
                try {
                    t.join();
                } catch ( InterruptedException e ) {
                    throw new RuntimeException( "Unexpected interrupt", e );
                }
            }
        }

        int numberOfAuctionGenerationThreads = config.numberOfAuctionGenerationThreads;
        if ( executorFactory.getMaxNumberOfThreads() > 0 && config.numberOfAuctionGenerationThreads > executorFactory.getMaxNumberOfThreads() ) {
            numberOfAuctionGenerationThreads = executorFactory.getMaxNumberOfThreads();
            log.warn( "Limiting number of auction generation threads to {} threads (instead of {} as specified by the job)", numberOfAuctionGenerationThreads, config.numberOfAuctionGenerationThreads );
        }
        int rangeSize = config.numberOfAuctions / numberOfAuctionGenerationThreads;
        for ( int i = 1; i <= numberOfAuctionGenerationThreads; i++ ) {
            final int start = ((i - 1) * rangeSize) + 1;
            final int end = rangeSize * i;
            Runnable task = () -> {
                Executor executor = executorFactory.createExecutorInstance();
                try {
                    DataGenerator dg = new DataGenerator( executor, config, progressReporter, threadMonitor );
                    dg.generateAuctions( start, end );
                } catch ( ExecutorException e ) {
                    threadMonitor.notifyAboutError( e );
                    try {
                        executor.executeRollback();
                    } catch ( ExecutorException ex ) {
                        log.error( "Error while rollback", e );
                    }
                    log.error( "Exception while generating data", e );
                } catch ( Throwable e ) {
                    log.error( "Exception while inserting auctions", e );
                } finally {
                    try {
                        executor.closeConnection();
                    } catch ( ExecutorException e ) {
                        log.error( "Error while closing connection", e );
                    }
                }
            };
            Thread thread = new Thread( task );
            threads.add( thread );
            thread.start();
        }

        for ( Thread t : threads ) {
            try {
                t.join();
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }
        }

        if ( queryMode == QueryMode.MATERIALIZED ) {
            updateMaterializedView();
        }

        if ( threadMonitor.aborted ) {
            throw new RuntimeException( "Exception while generating data", threadMonitor.exception );
        }
    }


    public void updateMaterializedView() {
        log.info( "Update Materialized View..." );
        Executor executor = null;

        try {
            executor = executorFactory.createExecutorInstance();

            executor.executeQuery( (new CreateTable( "ALTER MATERIALIZED VIEW user_materialized FRESHNESS MANUAL" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "ALTER MATERIALIZED VIEW bid_materialized FRESHNESS MANUAL" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "ALTER MATERIALIZED VIEW picture_materialized FRESHNESS MANUAL" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "ALTER MATERIALIZED VIEW auction_materialized FRESHNESS MANUAL" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "ALTER MATERIALIZED VIEW category_materialized FRESHNESS MANUAL" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "ALTER MATERIALIZED VIEW countAuction_materialized FRESHNESS MANUAL" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "ALTER MATERIALIZED VIEW countBid_materialized FRESHNESS MANUAL" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "ALTER MATERIALIZED VIEW auctionCategory_materialized FRESHNESS MANUAL" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "ALTER MATERIALIZED VIEW topHundredSellerByNumberOfAuctions_materialized FRESHNESS MANUAL" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "ALTER MATERIALIZED VIEW highestBid_materialized FRESHNESS MANUAL" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "ALTER MATERIALIZED VIEW priceBetween_materialized FRESHNESS MANUAL" )).getNewQuery() );

        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while updating Materialized View", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    public static class DataGenerationThreadMonitor {

        private final List<DataGenerator> dataGenerators;
        @Getter
        private boolean aborted;
        @Getter
        private Exception exception = null;


        public DataGenerationThreadMonitor() {
            this.dataGenerators = new LinkedList<>();
            aborted = false;
        }


        void registerDataGenerator( DataGenerator instance ) {
            dataGenerators.add( instance );
        }


        public void abortAll() {
            this.aborted = true;
            dataGenerators.forEach( DataGenerator::abort );
        }


        public void notifyAboutError( Exception e ) {
            exception = e;
            abortAll();
        }

    }


    @Override
    public int getNumberOfInsertThreads() {
        return config.numberOfUserGenerationThreads + config.numberOfAuctionGenerationThreads;
    }


    private void addNumberOfTimes( List<QueryListEntry> list, QueryBuilder queryBuilder, int numberOfTimes ) {
        int id = queryTypes.size() + 1;
        queryTypes.put( id, queryBuilder.getNewQuery().getSql() );
        measuredTimePerQueryType.put( id, Collections.synchronizedList( new LinkedList<>() ) );
        for ( int i = 0; i < numberOfTimes; i++ ) {
            list.add( new QueryListEntry( queryBuilder.getNewQuery(), id ) );
        }
    }


    private Map<String, Integer> getNumbers() {
        Map<String, Integer> numbers = new HashMap<>();
        Executor executor = null;
        try {
            executor = executorFactory.createExecutorInstance();
            numbers.put( "auctions", (int) countNumberOfRecords( executor, new CountAuction( QueryMode.TABLE ) ) );
            numbers.put( "users", (int) countNumberOfRecords( executor, new CountUser() ) );
            numbers.put( "categories", (int) countNumberOfRecords( executor, new CountCategory() ) );
            numbers.put( "bids", (int) countNumberOfRecords( executor, new CountBid( QueryMode.TABLE ) ) );

            log.debug( "Number of auctions: " + numbers.get( "auctions" ) );
            log.debug( "Number of users: " + numbers.get( "users" ) );
            log.debug( "Number of categories: " + numbers.get( "categories" ) );
            log.debug( "Number of bids: " + numbers.get( "bids" ) );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while analyzing currently stored data: " + e.getMessage(), e );
        } finally {
            commitAndCloseExecutor( executor );
        }
        log.info( "Current number of elements in the database:\nAuctions: {} | Users: {} | Categories: {} | Bids: {}", numbers.get( "auction" ), numbers.get( "user" ), numbers.get( "categories" ), numbers.get( "bids" ) );
        return numbers;
    }

}
