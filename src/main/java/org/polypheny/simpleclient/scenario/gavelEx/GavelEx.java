/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 The Polypheny Project
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

package org.polypheny.simpleclient.scenario.gavelEx;


import com.google.common.base.Joiner;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.Pair;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.JdbcExecutor;
import org.polypheny.simpleclient.executor.PolyphenyDbMongoQlExecutor;
import org.polypheny.simpleclient.main.ChronosAgent;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.Scenario;
import org.polypheny.simpleclient.scenario.gavelEx.GavelExProfile.QueryPossibility;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.ChangePasswordOfRandomUser;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.ChangeRandomAuction;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.CountAuction;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.CountBid;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.CountCategory;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.CountUser;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.InsertRandomAuction;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.InsertRandomBid;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.InsertUser;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.SearchAuction;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.SelectAllBidsOnRandomAuction;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.SelectHighestBidOnRandomAuction;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.SelectHighestOverallBid;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.SelectPriceBetweenAndNotInCategory;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.SelectRandomAuction;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.SelectRandomUser;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.SelectTheHundredNextEndingAuctionsOfRandomCategory;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.SelectTopHundredSellerByNumberOfAuctions;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.SelectTopTenCitiesByNumberOfCustomers;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.TruncateAuction;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.TruncateBid;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.TruncateCategory;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.TruncatePicture;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.TruncateUser;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.CreateTable;


@Slf4j
public class GavelEx extends Scenario {

    private final GavelExConfig config;

    private final List<Long> measuredTimes;
    private final Map<Integer, String> queryTypes;
    private final Map<Integer, List<Long>> measuredTimePerQueryType;


    public GavelEx( JdbcExecutor.ExecutorFactory executorFactoryHSQLDB, PolyphenyDbMongoQlExecutor.ExecutorFactory executorFactoryMONGODB, GavelExConfig config, boolean commitAfterEveryQuery, boolean dumpQueryList, QueryMode queryMode ) {
        super( executorFactoryHSQLDB, executorFactoryMONGODB, commitAfterEveryQuery, dumpQueryList, queryMode );
        this.config = config;
        measuredTimes = Collections.synchronizedList( new LinkedList<>() );

        queryTypes = new HashMap<>();
        measuredTimePerQueryType = new ConcurrentHashMap<>();
    }


    public enum QueryLanguage {
        SQL, MQL
    }


    public Pair<List<QueryBuilder>, QueryLanguage> getPossibleClasses( QueryPossibility query, Map<String, Integer> numbers ) {
/*
        final List<QueryBuilder> insertQueries = Arrays.asList(
                new InsertRandomAuction( numbers.get( "users" ), numbers.get( "categories" ), config ),
                new InsertRandomBid( numbers.get( "auctions" ), numbers.get( "users" ) ) );
        final List<QueryBuilder> updateQueries = Arrays.asList(
                new ChangePasswordOfRandomUser( numbers.get( "users" ) ),
                new ChangeRandomAuction( numbers.get( "auctions" ), config ) );
        final List<QueryBuilder> simpleSelectQueries = Arrays.asList(
                new SearchAuction( queryMode ),
                new SelectAllBidsOnRandomAuction( numbers.get( "auctions" ), queryMode ),
                new SelectHighestBidOnRandomAuction( numbers.get( "auctions" ), queryMode ),
                new SelectHighestOverallBid( queryMode ),
                new SelectPriceBetweenAndNotInCategory( queryMode ),
                new SelectRandomAuction( numbers.get( "auctions" ), queryMode ),
                new SelectRandomBid( numbers.get( "bids" ), queryMode ),
                new SelectRandomUser( numbers.get( "users" ), queryMode ) );
        final List<QueryBuilder> complexSelectQueries = Arrays.asList(
                new SelectTheHundredNextEndingAuctionsOfRandomCategory( numbers.get( "categories" ), config, queryMode ),
                new SelectTopHundredSellerByNumberOfAuctions( queryMode ),
                new SelectTopTenCitiesByNumberOfCustomers( queryMode ) );
        final List<QueryBuilder> truncateQueries = Arrays.asList(
                new TruncateAuction(),
                new TruncateBid(),
                new TruncateCategory(),
                new TruncatePicture(),
                new TruncateUser()

        );
        final List<QueryBuilder> deleteQueries = Arrays.asList(

        );

 */
        final List<QueryBuilder> insertQueries = Arrays.asList(
                new InsertRandomAuction( config.numberOfUsers, config.numberOfCategories, config ),
                new InsertRandomBid( config.numberOfAuctions, config.numberOfUsers ) );
        final List<QueryBuilder> updateQueries = Arrays.asList(
                new ChangePasswordOfRandomUser( config.numberOfUsers ),
                new ChangeRandomAuction( config.numberOfUsers, config ) );
        final List<QueryBuilder> simpleSelectQueries = Arrays.asList(
                new SearchAuction( queryMode ),
                new SelectAllBidsOnRandomAuction( config.numberOfAuctions, queryMode ),
                new SelectHighestBidOnRandomAuction( config.numberOfAuctions, queryMode ),
                new SelectHighestOverallBid( queryMode ),
                new SelectPriceBetweenAndNotInCategory( queryMode ),
                new SelectRandomAuction( config.numberOfAuctions, queryMode ),
                //new SelectRandomBid( confi, queryMode ),
                new SelectRandomUser( config.numberOfUsers, queryMode ) );
        final List<QueryBuilder> complexSelectQueries = Arrays.asList(
                new SelectTheHundredNextEndingAuctionsOfRandomCategory( config.numberOfCategories, config, queryMode ),
                new SelectTopHundredSellerByNumberOfAuctions( queryMode ),
                new SelectTopTenCitiesByNumberOfCustomers( queryMode ) );
        final List<QueryBuilder> truncateQueries = Arrays.asList(
                new TruncateAuction(),
                new TruncateBid(),
                new TruncateCategory(),
                new TruncatePicture(),
                new TruncateUser()

        );
        final List<QueryBuilder> deleteQueries = Arrays.asList(

        );

        switch ( query ) {
            case INSERT_SQL:
                return new Pair<>( insertQueries, QueryLanguage.SQL );
            case INSERT_MQL:
                return new Pair<>( insertQueries, QueryLanguage.MQL );
            case UPDATE_SQL:
                return new Pair<>( updateQueries, QueryLanguage.SQL );
            case UPDATE_MQL:
                return new Pair<>( updateQueries, QueryLanguage.MQL );
            case SIMPLE_SELECT_SQL:
                return new Pair<>( simpleSelectQueries, QueryLanguage.SQL );
            case SIMPLE_SELECT_MQL:
                return new Pair<>( simpleSelectQueries, QueryLanguage.MQL );
            case COMPLEX_SELECT_SQL:
                return new Pair<>( complexSelectQueries, QueryLanguage.SQL );
            case COMPLEX_SELECT_MQL:
                return new Pair<>( complexSelectQueries, QueryLanguage.MQL );
            case TRUNCATE_SQL:
                return new Pair<>( truncateQueries, QueryLanguage.SQL );
            case TRUNCATE_MQL:
                return new Pair<>( truncateQueries, QueryLanguage.MQL );
            case DELETE_SQL:
                return new Pair<>( deleteQueries, QueryLanguage.SQL );
            case DELETE_MQL:
                return new Pair<>( deleteQueries, QueryLanguage.MQL );

            default:
                throw new RuntimeException( "This QueryPossibility has no saved Queries. Please add a List of Classes with suitable queries." );
        }
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads, GavelExProfile profile ) {

        log.info( "Analyzing currently stored data..." );
        Map<String, Integer> numbers = getNumbers();

        InsertRandomAuction.setNextId( numbers.get( "auctions" ) + 1 );
        InsertRandomBid.setNextId( numbers.get( "bids" ) + 1 );

        log.info( "Preparing query list for the benchmark..." );

        List<QueryListEntry> queryList = new Vector<>();

        for ( Pair<Pair<QueryPossibility, Integer>, Integer> part : profile.timeline ) {
            Pair<QueryPossibility, Integer> queryInfo = part.left;
            QueryPossibility query = queryInfo.left;

            Pair<List<QueryBuilder>, QueryLanguage> possibleQueries = getPossibleClasses( query, numbers );

            if ( possibleQueries.left.size() > 0 ) {
                Random rand = new Random();
                for ( int i = 0; i < queryInfo.right; i++ ) {
                    addNumberOfTimes( queryList, possibleQueries.left.get( rand.nextInt( possibleQueries.left.size() ) ), 1, part.right, possibleQueries.right );
                }
            }

        }

        // This dumps the sql queries independent of the selected interface
        if ( outputDirectory != null && dumpQueryList ) {
            log.info( "Dump query list..." );
            try {
                FileWriter fw = new FileWriter( outputDirectory.getPath() + File.separator + "queryList" );
                queryList.forEach( query -> {
                    try {
                        if(query.queryLanguage == QueryLanguage.SQL){
                            fw.append( query.query.getSql() ).append( "\n" );
                        }else if(query.queryLanguage == QueryLanguage.MQL){
                            fw.append( query.query.getMongoQl() ).append( "\n" );
                        }else {
                            throw new RuntimeException("Querylanguag is not implemented yet.");
                        }


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
            threads.add( new EvaluationThread( queryList, executorFactory.createExecutorInstance( csvWriter ), executorFactoryMONGODB.createExecutorInstance(csvWriter) ) );
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
        log.info( "Analyzing currently stored data..." );
        Map<String, Integer> numbers = getNumbers();

        InsertRandomAuction.setNextId( numbers.get( "auctions" ) + 2 );
        InsertRandomBid.setNextId( numbers.get( "bids" ) + 2 );

        log.info( "Warm-up..." );
        Executor executor = null;
        for ( int i = 0; i < iterations; i++ ) {
            try {
                executor = executorFactory.createExecutorInstance();
                executor.executeQuery( new InsertUser().getNewQuery() );
                /*
                if ( config.numberOfAddUserQueries > 0 ) {
                    executor.executeQuery( new InsertUser().getNewQuery() );
                }
                if ( config.numberOfChangePasswordQueries > 0 ) {
                    executor.executeQuery( new ChangePasswordOfRandomUser( numbers.get( "users" ) ).getNewQuery() );
                }
                if ( config.numberOfAddAuctionQueries > 0 ) {
                    executor.executeQuery( new InsertRandomAuction( numbers.get( "users" ), numbers.get( "categories" ), config ).getNewQuery() );
                }
                if ( config.numberOfAddBidQueries > 0 ) {
                    executor.executeQuery( new InsertRandomBid( numbers.get( "auctions" ), numbers.get( "users" ) ).getNewQuery() );
                }
                if ( config.numberOfChangeAuctionQueries > 0 ) {
                    executor.executeQuery( new ChangeRandomAuction( numbers.get( "auctions" ), config ).getNewQuery() );
                }
                if ( config.numberOfGetAuctionQueries > 0 ) {
                    executor.executeQuery( new SelectRandomAuction( numbers.get( "auctions" ), queryMode ).getNewQuery() );
                }
                if ( config.numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries > 0 ) {
                    executor.executeQuery( new SelectTheHundredNextEndingAuctionsOfRandomCategory( numbers.get( "categories" ), config, queryMode ).getNewQuery() );
                }
                if ( config.numberOfSearchAuctionQueries > 0 ) {
                    executor.executeQuery( new SearchAuction( queryMode ).getNewQuery() );
                }
                if ( config.numberOfCountAuctionsQueries > 0 ) {
                    executor.executeQuery( new CountAuction( queryMode ).getNewQuery() );
                }
                if ( config.numberOfTopTenCitiesByNumberOfCustomersQueries > 0 ) {
                    executor.executeQuery( new SelectTopTenCitiesByNumberOfCustomers( queryMode ).getNewQuery() );
                }
                if ( config.numberOfCountBidsQueries > 0 ) {
                    executor.executeQuery( new CountBid( queryMode ).getNewQuery() );
                }
                if ( config.numberOfGetBidQueries > 0 ) {
                    executor.executeQuery( new SelectRandomBid( numbers.get( "bids" ), queryMode ).getNewQuery() );
                }
                if ( config.numberOfGetUserQueries > 0 ) {
                    executor.executeQuery( new SelectRandomUser( numbers.get( "users" ), queryMode ).getNewQuery() );
                }
                if ( config.numberOfGetAllBidsOnAuctionQueries > 0 ) {
                    executor.executeQuery( new SelectAllBidsOnRandomAuction( numbers.get( "auctions" ), queryMode ).getNewQuery() );
                }
                if ( config.numberOfGetCurrentlyHighestBidOnAuctionQueries > 0 ) {
                    executor.executeQuery( new SelectHighestBidOnRandomAuction( numbers.get( "auctions" ), queryMode ).getNewQuery() );
                }
                if ( config.totalNumOfPriceBetweenAndNotInCategoryQueries > 0 ) {
                    executor.executeQuery( new SelectPriceBetweenAndNotInCategory( queryMode ).getNewQuery() );
                }
                if ( config.totalNumOfTopHundredSellerByNumberOfAuctionsQueries > 0 ) {
                    executor.executeQuery( new SelectTopHundredSellerByNumberOfAuctions( queryMode ).getNewQuery() );
                }
                if ( config.totalNumOfHighestOverallBidQueries > 0 ) {
                    executor.executeQuery( new SelectHighestOverallBid( queryMode ).getNewQuery() );
                }

                 */
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

        private final Executor executorSQL;
        private final Executor executorMQL;
        private final List<QueryListEntry> theQueryList;
        private boolean abort = false;
        @Setter
        private EvaluationThreadMonitor threadMonitor;


        EvaluationThread( List<QueryListEntry> queryList, Executor executorSQL, Executor  executorMQL) {
            super( "EvaluationThread" );
            this.executorSQL = executorSQL;
            this.executorMQL = executorMQL;
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
                    if(queryListEntry.queryLanguage == QueryLanguage.SQL){
                        executorSQL.executeQuery( queryListEntry.query );
                    }else if(queryListEntry.queryLanguage == QueryLanguage.MQL){
                        executorMQL.executeQuery( queryListEntry.query );
                    }else{
                        throw new RuntimeException("Query language is not implemented yet.");
                    }

                } catch ( ExecutorException e ) {
                    log.error( "Caught exception while executing the following query: {}", queryListEntry.query.getClass().getName(), e );
                    threadMonitor.notifyAboutError( e );
                    try {
                        if(queryListEntry.queryLanguage == QueryLanguage.SQL){
                            executorSQL.executeRollback();
                        }else if(queryListEntry.queryLanguage == QueryLanguage.MQL){
                            executorMQL.executeRollback();
                        }
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
                        if(queryListEntry.queryLanguage == QueryLanguage.SQL){
                            executorSQL.executeCommit();
                        }else if(queryListEntry.queryLanguage == QueryLanguage.MQL){
                            executorMQL.executeCommit();
                        }
                    } catch ( ExecutorException e ) {
                        log.error( "Caught exception while committing", e );
                        threadMonitor.notifyAboutError( e );
                        try {
                            if(queryListEntry.queryLanguage == QueryLanguage.SQL){
                                executorSQL.executeRollback();
                            }else if(queryListEntry.queryLanguage == QueryLanguage.MQL){
                                executorMQL.executeRollback();
                            }
                        } catch ( ExecutorException ex ) {
                            log.error( "Error while rollback", e );
                        }
                        throw new RuntimeException( e );
                    }
                }
            }

            try {
                executorSQL.executeCommit();
                executorMQL.executeCommit();
            } catch ( ExecutorException e ) {
                log.error( "Caught exception while committing", e );
                threadMonitor.notifyAboutError( e );
                try {
                    executorSQL.executeRollback();
                    executorMQL.executeRollback();
                } catch ( ExecutorException ex ) {
                    log.error( "Error while rollback", e );
                }
                throw new RuntimeException( e );
            }
            executorSQL.flushCsvWriter();
            executorMQL.flushCsvWriter();
        }


        public void abort() {
            this.abort = true;
        }


        public void closeExecutor() {
            commitAndCloseExecutor( executorSQL );
            commitAndCloseExecutor( executorMQL );
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


    private long countNumberOfRecords( Executor executor, QueryBuilder queryBuilder ) throws ExecutorException {
        return executor.executeQueryAndGetNumber( queryBuilder.getNewQuery() );
    }


    @Override
    public void createSchema( boolean includingKeys, GavelExSettings gavelExSettings ) {
        log.info( "Creating schema..." );
        InputStream file;

        file = ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavelEx/schema.sql" );
        executeSchema( file, gavelExSettings );

        file = ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavelEx/schema.mongoql" );
        executeMongoQlSchema( file, gavelExSettings );

        // Create Views / Materialized Views
        if ( queryMode == QueryMode.VIEW ) {
            log.info( "Creating Views ..." );
            file = ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavel/view.sql" );
            executeSchema( file, gavelExSettings );
        } else if ( queryMode == QueryMode.MATERIALIZED ) {
            log.info( "Creating Materialized Views ..." );
            file = ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavel/materialized.sql" );
            executeSchema( file, gavelExSettings );
        }
    }


    private void executeMongoQlSchema( InputStream file, GavelExSettings gavelExSettings ) {
        Executor executor = null;
        if ( file == null ) {
            throw new RuntimeException( "Unable to load schema definition file" );
        }
        try ( BufferedReader bf = new BufferedReader( new InputStreamReader( file ) ) ) {
            executor = executorFactoryMONGODB.createExecutorInstance();
            String line = bf.readLine();
            executor.executeQuery( new RawQuery( null, null, "use test", false ) );
            while ( line != null ) {
                executor.executeQuery( new RawQuery( null, null, "db.createCollection(" + line + ")", false ) );
                line = bf.readLine();
            }
        } catch ( IOException | ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    private void executeSchema( InputStream file, GavelExSettings gavelExSettings ) {
        Executor executor = null;
        if ( file == null ) {
            throw new RuntimeException( "Unable to load schema definition file" );
        }
        try ( BufferedReader bf = new BufferedReader( new InputStreamReader( file ) ) ) {
            executor = executorFactory.createExecutorInstance();
            String line = bf.readLine();
            while ( line != null ) {
                if(!gavelExSettings.tableStores.isEmpty()){
                    List<Pair<String, String>> tableStores = gavelExSettings.tableStores;
                    for ( Pair<String, String> tableStore : tableStores ) {
                        if(line.startsWith( "CREATE" ) && line.split( " " )[2].replace( "\"", "" ).equals( tableStore.left )){
                            line = line + " ON STORE \"" + tableStore.right + "\"";
                        }
                    }

                }else if(!gavelExSettings.factoryStores.isEmpty()){
                    List<Pair<String, String>> factoryStores = gavelExSettings.factoryStores;
                    for ( Pair<String, String> tableStore : factoryStores ) {
                        if(line.startsWith( "CREATE" ) && "efHsqldb".equals( tableStore.left )){
                            line = line + " ON STORE \"" + tableStore.right + "\"";
                        }
                    }
                }
                executor.executeQuery( new RawQuery( line, null, false ) );
                line = bf.readLine();
            }
        } catch ( IOException | ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    public void generateData( ProgressReporter progressReporter ) {
        log.info( "Generating data..." );

        DataGenerationThreadMonitor threadMonitor = new DataGenerationThreadMonitor();

        Executor executor1 = executorFactory.createExecutorInstance();
        DataGeneratorEx dataGeneratorEx = new DataGeneratorEx( executor1, config, progressReporter, threadMonitor );

        Executor executor1Mongo = executorFactoryMONGODB.createExecutorInstance();
        DataGeneratorEx dataGeneratorExMongo = new DataGeneratorEx( executor1Mongo, config, progressReporter, threadMonitor );
        try {
            //dataGenerator.truncateTables();
            dataGeneratorEx.generateCategories();
            dataGeneratorExMongo.generateCategories();
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while generating data", e );
        } finally {
            commitAndCloseExecutor( executor1Mongo );
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
                Executor executor2Mongo = executorFactoryMONGODB.createExecutorInstance();
                try {
                    DataGeneratorEx dg = new DataGeneratorEx( executor, config, progressReporter, threadMonitor );
                    dg.generateUsers( config.numberOfUsers / numberOfUserGenerationThreads );

                    DataGeneratorEx dgMongo = new DataGeneratorEx( executor2Mongo, config, progressReporter, threadMonitor );
                    dgMongo.generateUsers( config.numberOfUsers / numberOfUserGenerationThreads );
                } catch ( ExecutorException e ) {
                    threadMonitor.notifyAboutError( e );
                    try {
                        executor.executeRollback();
                        executor2Mongo.executeRollback();
                    } catch ( ExecutorException ex ) {
                        log.error( "Error while rollback", e );
                    }
                    log.error( "Exception while generating data", e );
                } finally {
                    try {
                        executor.closeConnection();
                        executor2Mongo.closeConnection();
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
                Executor executor3Mongo = executorFactoryMONGODB.createExecutorInstance();
                try {
                    DataGeneratorEx dg = new DataGeneratorEx( executor, config, progressReporter, threadMonitor );
                    dg.generateAuctions( start, end );

                    DataGeneratorEx dgMongo = new DataGeneratorEx( executor3Mongo, config, progressReporter, threadMonitor );
                    dgMongo.generateAuctions( start, end );
                } catch ( ExecutorException e ) {
                    threadMonitor.notifyAboutError( e );
                    try {
                        executor.executeRollback();
                        executor3Mongo.executeRollback();
                    } catch ( ExecutorException ex ) {
                        log.error( "Error while rollback", e );
                    }
                    log.error( "Exception while generating data", e );
                } finally {
                    try {
                        executor.closeConnection();
                        executor3Mongo.closeConnection();
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


    static class DataGenerationThreadMonitor {

        private final List<DataGeneratorEx> dataGeneratorExes;
        @Getter
        private boolean aborted;
        @Getter
        private Exception exception = null;


        DataGenerationThreadMonitor() {
            this.dataGeneratorExes = new LinkedList<>();
            aborted = false;
        }


        void registerDataGenerator( DataGeneratorEx instance ) {
            dataGeneratorExes.add( instance );
        }


        public void abortAll() {
            this.aborted = true;
            dataGeneratorExes.forEach( DataGeneratorEx::abort );
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
            properties.put( "queryTypes_" + templateId + "_mean", calculateMean( time ) );
            if ( ChronosAgent.STORE_INDIVIDUAL_QUERY_TIMES ) {
                properties.put( "queryTypes_" + templateId + "_all", Joiner.on( ',' ).join( time ) );
            }
            properties.put( "queryTypes_" + templateId + "_example", queryTypes.get( templateId ) );
        } );
        properties.put( "queryTypes_maxId", queryTypes.size() );
    }


    @Override
    public int getNumberOfInsertThreads() {
        return config.numberOfUserGenerationThreads + config.numberOfAuctionGenerationThreads;
    }


    private void addNumberOfTimes( List<QueryListEntry> list, QueryBuilder queryBuilder, int numberOfTimes, int delay, QueryLanguage queryLanguage ) {
        int id = queryTypes.size() + 1;
        queryTypes.put( id, queryBuilder.getNewQuery().getSql() );
        measuredTimePerQueryType.put( id, Collections.synchronizedList( new LinkedList<>() ) );
        for ( int i = 0; i < numberOfTimes; i++ ) {
            list.add( new QueryListEntry( queryBuilder.getNewQuery(), id, delay, queryLanguage ) );
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
