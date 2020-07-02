/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
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

package org.polypheny.simpleclient.scenario.gavel;


import com.google.common.base.Joiner;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.JdbcExecutor;
import org.polypheny.simpleclient.main.ChronosAgent;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.Scenario;
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
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectRandomAuction;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectRandomBid;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectRandomUser;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectTheHundredNextEndingAuctionsOfRandomCategory;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.SelectTopTenCitiesByNumberOfCustomers;


@Slf4j
public class Gavel extends Scenario {

    private final Config config;

    private final List<Long> measuredTimes;
    private final Map<Integer, String> queryTypes;
    private final Map<Integer, List<Long>> measuredTimePerQueryType;


    public Gavel( JdbcExecutor.ExecutorFactory executorFactory, Config config, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList );
        this.config = config;
        measuredTimes = Collections.synchronizedList( new LinkedList<>() );

        queryTypes = new HashMap<>();
        measuredTimePerQueryType = new ConcurrentHashMap<>();
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory ) {
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
        addNumberOfTimes( queryList, new SelectRandomAuction( numbers.get( "auctions" ) ), config.numberOfGetAuctionQueries );
        addNumberOfTimes( queryList, new SelectTheHundredNextEndingAuctionsOfRandomCategory( numbers.get( "categories" ), config ), config.numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries );
        addNumberOfTimes( queryList, new SearchAuction(), config.numberOfSearchAuctionQueries );
        addNumberOfTimes( queryList, new CountAuction(), config.numberOfCountAuctionsQueries );
        addNumberOfTimes( queryList, new SelectTopTenCitiesByNumberOfCustomers(), config.numberOfTopTenCitiesByNumberOfCustomersQueries );
        addNumberOfTimes( queryList, new CountBid(), config.numberOfCountBidsQueries );
        addNumberOfTimes( queryList, new SelectRandomBid( numbers.get( "bids" ) ), config.numberOfGetBidQueries );
        addNumberOfTimes( queryList, new SelectRandomUser( numbers.get( "users" ) ), config.numberOfGetUserQueries );
        addNumberOfTimes( queryList, new SelectAllBidsOnRandomAuction( numbers.get( "auctions" ) ), config.numberOfGetAllBidsOnAuctionQueries );
        addNumberOfTimes( queryList, new SelectHighestBidOnRandomAuction( numbers.get( "auctions" ) ), config.numberOfGetCurrentlyHighestBidOnAuctionQueries );
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
        int numberOfThreads = config.numberOfThreads;
        if ( executorFactory.getMaxNumberOfThreads() > 0 && config.numberOfThreads > executorFactory.getMaxNumberOfThreads() ) {
            numberOfThreads = executorFactory.getMaxNumberOfThreads();
            log.warn( "Limiting number of executor threads to {} threads (instead of {} as specified by the job)", numberOfThreads, executorFactory.getMaxNumberOfThreads() );
        }
        for ( int i = 0; i < numberOfThreads; i++ ) {
            threads.add( new EvaluationThread( queryList, executorFactory.createInstance( csvWriter ) ) );
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

        if ( threadMonitor.aborted ) {
            throw new RuntimeException( "Exception while executing benchmark", threadMonitor.exception );
        }

        long runTime = System.nanoTime() - startTime;
        log.info( "run time: {} s", runTime / 1000000000 );

        for ( EvaluationThread thread : threads ) {
            thread.closeExecutor();
        }

        return runTime;
    }


    @Override
    public void warmUp( ProgressReporter progressReporter ) {
        log.info( "Analyzing currently stored data..." );
        Map<String, Integer> numbers = getNumbers();

        InsertRandomAuction.setNextId( numbers.get( "auctions" ) + 2 );
        InsertRandomBid.setNextId( numbers.get( "bids" ) + 2 );

        log.info( "Warm-up..." );
        Executor executor = null;
        for ( int i = 0; i < 2; i++ ) {
            try {
                executor = executorFactory.createInstance();
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
                    executor.executeQuery( new SelectRandomAuction( numbers.get( "auctions" ) ).getNewQuery() );
                }
                if ( config.numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries > 0 ) {
                    executor.executeQuery( new SelectTheHundredNextEndingAuctionsOfRandomCategory( numbers.get( "categories" ), config ).getNewQuery() );
                }
                if ( config.numberOfSearchAuctionQueries > 0 ) {
                    executor.executeQuery( new SearchAuction().getNewQuery() );
                }
                if ( config.numberOfCountAuctionsQueries > 0 ) {
                    executor.executeQuery( new CountAuction().getNewQuery() );
                }
                if ( config.numberOfTopTenCitiesByNumberOfCustomersQueries > 0 ) {
                    executor.executeQuery( new SelectTopTenCitiesByNumberOfCustomers().getNewQuery() );
                }
                if ( config.numberOfCountBidsQueries > 0 ) {
                    executor.executeQuery( new CountBid().getNewQuery() );
                }
                if ( config.numberOfGetBidQueries > 0 ) {
                    executor.executeQuery( new SelectRandomBid( numbers.get( "bids" ) ).getNewQuery() );
                }
                if ( config.numberOfGetUserQueries > 0 ) {
                    executor.executeQuery( new SelectRandomUser( numbers.get( "users" ) ).getNewQuery() );
                }
                if ( config.numberOfGetAllBidsOnAuctionQueries > 0 ) {
                    executor.executeQuery( new SelectAllBidsOnRandomAuction( numbers.get( "auctions" ) ).getNewQuery() );
                }
                if ( config.numberOfGetCurrentlyHighestBidOnAuctionQueries > 0 ) {
                    executor.executeQuery( new SelectHighestBidOnRandomAuction( numbers.get( "auctions" ) ).getNewQuery() );
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


    private long countNumberOfRecords( Executor executor, QueryBuilder queryBuilder ) throws ExecutorException {
        return executor.executeQueryAndGetNumber( queryBuilder.getNewQuery() );
    }


    @Override
    public void createSchema( boolean includingKeys ) {
        log.info( "Creating schema..." );
        Executor executor = null;
        InputStream file;
        if ( includingKeys ) {
            file = ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavel/schema.sql" );
        } else {
            file = ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavel/schema-without-keys-and-constraints.sql" );
        }
        // Check if file != null
        if ( file == null ) {
            throw new RuntimeException( "Unable to load schema definition file" );
        }
        try ( BufferedReader bf = new BufferedReader( new InputStreamReader( file ) ) ) {
            executor = executorFactory.createInstance();
            String line = bf.readLine();
            while ( line != null ) {
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

        Executor executor1 = executorFactory.createInstance();
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
                Executor executor = executorFactory.createInstance();
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
                Executor executor = executorFactory.createInstance();
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

        if ( threadMonitor.aborted ) {
            throw new RuntimeException( "Exception while generating data", threadMonitor.exception );
        }
    }


    static class DataGenerationThreadMonitor {

        private final List<DataGenerator> dataGenerators;
        @Getter
        private boolean aborted;
        @Getter
        private Exception exception = null;


        DataGenerationThreadMonitor() {
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


    private double calculateMean( List<Long> times ) {
        DecimalFormat df = new DecimalFormat( "0.000" );
        OptionalDouble meanOptional = times.stream().mapToLong( Long::longValue ).average();
        if ( meanOptional.isPresent() ) {
            // scale
            double mean = meanOptional.getAsDouble() / 1000000;
            String roundFormat = df.format( mean );
            try {
                return df.parse( roundFormat ).doubleValue();
            } catch ( ParseException e ) {
                log.error( "Exception", e );
            }
        }
        return -1;
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
            executor = executorFactory.createInstance();
            numbers.put( "auctions", (int) countNumberOfRecords( executor, new CountAuction() ) );
            numbers.put( "users", (int) countNumberOfRecords( executor, new CountUser() ) );
            numbers.put( "categories", (int) countNumberOfRecords( executor, new CountCategory() ) );
            numbers.put( "bids", (int) countNumberOfRecords( executor, new CountBid() ) );

            log.debug( "Number of auctions: " + numbers.get( "auctions" ) );
            log.debug( "Number of users: " + numbers.get( "users" ) );
            log.debug( "Number of categories: " + numbers.get( "categories" ) );
            log.debug( "Number of bids: " + numbers.get( "bids" ) );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while analyzing currently stored data" );
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
