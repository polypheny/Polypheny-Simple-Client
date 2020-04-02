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


import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
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
import java.util.stream.Stream;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.cli.ChronosCommand;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.main.Query;
import org.polypheny.simpleclient.main.QueryBuilder;
import org.polypheny.simpleclient.main.QueryListEntry;
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

    private final Map<String, List<Long>> executionTimes;
    private final Map<String, List<Long>> totalTimes;
    private final List<Long> polyphenyDbTotalTimes;
    private final List<Long> polyphenyDbTimes;
    private final List<Long> measuredTimes;

    private final Map<Integer, String> queryTypes;
    private final Map<Integer, List<Long>> measuredTimePerQueryType;


    public Gavel( String polyphenyDbUrl, Config config ) {
        super( polyphenyDbUrl );
        this.config = config;
        executionTimes = new ConcurrentHashMap<>();
        totalTimes = new ConcurrentHashMap<>();
        polyphenyDbTotalTimes = Collections.synchronizedList( new LinkedList<>() );
        polyphenyDbTimes = Collections.synchronizedList( new LinkedList<>() );
        measuredTimes = Collections.synchronizedList( new LinkedList<>() );

        queryTypes = new HashMap<>();
        measuredTimePerQueryType = new ConcurrentHashMap<>();
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, Executor executor, boolean warmUp ) throws SQLException {
        log.info( "Analyzing currently stored data..." );
        final int numberOfAuctions = (int) countNumberOfRecords( executor, new CountAuction() );
        final int numberOfUsers = (int) countNumberOfRecords( executor, new CountUser() );
        final int numberOfCategories = (int) countNumberOfRecords( executor, new CountCategory() );
        final int numberOfBids = (int) countNumberOfRecords( executor, new CountBid() );
        log.info( "Current number of elements in the database:\nAuctions: {} | Users: {} | Categories: {} | Bids: {}", numberOfAuctions, numberOfUsers, numberOfCategories, numberOfBids );

        InsertRandomAuction.setNextId( numberOfAuctions + 1 );
        InsertRandomBid.setNextId( numberOfBids + 1 );

        log.info( "Preparing query list for the benchmark..." );
        List<QueryListEntry> queryList = new Vector<>();
        addNumberOfTimes( queryList, new InsertUser(), config.numberOfAddUserQueries );
        addNumberOfTimes( queryList, new ChangePasswordOfRandomUser( numberOfUsers ), config.numberOfChangePasswordQueries );
        addNumberOfTimes( queryList, new InsertRandomAuction( numberOfUsers, numberOfCategories, config ), config.numberOfAddAuctionQueries );
        addNumberOfTimes( queryList, new InsertRandomBid( numberOfAuctions, numberOfUsers ), config.numberOfAddBidQueries );
        addNumberOfTimes( queryList, new ChangeRandomAuction( numberOfAuctions, config ), config.numberOfChangeAuctionQueries );
        addNumberOfTimes( queryList, new SelectRandomAuction( numberOfAuctions ), config.numberOfGetAuctionQueries );
        addNumberOfTimes( queryList, new SelectTheHundredNextEndingAuctionsOfRandomCategory( numberOfCategories, config ), config.numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries );
        addNumberOfTimes( queryList, new SearchAuction(), config.numberOfSearchAuctionQueries );
        addNumberOfTimes( queryList, new CountAuction(), config.numberOfCountAuctionsQueries );
        addNumberOfTimes( queryList, new SelectTopTenCitiesByNumberOfCustomers(), config.numberOfTopTenCitiesByNumberOfCustomersQueries );
        addNumberOfTimes( queryList, new CountBid(), config.numberOfCountBidsQueries );
        addNumberOfTimes( queryList, new SelectRandomBid( numberOfBids ), config.numberOfGetBidQueries );
        addNumberOfTimes( queryList, new SelectRandomUser( numberOfUsers ), config.numberOfGetUserQueries );
        addNumberOfTimes( queryList, new SelectAllBidsOnRandomAuction( numberOfAuctions ), config.numberOfGetAllBidsOnAuctionQueries );
        addNumberOfTimes( queryList, new SelectHighestBidOnRandomAuction( numberOfAuctions ), config.numberOfGetCurrentlyHighestBidOnAuctionQueries );
        Collections.shuffle( queryList );

        if ( outputDirectory != null ) {
            log.info( "Dump query list..." );
            try {
                FileWriter fw = new FileWriter( outputDirectory.getPath() + File.separator + "queryList" );
                queryList.forEach( query -> {
                    try {
                        fw.append( query.query.sqlQuery ).append( "\n" );
                    } catch ( IOException e ) {
                        log.error( "Error while dumping query list", e );
                    }
                } );
                fw.close();
            } catch ( IOException e ) {
                log.error( "Error while dumping query list", e );
            }
        }

        if ( warmUp ) {
            log.info( "Warm-up..." );
            if ( config.numberOfAddUserQueries > 0 ) {
                executor.executeStatement( new InsertUser().getNewQuery() );
            }
            if ( config.numberOfChangePasswordQueries > 0 ) {
                executor.executeStatement( new ChangePasswordOfRandomUser( numberOfUsers ).getNewQuery() );
            }
            if ( config.numberOfAddAuctionQueries > 0 ) {
                executor.executeStatement( new InsertRandomAuction( numberOfUsers, numberOfCategories, config ).getNewQuery() );
            }
            if ( config.numberOfAddBidQueries > 0 ) {
                executor.executeStatement( new InsertRandomBid( numberOfAuctions, numberOfUsers ).getNewQuery() );
            }
            if ( config.numberOfChangeAuctionQueries > 0 ) {
                executor.executeStatement( new ChangeRandomAuction( numberOfAuctions, config ).getNewQuery() );
            }
            if ( config.numberOfGetAuctionQueries > 0 ) {
                executor.executeQuery( new SelectRandomAuction( numberOfAuctions ).getNewQuery() );
            }
            if ( config.numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries > 0 ) {
                executor.executeQuery( new SelectTheHundredNextEndingAuctionsOfRandomCategory( numberOfCategories, config ).getNewQuery() );
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
                executor.executeQuery( new SelectRandomBid( numberOfBids ).getNewQuery() );
            }
            if ( config.numberOfGetUserQueries > 0 ) {
                executor.executeQuery( new SelectRandomUser( numberOfUsers ).getNewQuery() );
            }
            if ( config.numberOfGetAllBidsOnAuctionQueries > 0 ) {
                executor.executeQuery( new SelectAllBidsOnRandomAuction( numberOfAuctions ).getNewQuery() );
            }
            if ( config.numberOfGetCurrentlyHighestBidOnAuctionQueries > 0 ) {
                executor.executeQuery( new SelectHighestBidOnRandomAuction( numberOfAuctions ).getNewQuery() );
            }
            try {
                executor.executeCommit();
                Thread.sleep( 5000 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }
        }

        log.info( "Executing benchmark..." );
        (new Thread( new ProgressReporter.ReportQueryListProgress( queryList, progressReporter ) )).start();
        long startTime = System.nanoTime();

        ArrayList<EvaluationThread> threads = new ArrayList<>();
        for ( int i = 0; i < config.numberOfThreads; i++ ) {
            //TODO
            threads.add( new EvaluationThread( queryList, csvWriter, new PolyphenyDbExecutor( polyphenyDbUrl, config ) ) );
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

        //log.info( "Delete inserted rows" );
        // !!!!!!!!!!!!!! Potential Bug !!!!!!!!!!!!!!!!
        // Using hardcoded id's is not nice! But using the number of entries retrieved before can cause problems if the client is killed.
        /*executor.executeStatement( new DeleteBidsWithIdLargerThan( 62270000 ).getNewQuery() );
        executor.executeStatement( new DeleteUsersWithIdLargerThan( 1200000 ).getNewQuery() );
        executor.executeStatement( new DeleteAuctionsWithIdLargerThan( 546000 ).getNewQuery() );
        executor.executeStatement( new DeleteCategoriesWithIdLargerThan( 35 ).getNewQuery() );
        executor.executeCommit();
        InsertRandomAuction.setNextId( 750000 + 1 );
        InsertRandomBid.setNextId( 71250000 + 1 );*/
        executor.executeCommit();
        return runTime;
    }


    private class EvaluationThread extends Thread {

        private final Executor executor;
        private final List<QueryListEntry> theQueryList;
        private final CsvWriter csvWriter;
        private boolean abort = false;
        @Setter
        private EvaluationThreadMonitor threadMonitor;


        EvaluationThread( List<QueryListEntry> queryList, CsvWriter csvWriter, Executor executor ) {
            super( "EvaluationThread" );
            this.csvWriter = csvWriter;
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
                queryListEntry = theQueryList.remove( 0 );
                try {
                    if ( queryListEntry.query.expectResultSet ) {
                        executor.executeQuery( queryListEntry.query );
                    } else {
                        executor.executeStatement( queryListEntry.query );
                    }
                } catch ( SQLException e ) {
                    log.error( "Caught exception while executing queries", e );
                    threadMonitor.notifyAboutError( e );
                    throw new RuntimeException( e );
                }
                measuredTime = System.nanoTime() - measuredTimeStart;
                if ( csvWriter != null ) {
                    csvWriter.appendToCsv( queryListEntry.query.sqlQuery, measuredTime );
                }
                measuredTimes.add( measuredTime );
                measuredTimePerQueryType.get( queryListEntry.templateId ).add( measuredTime );
                if ( ChronosCommand.commit ) {
                    try {
                        executor.executeCommit();
                    } catch ( SQLException e ) {
                        log.error( "Caught exception while committing", e );
                        threadMonitor.notifyAboutError( e );
                        throw new RuntimeException( e );
                    }
                }
            }

            try {
                executor.executeCommit();
            } catch ( SQLException e ) {
                log.error( "Caught exception while committing", e );
                threadMonitor.notifyAboutError( e );
                throw new RuntimeException( e );
            }

            if ( csvWriter != null ) {
                try {
                    csvWriter.flush();
                } catch ( IOException e ) {
                    threadMonitor.notifyAboutError( e );
                    log.warn( "Exception while flushing csv writer", e );
                }
            }
        }


        public void abort() {
            this.abort = true;
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


    private long countNumberOfRecords( Executor executor, QueryBuilder queryBuilder ) throws SQLException {
        return executor.executeQueryAndGetNumber( queryBuilder.getNewQuery() );
    }


    @Override
    public void buildDatabase( ProgressReporter progressReporter ) throws SQLException {
        log.info( "Generating Data..." );

        DataGenerationThreadMonitor threadMonitor = new DataGenerationThreadMonitor();

        DataGenerator dataGenerator = new DataGenerator( new PolyphenyDbExecutor( polyphenyDbUrl, config ), config, progressReporter, threadMonitor );
        dataGenerator.truncateTables();
        dataGenerator.generateCategories();

        ArrayList<Thread> threads = new ArrayList<>();

        for ( int i = 0; i < config.numberOfUserGenerationThreads; i++ ) {
            Runnable task = () -> {
                try {
                    DataGenerator dg = new DataGenerator( new PolyphenyDbExecutor( polyphenyDbUrl, config ), config, progressReporter, threadMonitor );
                    dg.generateUsers( config.numberOfUsers / config.numberOfUserGenerationThreads );
                } catch ( SQLException e ) {
                    threadMonitor.notifyAboutError( e );
                    log.error( "Exception while generating data", e );
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
                    threadMonitor.notifyAboutError( e );
                    throw new RuntimeException( "Unexpected interrupt", e );
                }
            }
        }

        int rangeSize = config.numberOfAuctions / config.numberOfAuctionGenerationThreads;
        for ( int i = 1; i <= config.numberOfAuctionGenerationThreads; i++ ) {
            final int start = ((i - 1) * rangeSize) + 1;
            final int end = rangeSize * i;
            Runnable task = () -> {
                try {
                    DataGenerator dg = new DataGenerator( new PolyphenyDbExecutor( polyphenyDbUrl, config ), config, progressReporter, threadMonitor );
                    dg.generateAuctions( start, end );
                } catch ( SQLException e ) {
                    threadMonitor.notifyAboutError( e );
                    log.error( "Exception while generating data", e );
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
                threadMonitor.notifyAboutError( e );
                throw new RuntimeException( "Unexpected interrupt", e );
            }
        }

        if ( threadMonitor.aborted ) {
            throw new RuntimeException( "Exception while generating data", threadMonitor.exception );
        }
    }


    static class DataGenerationThreadMonitor {

        private List<DataGenerator> dataGenerators;
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


    public void createSchema() throws SQLException {
        log.info( "Creating schema..." );
        PolyphenyDbExecutor executor = new PolyphenyDbExecutor( polyphenyDbUrl, config );

        InputStreamReader in = new InputStreamReader( ClassLoader.getSystemResourceAsStream( "gavel/schema.sql" ) );
        try ( Stream<String> stream = new BufferedReader( in ).lines() ) {
            stream.forEach( s -> {
                try {
                    executor.executeStatement( new Query( s, false ) );
                } catch ( SQLException e ) {
                    throw new RuntimeException( "Exception while creating schema", e );
                }
            } );
        }
        executor.executeCommit();
    }


    public void analyze( Properties properties ) {

        executionTimes.forEach( ( dataStore, times ) -> {
            properties.put( "executionTime." + dataStore, calculateMean( times ) );
            properties.put( "number." + dataStore, times.size() );
        } );

        totalTimes.forEach( ( dataStore, times ) -> properties.put( "totalTime." + dataStore, calculateMean( times ) ) );

        properties.put( "totalTime", calculateMean( polyphenyDbTotalTimes ) );
        properties.put( "polyphenyDBTime", calculateMean( polyphenyDbTimes ) );
    }


    public void analyzeMeasuredTime( Properties properties ) {
        properties.put( "measuredTime", calculateMean( measuredTimes ) );

        measuredTimePerQueryType.forEach( ( templateId, time ) -> {
            properties.put( "queryTypes_" + templateId + "_mean", calculateMean( time ) );
            //properties.put( "queryTypes_" + templateId + "_all", Joiner.on( ',' ).join( time ) );
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


    public String getTimesAsString( Properties properties ) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append( "\n\nData Store\t\t Execution Time\t\t Total Times\t\t # of Queries\n" );
        for ( Map.Entry<?, ?> entry : properties.entrySet() ) {
            String key = (String) entry.getKey();
            if ( key.startsWith( "executionTime" ) ) {
                String dataStore = key.replace( "executionTime.", "" );
                stringBuilder.append( dataStore ).append( "\t\t" )
                        .append( entry.getValue() ).append( " ms" ).append( "\t\t\t" )
                        .append( properties.get( "totalTime." + dataStore ) ).append( " ms\t\t\t" )
                        .append( properties.get( "number." + dataStore ) ).append( "\n" );
            }
        }
        stringBuilder.append( "\n\n" );
        stringBuilder.append( "Total Time:     " ).append( properties.get( "totalTime" ) ).append( " ms\n" );
        stringBuilder.append( "Polypheny Stack:   " ).append( properties.get( "polyphenyDBTime" ) ).append( " ms\n" );
        return stringBuilder.toString();
    }


    private void addNumberOfTimes( List<QueryListEntry> list, QueryBuilder queryBuilder, int numberOfTimes ) {
        int id = queryTypes.size() + 1;
        queryTypes.put( id, queryBuilder.getNewQuery().sqlQuery );
        measuredTimePerQueryType.put( id, new LinkedList<>() );
        for ( int i = 0; i < numberOfTimes; i++ ) {
            list.add( new QueryListEntry( queryBuilder.getNewQuery(), id ) );
        }
    }
}
