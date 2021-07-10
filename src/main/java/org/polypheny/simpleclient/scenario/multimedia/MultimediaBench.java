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

package org.polypheny.simpleclient.scenario.multimedia;


import com.google.common.base.Joiner;
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
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import kong.unirest.Unirest;
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
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.CreateTable;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.DeleteRandomTimeline;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.InsertRandomTimeline;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.SelectMediaWhereAlbum;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.SelectMultipleProfilePics;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.SelectRandomProfilePic;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.SelectRandomTimeline;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.SelectRandomUser;


@Slf4j
public class MultimediaBench extends Scenario {

    private final MultimediaConfig config;

    private final List<Long> measuredTimes;
    private final Map<Integer, String> queryTypes;
    private final Map<Integer, List<Long>> measuredTimePerQueryType;

    static {
        Unirest.config().reset();
        Unirest.config().socketTimeout( 0 );
    }

    public MultimediaBench( Executor.ExecutorFactory executorFactory, MultimediaConfig config, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        //never dump mm queries, because the dumps can get very large for large binary inserts
        super( executorFactory, commitAfterEveryQuery, false,false );
        this.config = config;

        measuredTimes = Collections.synchronizedList( new LinkedList<>() );
        queryTypes = new HashMap<>();
        measuredTimePerQueryType = new ConcurrentHashMap<>();

        //make sure the tmp folder exists
        new File( System.getProperty( "user.home" ), ".polypheny/tmp/" ).mkdirs();
    }


    @Override
    public void createSchema( boolean includingKeys ) {
        log.info( "Creating schema..." );
        Executor executor = null;

        try {
            executor = executorFactory.createExecutorInstance();
            String onStore = String.format( " ON STORE \"%s\"", config.dataStore );
            executor.executeQuery( (new CreateTable( "ALTER CONFIG 'validation/validateMultimediaContentType' SET FALSE" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "CREATE TABLE IF NOT EXISTS \"users\" (\"id\" INTEGER NOT NULL, \"firstName\" VARCHAR(1000) NOT NULL, \"lastName\" VARCHAR(1000) NOT NULL, \"email\" VARCHAR(1000) NOT NULL, \"password\" VARCHAR(1000) NOT NULL, \"profile_pic\" IMAGE NOT NULL, PRIMARY KEY(\"id\"))" + onStore )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "CREATE TABLE IF NOT EXISTS \"album\" (\"id\" INTEGER NOT NULL, \"user_id\" INTEGER NOT NULL, \"name\" VARCHAR(200) NOT NULL, PRIMARY KEY(\"id\"))" + onStore )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "CREATE TABLE IF NOT EXISTS \"media\" (\"id\" INTEGER NOT NULL, \"timestamp\" TIMESTAMP NOT NULL, \"album_id\" INTEGER NOT NULL, \"img\" IMAGE, \"video\" VIDEO, \"sound\" SOUND, PRIMARY KEY(\"id\"))" + onStore )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "CREATE TABLE IF NOT EXISTS \"timeline\" (\"id\" INTEGER NOT NULL, \"timestamp\" TIMESTAMP NOT NULL, \"user_id\" INTEGER NOT NULL, \"message\" VARCHAR(2000), \"img\" IMAGE, \"video\" VIDEO, \"sound\" SOUND, PRIMARY KEY(\"id\"))" + onStore )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "CREATE TABLE IF NOT EXISTS \"followers\" (\"user_id\" INTEGER NOT NULL,\"friend_id\" INTEGER NOT NULL, PRIMARY KEY(\"user_id\", \"friend_id\"))" + onStore )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "TRUNCATE TABLE \"users\"" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "TRUNCATE TABLE \"album\"" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "TRUNCATE TABLE \"media\"" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "TRUNCATE TABLE \"timeline\"" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "TRUNCATE TABLE \"followers\"" )).getNewQuery() );

            if ( !config.multimediaStore.equals( "same" ) && !config.dataStore.equals( config.multimediaStore ) ) {
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"users\" ADD PLACEMENT (\"profile_pic\") ON STORE \"" + config.multimediaStore + "\"" )).getNewQuery() );
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"media\" ADD PLACEMENT (\"img\", \"video\", \"sound\") ON STORE \"" + config.multimediaStore + "\"" )).getNewQuery() );
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"timeline\" ADD PLACEMENT (\"img\", \"video\", \"sound\") ON STORE \"" + config.multimediaStore + "\"" )).getNewQuery() );

                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"users\" MODIFY PLACEMENT (\"id\", \"firstName\", \"lastName\", \"email\", \"password\") ON STORE \"" + config.dataStore + "\"" )).getNewQuery() );
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"media\" MODIFY PLACEMENT (\"id\", \"timestamp\", \"album_id\") ON STORE \"" + config.dataStore + "\"" )).getNewQuery() );
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"timeline\" MODIFY PLACEMENT (\"id\", \"timestamp\", \"user_id\", \"message\") ON STORE \"" + config.dataStore + "\"" )).getNewQuery() );
            }

            if ( includingKeys ) {
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"album\" ADD CONSTRAINT \"fk1\" FOREIGN KEY(\"user_id\") REFERENCES \"users\"(\"id\") ON UPDATE RESTRICT ON DELETE RESTRICT" )).getNewQuery() );
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"media\" ADD CONSTRAINT \"fk2\" FOREIGN KEY(\"album_id\") REFERENCES \"album\"(\"id\") ON UPDATE RESTRICT ON DELETE RESTRICT" )).getNewQuery() );
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"timeline\" ADD CONSTRAINT \"fk3\" FOREIGN KEY(\"user_id\") REFERENCES \"users\"(\"id\") ON UPDATE RESTRICT ON DELETE RESTRICT" )).getNewQuery() );
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"followers\" ADD CONSTRAINT \"fk4\" FOREIGN KEY(\"user_id\") REFERENCES \"users\"(\"id\") ON UPDATE RESTRICT ON DELETE RESTRICT" )).getNewQuery() );
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"followers\" ADD CONSTRAINT \"fk5\" FOREIGN KEY(\"friend_id\") REFERENCES \"users\"(\"id\") ON UPDATE RESTRICT ON DELETE RESTRICT" )).getNewQuery() );
            }
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
            Map<String, List<Long>> executionTimes = dataGenerator.generateUsers();
            executionTimes.forEach( ( s, l ) -> {
                measuredTimes.addAll( l );
                int id = queryTypes.size() + 1;
                queryTypes.put( id, s );
                measuredTimePerQueryType.put( id, l );
            } );
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
        addNumberOfTimes( queryList, new SelectRandomUser( config.numberOfUsers ), config.numberOfSelectUserQueries );
        addNumberOfTimes( queryList, new SelectRandomProfilePic( config.numberOfUsers ), config.numberOfSelectProfilePicQueries );
        addNumberOfTimes( queryList, new SelectMultipleProfilePics( config.numberOfUsers ), config.numberOfSelectProfilePicsQueries );
        addNumberOfTimes( queryList, new SelectMediaWhereAlbum( config.numberOfUsers ), config.numberOfSelectMediaQueries );//numberOfAlbums = numberOfUsers (1 album per user)
        addNumberOfTimes( queryList, new SelectRandomTimeline( config.numberOfUsers * config.postsPerUser ), config.numberOfSelectTimelineQueries );
        addNumberOfTimes( queryList, new DeleteRandomTimeline( config.numberOfUsers * config.postsPerUser ), config.numberOfDeleteTimelineQueries );
        addNumberOfTimes( queryList, new InsertRandomTimeline( config.numberOfUsers, config.postsPerUser, config.imgSize, config.numberOfFrames, config.fileSizeKB, false ), config.numberOfInsertTimelineQueries );

        Collections.shuffle( queryList );

        // This dumps the sql queries independent of the selected interface
        // always false for the MultimediaBench
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

        for ( int i = 0; i < iterations; i++ ) {
            try {
                executor = executorFactory.createExecutorInstance();
                if ( config.numberOfUsers > 0 ) {
                    executor.executeQuery( new SelectRandomUser( config.numberOfUsers ).getNewQuery() );
                    executor.executeQuery( new SelectRandomProfilePic( config.numberOfUsers ).getNewQuery() );
                    executor.executeQuery( new SelectMultipleProfilePics( config.numberOfUsers ).getNewQuery() );
                    executor.executeQuery( new SelectMediaWhereAlbum( config.numberOfUsers ).getNewQuery() );//numberOfAlbums = numberOfUsers (1 album per user)
                    executor.executeQuery( new SelectRandomTimeline( config.numberOfUsers * config.postsPerUser ).getNewQuery() );
                    executor.executeQuery( new DeleteRandomTimeline( config.numberOfUsers * config.postsPerUser ).getNewQuery() );
                    executor.executeQuery( new InsertRandomTimeline( config.numberOfUsers, config.postsPerUser, config.imgSize, config.numberOfFrames, config.fileSizeKB, true ).getNewQuery() );
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
        return 1;
    }


    private void addNumberOfTimes( List<QueryListEntry> list, QueryBuilder queryBuilder, int numberOfTimes ) {
        int id = queryTypes.size() + 1;
        String sql = queryBuilder.getNewQuery().getSql();
        //cut long queries with binary data
        queryTypes.put( id, sql.substring( 0, Math.min( 500, sql.length() ) ) );
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
