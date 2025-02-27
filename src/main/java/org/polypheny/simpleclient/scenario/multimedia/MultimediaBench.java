/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2023 The Polypheny Project
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


import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import kong.unirest.core.Unirest;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.scenario.PolyphenyScenario;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.CreateTable;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.DeleteRandomTimeline;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.InsertRandomTimeline;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.SelectMediaWhereAlbum;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.SelectMultipleProfilePics;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.SelectRandomProfilePic;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.SelectRandomTimeline;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.SelectRandomUser;


@Slf4j
public class MultimediaBench extends PolyphenyScenario {

    private final MultimediaConfig config;

    private final List<Long> measuredTimes;
    private final Map<Integer, String> queryTypes;
    private final Map<Integer, List<Long>> measuredTimePerQueryType;


    static {
        Unirest.config().reset();
        Unirest.config().connectTimeout( 0 );
    }


    public MultimediaBench( Executor.ExecutorFactory executorFactory, MultimediaConfig config, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        // Never dump mm queries, because the dumps can get very large for large binary inserts
        super( executorFactory, commitAfterEveryQuery, false, QueryMode.TABLE );
        if ( dumpQueryList ) {
            log.warn( "Ignoring dump query parameter for MultiMediaBench since the dump would get extremely large!" );
        }
        this.config = config;

        measuredTimes = Collections.synchronizedList( new LinkedList<>() );
        queryTypes = new HashMap<>();
        measuredTimePerQueryType = new ConcurrentHashMap<>();

        // Make sure the tmp folder exists
        new File( System.getProperty( "user.home" ), ".polypheny/tmp/" ).mkdirs();
    }


    @Override
    public void createSchema( DatabaseInstance databaseInstance, boolean includingKeys ) {
        if ( queryMode != QueryMode.TABLE ) {
            throw new UnsupportedOperationException( "Unsupported query mode: " + queryMode.name() );
        }

        log.info( "Creating schema..." );
        Executor executor = null;
        try {
            executor = executorFactory.createExecutorInstance();
            String onStore = String.format( " ON STORE \"%s\"", findMatchingDataStoreName( config.dataStore ) );
            executor.executeQuery( (new CreateTable( "ALTER CONFIG 'validation/validateMultimediaContentType' SET FALSE" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "CREATE TABLE IF NOT EXISTS \"users\" (\"id\" INTEGER NOT NULL, \"firstName\" VARCHAR(1000) NOT NULL, \"lastName\" VARCHAR(1000) NOT NULL, \"email\" VARCHAR(1000) NOT NULL, \"password\" VARCHAR(1000) NOT NULL, \"profile_pic\" IMAGE NOT NULL, PRIMARY KEY(\"id\"))" + onStore )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "CREATE TABLE IF NOT EXISTS \"album\" (\"id\" INTEGER NOT NULL, \"user_id\" INTEGER NOT NULL, \"name\" VARCHAR(200) NOT NULL, PRIMARY KEY(\"id\"))" + onStore )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "CREATE TABLE IF NOT EXISTS \"media\" (\"id\" INTEGER NOT NULL, \"timestamp\" TIMESTAMP NOT NULL, \"album_id\" INTEGER NOT NULL, \"img\" IMAGE, \"video\" VIDEO, \"audio\" AUDIO, PRIMARY KEY(\"id\"))" + onStore )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "CREATE TABLE IF NOT EXISTS \"timeline\" (\"id\" INTEGER NOT NULL, \"timestamp\" TIMESTAMP NOT NULL, \"user_id\" INTEGER NOT NULL, \"message\" VARCHAR(2000), \"img\" IMAGE, \"video\" VIDEO, \"audio\" AUDIO, PRIMARY KEY(\"id\"))" + onStore )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "CREATE TABLE IF NOT EXISTS \"followers\" (\"user_id\" INTEGER NOT NULL,\"friend_id\" INTEGER NOT NULL, PRIMARY KEY(\"user_id\", \"friend_id\"))" + onStore )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "TRUNCATE TABLE \"users\"" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "TRUNCATE TABLE \"album\"" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "TRUNCATE TABLE \"media\"" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "TRUNCATE TABLE \"timeline\"" )).getNewQuery() );
            executor.executeQuery( (new CreateTable( "TRUNCATE TABLE \"followers\"" )).getNewQuery() );

            if ( !config.multimediaStore.equals( "same" ) && !config.dataStore.equals( config.multimediaStore ) ) {
                String ds = findMatchingDataStoreName( config.dataStore );
                String dsMultimedia = findMatchingDataStoreName( config.multimediaStore );
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"users\" ADD PLACEMENT (\"profile_pic\") ON STORE \"" + dsMultimedia + "\"" )).getNewQuery() );
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"media\" ADD PLACEMENT (\"img\", \"video\", \"audio\") ON STORE \"" + dsMultimedia + "\"" )).getNewQuery() );
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"timeline\" ADD PLACEMENT (\"img\", \"video\", \"audio\") ON STORE \"" + dsMultimedia + "\"" )).getNewQuery() );

                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"users\" MODIFY PLACEMENT (\"id\", \"firstName\", \"lastName\", \"email\", \"password\") ON STORE \"" + ds + "\"" )).getNewQuery() );
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"media\" MODIFY PLACEMENT (\"id\", \"timestamp\", \"album_id\") ON STORE \"" + ds + "\"" )).getNewQuery() );
                executor.executeQuery( (new CreateTable( "ALTER TABLE public.\"timeline\" MODIFY PLACEMENT (\"id\", \"timestamp\", \"user_id\", \"message\") ON STORE \"" + ds + "\"" )).getNewQuery() );
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
    public void generateData( DatabaseInstance databaseInstance, ProgressReporter progressReporter ) {
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

        return commonExecute( queryList, progressReporter, outputDirectory, numberOfThreads, Query::getSql, () -> executorFactory.createExecutorInstance( csvWriter ), new Random() );
    }


    @Override
    public void warmUp( ProgressReporter progressReporter ) {
        log.info( "Warm-up..." );

        Executor executor = null;

        for ( int i = 0; i < config.numberOfWarmUpIterations; i++ ) {
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


    @Override
    public int getNumberOfInsertThreads() {
        return 1;
    }


    private void addNumberOfTimes( List<QueryListEntry> list, QueryBuilder queryBuilder, int numberOfTimes ) {
        int id = queryTypes.size() + 1;
        String sql = queryBuilder.getNewQuery().getSql();
        // Cut long queries with binary data
        queryTypes.put( id, sql.substring( 0, Math.min( 500, sql.length() ) ) );
        measuredTimePerQueryType.put( id, Collections.synchronizedList( new LinkedList<>() ) );
        for ( int i = 0; i < numberOfTimes; i++ ) {
            list.add( new QueryListEntry( queryBuilder.getNewQuery(), id ) );
        }
    }

}
