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

package org.polypheny.simpleclient.scenario.multimedia;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.InsertAlbum;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.InsertAlbum.InsertAlbumQuery;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.InsertFriends;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.InsertMedia;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.InsertTimeline;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.InsertUser;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.InsertUser.InsertUserQuery;


@Slf4j
public class DataGenerator {

    private final Executor theExecutor;
    private final MultimediaConfig config;
    private final ProgressReporter progressReporter;

    private final List<BatchableInsert> batchList;
    Map<String, List<Long>> queryTimes = new HashMap<>();

    private boolean aborted;


    DataGenerator( Executor executor, MultimediaConfig config, ProgressReporter progressReporter ) {
        theExecutor = executor;
        this.config = config;
        this.progressReporter = progressReporter;
        batchList = new LinkedList<>();

        aborted = false;
    }


    Map<String, List<Long>> generateUsers() throws ExecutorException {
        int numberOfUsers = config.numberOfUsers;
        int mod = numberOfUsers / progressReporter.base;
        InsertUser insertUser = new InsertUser( config.imgSize );
        for ( int i = 0; i < numberOfUsers; i++ ) {
            if ( aborted ) {
                break;
            }
            if ( mod > 0 && (i % mod) == 0 ) {
                progressReporter.updateProgress();
            }
            InsertUserQuery insertUserQuery = insertUser.getNewQuery();
            addToInsertList( insertUserQuery );
            executeInsertList();

            //add 1 album per user
            InsertAlbum insertAlbum = new InsertAlbum( insertUserQuery.id );
            InsertAlbumQuery insertAlbumQuery = insertAlbum.getNewQuery();
            addToInsertList( insertAlbumQuery );
            executeInsertList();

            //add media data to the album
            for ( int j = 0; j < config.albumSize; j++ ) {
                InsertMedia insertMedia = new InsertMedia( insertAlbumQuery.album_id, config.imgSize, config.numberOfFrames, config.fileSizeKB );
                addToInsertList( insertMedia.getNewQuery() );
            }
            executeInsertList();

            //add posts to timeline
            for ( int j = 0; j < config.postsPerUser; j++ ) {
                InsertTimeline insertTimeline = new InsertTimeline( insertUserQuery.id, config.imgSize, config.numberOfFrames, config.fileSizeKB );
                addToInsertList( insertTimeline.getNewQuery() );
            }
            executeInsertList();

            //add friends of this user
            HashSet<Integer> friends = new HashSet<>( config.numberOfFriends + 1 );
            friends.add( insertUserQuery.id );//to make sure that a user doesn't add himself
            int friend = ThreadLocalRandom.current().nextInt( 1, config.numberOfUsers );
            while ( friends.size() <= config.numberOfFriends ) {
                //add someone that hasn't been added yet
                while ( friends.contains( friend ) ) {
                    friend = ThreadLocalRandom.current().nextInt( 1, config.numberOfUsers );
                }
                friends.add( friend );
                InsertFriends insertFriends = new InsertFriends( insertUserQuery.id, friend );
                addToInsertList( insertFriends.getNewQuery() );
            }
            executeInsertList();
        }
        return queryTimes;
    }


    private void addToInsertList( BatchableInsert query ) throws ExecutorException {
        batchList.add( query );
        if ( batchList.size() >= config.maxBatchSize ) {
            executeInsertList();
        }
    }


    private void executeInsertList() throws ExecutorException {
        if ( batchList.isEmpty() ) {
            return;
        }
        long startTime = System.nanoTime();
        theExecutor.executeInsertList( batchList, config );
        theExecutor.executeCommit();
        long executionTime = System.nanoTime() - startTime;
        ArrayList<Long> executionTimes = new ArrayList<>();
        //add execution n times to get the right average later on
        for ( int i = 0; i < batchList.size(); i++ ) {
            executionTimes.add( executionTime );
        }
        //the batchList contains only queries of one type
        String sql = batchList.getFirst().getParameterizedSqlQuery();
        if ( sql == null ) {
            sql = batchList.getFirst().getSql();
            sql = sql.substring( 0, Math.min( 500, sql.length() ) );
        }
        if ( !queryTimes.containsKey( sql ) ) {
            queryTimes.put( batchList.getFirst().getParameterizedSqlQuery(), new ArrayList<>() );
        }
        queryTimes.get( batchList.getFirst().getParameterizedSqlQuery() ).addAll( executionTimes );
        batchList.clear();
    }


    public void abort() {
        aborted = true;
    }

}
