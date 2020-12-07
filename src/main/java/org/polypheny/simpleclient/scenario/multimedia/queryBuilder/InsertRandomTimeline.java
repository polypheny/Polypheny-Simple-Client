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

package org.polypheny.simpleclient.scenario.multimedia.queryBuilder;


import com.devskiller.jfairy.Fairy;
import com.google.gson.JsonObject;
import java.io.File;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.HttpRequest;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.multimedia.MediaGenerator;


public class InsertRandomTimeline extends QueryBuilder {

    @Getter
    private static final AtomicInteger nextId = new AtomicInteger( 1 );

    private final int numberOfUsers;
    private final int minImgSize;
    private final int maxImgSize;
    private final int numberOfFrames;
    private final int minFileSizeKB;
    private final int maxFileSizeKB;


    public InsertRandomTimeline( int numberOfUsers, int minImgSize, int maxImgSize, int numberOfFrames, int minFileSizeKB, int maxFileSizeKB ) {
        this.numberOfUsers = numberOfUsers;
        this.minImgSize = minImgSize;
        this.maxImgSize = maxImgSize;
        this.numberOfFrames = numberOfFrames;
        this.minFileSizeKB = minFileSizeKB;
        this.maxFileSizeKB = maxFileSizeKB;
    }

    @Override
    public BatchableInsert getNewQuery() {
        int imgSize = ThreadLocalRandom.current().nextInt( minImgSize, maxImgSize );
        Fairy fairy = Fairy.create();
        return new InsertRandomTimelineQuery(
                nextId.getAndIncrement(),
                MediaGenerator.randomTimestamp(),
                ThreadLocalRandom.current().nextInt( 1, numberOfUsers ),
                fairy.textProducer().paragraph( 5 ),
                MediaGenerator.generateRandomImg( imgSize, imgSize ),
                MediaGenerator.generateRandomVideoFile( numberOfFrames, imgSize, imgSize ),
                MediaGenerator.generateRandomWav( ThreadLocalRandom.current().nextInt( minFileSizeKB, maxFileSizeKB ) )
        );
    }


    private static class InsertRandomTimelineQuery extends BatchableInsert {

        private static final String SQL = "INSERT INTO \"timeline\" (\"id\", \"timestamp\", \"user_id\", \"message\", \"img\", \"video\", \"sound\") VALUES ";
        private final int timelineId;
        private final Timestamp timestamp;
        private final int userId;
        private final String message;
        private final File img;
        private final File video;
        private final File sound;


        public InsertRandomTimelineQuery( int timelineId, Timestamp timestamp, int userId, String message, File img, File video, File sound ) {
            super( false );
            this.timelineId = timelineId;
            this.timestamp = timestamp;
            this.userId = userId;
            this.message = message;
            this.img = img;
            this.video = video;
            this.sound = sound;
        }

        @Override
        public String getSql() {
            return SQL + getSqlRowExpression();
        }


        @Override
        public String getSqlRowExpression() {
            return "("
                    + timelineId + ","
                    + "timestamp '" + timestamp.toString() + "',"
                    + userId + ","
                    + "'" + message + "',"
                    + MediaGenerator.insertByteHexString( MediaGenerator.getAndDeleteFile( img, 2 ) ) + ","
                    + MediaGenerator.insertByteHexString( MediaGenerator.getAndDeleteFile( video, 2 ) ) + ","
                    + MediaGenerator.insertByteHexString( MediaGenerator.getAndDeleteFile( sound, 2 ) )
                    + ")";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return SQL + "(?, ?, ?, ?, ?, ?, ?)";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, timelineId ) );
            map.put( 2, new ImmutablePair<>( DataTypes.TIMESTAMP, timestamp ) );
            map.put( 3, new ImmutablePair<>( DataTypes.INTEGER, userId ) );
            map.put( 4, new ImmutablePair<>( DataTypes.VARCHAR, message ) );
            map.put( 5, new ImmutablePair<>( DataTypes.BYTE_ARRAY, MediaGenerator.getAndDeleteFile( img, 2 ) ) );
            map.put( 6, new ImmutablePair<>( DataTypes.BYTE_ARRAY, MediaGenerator.getAndDeleteFile( video, 2 ) ) );
            map.put( 7, new ImmutablePair<>( DataTypes.BYTE_ARRAY, MediaGenerator.getAndDeleteFile( sound, 2 ) ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }


        @Override
        public JsonObject getRestRowExpression() {
            return null;
        }


        @Override
        public String getTable() {
            return "public.timeline";
        }

    }

}
