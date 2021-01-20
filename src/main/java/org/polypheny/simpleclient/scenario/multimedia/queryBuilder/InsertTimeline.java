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
import com.google.gson.JsonPrimitive;
import java.io.File;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.MultipartInsert;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.multimedia.MediaGenerator;


public class InsertTimeline extends QueryBuilder {

    private static final AtomicInteger nextId = new AtomicInteger( 1 );

    private final int userId;
    private final int imgSize;
    private final int numberOfFrames;
    private final int fileSizeKB;


    public InsertTimeline( int userId, int imgSize, int numberOfFrames, int fileSizeKB ) {
        this.userId = userId;
        this.imgSize = imgSize;
        this.numberOfFrames = numberOfFrames;
        this.fileSizeKB = fileSizeKB;
    }


    @Override
    public BatchableInsert getNewQuery() {
        Fairy fairy = Fairy.create();
        return new InsertTimelineQuery(
                nextId.getAndIncrement(),
                MediaGenerator.randomTimestamp(),
                userId,
                fairy.textProducer().paragraph( 5 ),
                MediaGenerator.generateRandomImg( imgSize, imgSize ),
                MediaGenerator.generateRandomVideoFile( numberOfFrames, imgSize, imgSize ),
                MediaGenerator.generateRandomWav( fileSizeKB )
        );
    }


    private static class InsertTimelineQuery extends MultipartInsert {

        private static final String SQL = "INSERT INTO \"timeline\" (\"id\", \"timestamp\", \"user_id\", \"message\", \"img\", \"video\", \"sound\") VALUES ";
        private final int timelineId;
        private final Timestamp timestamp;
        private final int userId;
        private final String message;
        private final File img;
        private final File video;
        private final File sound;


        public InsertTimelineQuery( int timelineId, Timestamp timestamp, int userId, String message, File img, File video, File sound ) {
            super( false );
            this.timelineId = timelineId;
            this.timestamp = timestamp;
            this.userId = userId;
            this.message = message;
            this.img = img;
            this.video = video;
            this.sound = sound;
            setFile( "img", img );
            setFile( "video", video );
            setFile( "sound", sound );
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
                    + MediaGenerator.insertByteHexString( MediaGenerator.getAndDeleteFile( img ) ) + ","
                    + MediaGenerator.insertByteHexString( MediaGenerator.getAndDeleteFile( video ) ) + ","
                    + MediaGenerator.insertByteHexString( MediaGenerator.getAndDeleteFile( sound ) )
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
            map.put( 5, new ImmutablePair<>( DataTypes.FILE, img ) );
            map.put( 6, new ImmutablePair<>( DataTypes.FILE, video ) );
            map.put( 7, new ImmutablePair<>( DataTypes.FILE, sound ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }


        @Override
        public JsonObject getRestRowExpression() {
            JsonObject set = new JsonObject();
            String table = getTable() + ".";
            set.add( table + "id", new JsonPrimitive( timelineId ) );
            set.add( table + "timestamp", new JsonPrimitive( timestamp.toLocalDateTime().format( DateTimeFormatter.ISO_LOCAL_DATE_TIME ) ) );
            set.add( table + "user_id", new JsonPrimitive( userId ) );
            set.add( table + "message", new JsonPrimitive( message ) );
            set.add( table + "img", new JsonPrimitive( "img" ) );
            set.add( table + "video", new JsonPrimitive( "video" ) );
            set.add( table + "sound", new JsonPrimitive( "sound" ) );
            return set;

        }


        @Override
        public String getTable() {
            return "public.timeline";
        }

        @Override
        public Map<String, String> getRestParameters() {
            return null;
        }
    }

}
