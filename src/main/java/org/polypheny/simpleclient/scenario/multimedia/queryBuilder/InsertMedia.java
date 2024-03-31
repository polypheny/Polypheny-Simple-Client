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

package org.polypheny.simpleclient.scenario.multimedia.queryBuilder;


import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.File;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.HttpRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.MultipartInsert;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.multimedia.MediaGenerator;


@Slf4j
public class InsertMedia extends QueryBuilder {

    private static final AtomicInteger nextId = new AtomicInteger( 1 );

    private final int albumId;
    private final int imgSize;
    private final int numberOfFrames;
    private final int fileSizeKB;


    public InsertMedia( int albumId, int imgSize, int numberOfFrames, int fileSizeKB ) {
        this.albumId = albumId;
        this.imgSize = imgSize;
        this.numberOfFrames = numberOfFrames;
        this.fileSizeKB = fileSizeKB;
    }


    @Override
    public BatchableInsert getNewQuery() {
        return new InsertMediaQuery(
                nextId.getAndIncrement(),
                MediaGenerator.randomTimestamp(),
                albumId,
                MediaGenerator.generateRandomImg( imgSize, imgSize ),
                MediaGenerator.generateRandomVideoFile( numberOfFrames, imgSize, imgSize ),
                MediaGenerator.generateRandomWav( fileSizeKB )
        );
    }


    private static class InsertMediaQuery extends MultipartInsert {

        private static final String SQL = "INSERT INTO \"media\" (\"id\", \"timestamp\", \"album_id\", \"img\", \"video\", \"audio\") VALUES ";
        private final int id;
        private final Timestamp timestamp;
        private final int album_id;
        private final File img;
        private final File video;
        private final File audio;


        public InsertMediaQuery( int id, Timestamp timestamp, int album_id, File img, File video, File audio ) {
            super( false );
            this.id = id;
            this.timestamp = timestamp;
            this.album_id = album_id;
            this.img = img;
            this.video = video;
            this.audio = audio;
            setFile( "img", img );
            setFile( "video", video );
            setFile( "audio", this.audio );
        }


        @Override
        public String getSql() {
            return SQL + getSqlRowExpression();
        }


        @Override
        public String getSqlRowExpression() {
            return "("
                    + id + ","
                    + "timestamp '" + timestamp.toString() + "',"
                    + album_id + ","
                    + MediaGenerator.insertByteHexString( MediaGenerator.getAndDeleteFile( img, 2 ) ) + ","
                    + MediaGenerator.insertByteHexString( MediaGenerator.getAndDeleteFile( video, 2 ) ) + ","
                    + MediaGenerator.insertByteHexString( MediaGenerator.getAndDeleteFile( audio, 2 ) )
                    + ")";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return SQL + "(?, ?, ?, ?, ?, ?)";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, id ) );
            map.put( 2, new ImmutablePair<>( DataTypes.TIMESTAMP, timestamp ) );
            map.put( 3, new ImmutablePair<>( DataTypes.INTEGER, album_id ) );
            map.put( 4, new ImmutablePair<>( DataTypes.FILE, img ) );
            map.put( 5, new ImmutablePair<>( DataTypes.FILE, video ) );
            map.put( 6, new ImmutablePair<>( DataTypes.FILE, audio ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }


        @Override
        public String getMongoQl() {
            return null;
        }


        @Override
        public JsonObject getRestRowExpression() {
            JsonObject set = new JsonObject();
            String table = getEntity() + ".";
            set.add( table + "id", new JsonPrimitive( id ) );
            set.add( table + "timestamp", new JsonPrimitive( timestamp.toLocalDateTime().format( DateTimeFormatter.ISO_LOCAL_DATE_TIME ) ) );
            set.add( table + "album_id", new JsonPrimitive( album_id ) );
            set.add( table + "img", new JsonPrimitive( "img" ) );
            set.add( table + "video", new JsonPrimitive( "video" ) );
            set.add( table + "audio", new JsonPrimitive( "audio" ) );
            return set;
        }


        @Override
        public String getEntity() {
            return "public.media";
        }


        @Override
        public Map<String, String> getRestParameters() {
            return null;
        }

    }

}
