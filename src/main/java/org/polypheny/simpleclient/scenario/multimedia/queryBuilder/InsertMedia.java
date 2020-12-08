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


import com.google.gson.JsonObject;
import java.io.File;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.multimedia.MediaGenerator;


public class InsertMedia extends QueryBuilder {

    private static final AtomicInteger nextId = new AtomicInteger( 1 );

    private final int albumId;
    private final int minImgSize;
    private final int maxImgSize;
    private final int numberOfFrames;
    private final int minFileSizeKB;
    private final int maxFileSizeKB;


    public InsertMedia( int albumId, int minImgSize, int maxImgSize, int numberOfFrames, int minFileSizeKB, int maxFileSizeKB ) {
        this.albumId = albumId;
        this.minImgSize = minImgSize;
        this.maxImgSize = maxImgSize;
        this.numberOfFrames = numberOfFrames;
        this.minFileSizeKB = minFileSizeKB;
        this.maxFileSizeKB = maxFileSizeKB;
    }


    @Override
    public BatchableInsert getNewQuery() {
        int imgSize = ThreadLocalRandom.current().nextInt( minImgSize, maxImgSize );
        return new InsertMediaQuery(
                nextId.getAndIncrement(),
                MediaGenerator.randomTimestamp(),
                albumId,
                MediaGenerator.generateRandomImg( imgSize, imgSize ),
                MediaGenerator.generateRandomVideoFile( numberOfFrames, imgSize, imgSize ),
                MediaGenerator.generateRandomWav( ThreadLocalRandom.current().nextInt( minFileSizeKB, maxFileSizeKB ) )
        );
    }


    private static class InsertMediaQuery extends BatchableInsert {

        private static final String SQL = "INSERT INTO \"media\" (\"id\", \"timestamp\", \"album_id\", \"img\", \"video\", \"sound\") VALUES ";
        private final int id;
        private final Timestamp timestamp;
        private final int album_id;
        private final File img;
        private final File video;
        private final File sound;


        public InsertMediaQuery( int id, Timestamp timestamp, int album_id, File img, File video, File sound ) {
            super( false );
            this.id = id;
            this.timestamp = timestamp;
            this.album_id = album_id;
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
                    + id + ","
                    + "timestamp '" + timestamp.toString() + "',"
                    + album_id + ","
                    + MediaGenerator.insertByteHexString( MediaGenerator.getAndDeleteFile( img ) ) + ","
                    + MediaGenerator.insertByteHexString( MediaGenerator.getAndDeleteFile( video ) ) + ","
                    + MediaGenerator.insertByteHexString( MediaGenerator.getAndDeleteFile( sound ) )
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
            map.put( 4, new ImmutablePair<>( DataTypes.BYTE_ARRAY, MediaGenerator.getAndDeleteFile( img ) ) );
            map.put( 5, new ImmutablePair<>( DataTypes.BYTE_ARRAY, MediaGenerator.getAndDeleteFile( video ) ) );
            map.put( 6, new ImmutablePair<>( DataTypes.BYTE_ARRAY, MediaGenerator.getAndDeleteFile( sound ) ) );
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
            return "public.media";
        }

    }

}
