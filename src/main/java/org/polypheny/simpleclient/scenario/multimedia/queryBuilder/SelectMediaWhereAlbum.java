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


import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import kong.unirest.core.HttpRequest;
import kong.unirest.core.Unirest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SelectMediaWhereAlbum extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;
    private static final int LIMIT = 4;

    private final int numberOfAlbums;


    public SelectMediaWhereAlbum( int numberOfAlbums ) {
        this.numberOfAlbums = numberOfAlbums;
    }


    @Override
    public Query getNewQuery() {
        int albumId = ThreadLocalRandom.current().nextInt( 1, numberOfAlbums + 1 );
        return new SelectMediaWhereAlbumQuery( albumId );
    }


    public static class SelectMediaWhereAlbumQuery extends Query {

        private final int albumId;


        public SelectMediaWhereAlbumQuery( int albumId ) {
            super( EXPECT_RESULT );
            this.albumId = albumId;
        }


        @Override
        public String getSql() {
            return "SELECT \"timestamp\", \"img\", \"video\", \"audio\" FROM \"public\".\"media\" WHERE \"album_id\" = " + albumId + " LIMIT " + LIMIT;
        }


        @Override
        public String getParameterizedSqlQuery() {
            return "SELECT \"timestamp\", \"img\", \"video\", \"audio\" FROM \"public\".\"media\" WHERE \"album_id\" = ? LIMIT " + LIMIT;
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, albumId ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            String table = "public.media.";
            StringJoiner joiner = new StringJoiner( "," );
            joiner.add( table + "timestamp" );
            joiner.add( table + "img" );
            joiner.add( table + "video" );
            joiner.add( table + "audio" );
            return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.media" )
                    .queryString( "public.media.album_id", "=" + albumId )
                    .queryString( "_project", joiner.toString() )
                    .queryString( "_limit", LIMIT );
        }


        @Override
        public String getMongoQl() {
            return null;
        }

    }

}
