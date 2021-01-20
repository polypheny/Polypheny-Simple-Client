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


import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import kong.unirest.HttpRequest;
import kong.unirest.Unirest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SelectTimelineWhereUser extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;
    private static final int LIMIT = 20;

    private final int numberOfUsers;


    public SelectTimelineWhereUser( int numberOfUsers ) {
        this.numberOfUsers = numberOfUsers;
    }


    @Override
    public Query getNewQuery() {
        int userId = ThreadLocalRandom.current().nextInt( 1, numberOfUsers + 1 );
        return new SelectTimelineWhereUserQuery( userId );
    }


    public static class SelectTimelineWhereUserQuery extends Query {

        private final int userId;


        public SelectTimelineWhereUserQuery( int userId ) {
            super( EXPECT_RESULT );
            this.userId = userId;
        }


        @Override
        public String getSql() {
            return "SELECT \"message\", \"img\", \"video\", \"sound\" FROM \"timeline\" WHERE \"user_id\" = " + userId + " LIMIT " + LIMIT;
        }


        @Override
        public String getParameterizedSqlQuery() {
            return "SELECT \"message\", \"img\", \"video\", \"sound\" FROM \"timeline\" WHERE \"user_id\" = ? LIMIT " + LIMIT;
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, userId ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            String table = "public.timeline.";
            StringJoiner joiner = new StringJoiner( "," );
            joiner.add( table + "message" );
            joiner.add( table + "img" );
            joiner.add( table + "video" );
            joiner.add( table + "sound" );
            return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.timeline" )
                    .queryString( "public.timeline.user_id", "=" + userId )
                    .queryString( "_project", joiner.toString() )
                    .queryString( "_limit", LIMIT );
        }

    }

}
