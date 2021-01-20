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


import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import kong.unirest.HttpRequest;
import kong.unirest.Unirest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SelectMultipleProfilePics extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;
    private static final int LIMIT = 100;

    private final int numberOfUsers;


    public SelectMultipleProfilePics( int numberOfUsers ) {
        this.numberOfUsers = numberOfUsers;
    }


    @Override
    public Query getNewQuery() {
        int offset;
        if ( numberOfUsers > LIMIT ) {
            offset = ThreadLocalRandom.current().nextInt( 1, numberOfUsers + 1 - LIMIT );
        } else {
            offset = 0;
        }
        return new SelectMultipleProfilePicsQuery( numberOfUsers, offset );
    }


    public static class SelectMultipleProfilePicsQuery extends Query {

        private final int numberOfUsers;
        private final int offset;


        public SelectMultipleProfilePicsQuery( int numberOfUsers, int offset ) {
            super( EXPECT_RESULT );
            this.numberOfUsers = numberOfUsers;
            this.offset = offset;
        }


        @Override
        public String getSql() {
            return "SELECT firstName, profile_pic FROM \"users\" LIMIT " + LIMIT + " OFFSET " + offset;
        }


        @Override
        public String getParameterizedSqlQuery() {
            return null;
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            return null;
        }


        @Override
        public HttpRequest<?> getRest() {
            String table = "public.users.";
            StringJoiner joiner = new StringJoiner( "," );
            joiner.add( table + "firstname" );
            joiner.add( table + "profile_pic" );
            return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.users" )
                    .queryString( "_project", joiner.toString() )
                    .queryString( "_limit", LIMIT )
                    .queryString( "_offset", offset );
        }

    }

}
