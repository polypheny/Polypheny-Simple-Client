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


import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.HashMap;
import java.util.Map;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;


public class InsertFriends extends QueryBuilder {

    private final int userId;
    private final int friend;


    public InsertFriends( int userId, int friend ) {
        this.userId = userId;
        this.friend = friend;
    }


    @Override
    public InsertAlbumQuery getNewQuery() {
        return new InsertAlbumQuery(
                userId,
                friend
        );
    }


    public static class InsertAlbumQuery extends BatchableInsert {

        private static final String SQL = "INSERT INTO \"followers\" (\"user_id\", \"friend_id\") VALUES ";
        private final int user;
        private final int friend;


        public InsertAlbumQuery( int user, int friend ) {
            super( false );
            this.user = user;
            this.friend = friend;
        }


        @Override
        public String getSql() {
            return SQL + getSqlRowExpression();
        }


        @Override
        public String getSqlRowExpression() {
            return "("
                    + user + ","
                    + friend + ","
                    + ")";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return SQL + "(?, ?)";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, user ) );
            map.put( 2, new ImmutablePair<>( DataTypes.INTEGER, friend ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return buildRestInsert( "public.followers", ImmutableList.of( getRestRowExpression() ) );
        }


        @Override
        public JsonObject getRestRowExpression() {
            JsonObject row = new JsonObject();
            row.add( "public.followers.user_id", new JsonPrimitive( user ) );
            row.add( "public.followers.friend_id", new JsonPrimitive( friend ) );
            return row;
        }


        @Override
        public String getTable() {
            return "public.followers";
        }

    }

}
