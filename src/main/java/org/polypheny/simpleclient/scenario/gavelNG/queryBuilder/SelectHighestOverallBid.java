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

package org.polypheny.simpleclient.scenario.gavelNG.queryBuilder;

import java.util.HashMap;
import java.util.Map;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SelectHighestOverallBid extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;
    private final QueryMode queryMode;


    public SelectHighestOverallBid( QueryMode queryMode ) {
        this.queryMode = queryMode;
    }


    @Override
    public Query getNewQuery() {
        return new SelectHighestOverallBidQuery( queryMode );
    }


    private static class SelectHighestOverallBidQuery extends Query {

        private final QueryMode queryMode;


        public SelectHighestOverallBidQuery( QueryMode queryMode ) {
            super( EXPECT_RESULT );
            this.queryMode = queryMode;
        }


        @Override
        public String getSql() {
            if ( queryMode.equals( QueryMode.MATERIALIZED ) ) {
                return "SELECT * FROM highestBid_materialized LIMIT 1";
            } else if ( queryMode.equals( QueryMode.VIEW ) ) {
                return "SELECT * FROM highestBid_view LIMIT 1";
            } else {
                return "SELECT last_name, first_name "
                        + "FROM \"user\" "
                        + "WHERE \"user\".id = (SELECT highest.highestUser FROM (SELECT bid.\"user\" as highestUser, MAX( bid.amount) "
                        + "FROM public.bid "
                        + "GROUP BY bid.\"user\" "
                        + "ORDER BY MAX( bid.amount) DESC) as highest Limit 1) LIMIT 1";
            }
        }


        @Override
        public String getParameterizedSqlQuery() {
            return getSql();
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            return new HashMap<>();
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;

            /*
            if ( queryMode.equals( QueryMode.VIEW ) ) {
                return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.highestBid_view" )
                        .queryString( "_limit", 100 );
            } else if ( queryMode.equals( QueryMode.MATERIALIZED ) ) {
                return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.highestBid_materialized" )
                        .queryString( "_limit", 100 );
            } else {
                return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.user" )
                        .queryString( "_project", "public.user.last_name,public.user.first_name")
                        .queryString( "public.bid.user", "=" + "public.user.id" )
                        .queryString( "public.bid.auction", "=" + "public.auction.id" )
                        .queryString( "public.picture.auction", "=" + "public.auction.id" )
                        .queryString( "public.auction.user", "=" + "public.user.id" )
                        .queryString( "public.auction.category", "=" + "public.category.id" )
                        .queryString( "_limit", 100 );
            }
             */
        }


        @Override
        public String getMongoQl() {
            // $lookup is not supported // substitute query

            return "db.bid.aggregate(["
                    + "{\"$group\":{\"_id\": \"user\", \"max_amount\":{\"$max\": \"amount\"}}},"
                    + "{\"$sort\":{\"max_amount\": -1 }},"
                    + "{\"$project\":{\"highestUser\": \"$user\", \"max_amount\": 1}},"
                    + "{\"$limit\": 1}])";
        }

    }

}

