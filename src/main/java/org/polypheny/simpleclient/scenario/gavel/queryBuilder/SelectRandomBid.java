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

package org.polypheny.simpleclient.scenario.gavel.queryBuilder;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import kong.unirest.HttpRequest;
import kong.unirest.Unirest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SelectRandomBid extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final int numberOfBids;
    private final QueryMode queryMode;


    public SelectRandomBid( int numberOfBids, QueryMode queryMode ) {
        this.numberOfBids = numberOfBids;
        this.queryMode = queryMode;
    }


    @Override
    public Query getNewQuery() {
        int bidId = ThreadLocalRandom.current().nextInt( 1, numberOfBids + 1 );
        return new SelectRandomBidQuery( bidId, queryMode );
    }


    private static class SelectRandomBidQuery extends Query {

        private final int bidId;
        private final String tableName;


        public SelectRandomBidQuery( int bidId, QueryMode queryMode ) {
            super( EXPECT_RESULT );
            this.bidId = bidId;

            if ( queryMode.equals( QueryMode.VIEW ) ) {
                tableName = "bid_view";
            } else if ( queryMode.equals( QueryMode.MATERIALIZED ) ) {
                tableName = "bid_materialized";
            } else {
                tableName = "bid";
            }
        }


        @Override
        public String getSql() {
            return "SELECT * FROM " + tableName + " b WHERE b.id=" + bidId;
        }


        @Override
        public String getParameterizedSqlQuery() {
            return "SELECT * FROM " + tableName + " b WHERE b.id=?";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, bidId ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public." + tableName )
                    .queryString( "public." + tableName + ".id", "=" + bidId );
        }

    }

}
