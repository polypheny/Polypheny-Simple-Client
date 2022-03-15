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

package org.polypheny.simpleclient.scenario.gavelEx.queryBuilder;


import java.util.HashMap;
import java.util.Map;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SelectDifferenceBetweenLowestAndHighestBid extends QueryBuilder {


    private static final boolean EXPECT_RESULT = true;
    private final QueryMode queryMode;


    public SelectDifferenceBetweenLowestAndHighestBid( QueryMode queryMode ) {
        this.queryMode = queryMode;
    }


    @Override
    public Query getNewQuery() {
        return new SelectDifferenceBetweenLowestAndHighestBidQuery( queryMode );
    }


    private static class SelectDifferenceBetweenLowestAndHighestBidQuery extends Query {

        private final QueryMode queryMode;


        public SelectDifferenceBetweenLowestAndHighestBidQuery( QueryMode queryMode ) {
            super( EXPECT_RESULT );
            this.queryMode = queryMode;
        }


        @Override
        public String getSql() {
            if ( queryMode.equals( QueryMode.MATERIALIZED ) ) {
                return "SELECT * FROM differenceBetweenLowestAndHighestBid_materialized LIMIT 100";
            } else if ( queryMode.equals( QueryMode.VIEW ) ) {
                return "SELECT * FROM differenceBetweenLowestAndHighestBid_view LIMIT 100";
            } else {
                return "SELECT difference, title "
                        + "FROM "
                        + "(SELECT (a.maxamount - a.minamount) as difference, a.auctionid as auctionid "
                        + "FROM "
                        + "(SELECT min(\"public\".\"bid\".\"amount\") as \"minamount\", max(\"public\".\"bid\".\"amount\") as \"maxamount\", \"public\".\"bid\".\"auction\" as \"auctionid\" "
                        + "FROM \"public\".\"bid\" "
                        + "WHERE \"public\".\"bid\".\"auction\" in ( SELECT \"public\".\"auction\".\"id\" FROM \"public\".\"auction\" ) "
                        + "GROUP BY \"public\".\"bid\".\"auction\") as \"a\")as \"b\" "
                        + "JOIN "
                        + "(SELECT \"public\".\"auction\".\"title\" as \"title\", \"public\".\"auction\".\"id\" as \"auctionid\" "
                        + "FROM \"public\".\"auction\") as \"c\" "
                        + "on b.auctionid = c.auctionid "
                        + "GROUP BY title, difference "
                        + "LIMIT 100 ";
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
        }


        @Override
        public String getMongoQl() {
            // $lookup not yet supported
            return null;
        }

    }

}
