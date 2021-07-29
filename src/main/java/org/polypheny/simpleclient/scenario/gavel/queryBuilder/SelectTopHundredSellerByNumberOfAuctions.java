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
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.QueryView;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SelectTopHundredSellerByNumberOfAuctions extends QueryBuilder {


    private static final boolean EXPECT_RESULT = true;
    private final QueryView queryView;


    public SelectTopHundredSellerByNumberOfAuctions( QueryView queryView ) {
        this.queryView = queryView;
    }


    @Override
    public Query getNewQuery() {
        return new SelectTopHundredSellerByNumberOfAuctionsQuery( queryView );
    }


    private static class SelectTopHundredSellerByNumberOfAuctionsQuery extends Query {

        private final QueryView queryView;


        public SelectTopHundredSellerByNumberOfAuctionsQuery( QueryView queryView ) {
            super( EXPECT_RESULT );
            this.queryView = queryView;
        }


        @Override
        public String getSql() {

            if ( queryView.equals( QueryView.MATERIALIZED ) ) {
                return "SELECT * FROM topHundredSellerByNumberOfAuctions_materialized LIMIT 100";
            } else if ( queryView.equals( QueryView.VIEW ) ) {
                return "SELECT * FROM topHundredSellerByNumberOfAuctions_view LIMIT 100";
            } else {
                return "SELECT u.last_name, u.first_name, count(a.id) as number "
                        + "FROM auction a INNER JOIN \"user\" u ON a.\"user\" = u.id "
                        + "GROUP BY a.\"user\", u.last_name, u.first_name "
                        + "ORDER BY number desc LIMIT 100";
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

    }

}
