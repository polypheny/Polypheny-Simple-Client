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

package org.polypheny.simpleclient.scenario.gavel.queryBuilder.not_used;


import kong.unirest.HttpRequest;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SelectTopHundredSellerByRevenue extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;


    @Override
    public Query getNewQuery() {
        return new SelectTopHundredSellerByRevenueQuery();
    }


    private static class SelectTopHundredSellerByRevenueQuery extends Query {

        public SelectTopHundredSellerByRevenueQuery() {
            super( EXPECT_RESULT );
        }


        @Override
        public String getSql() {
            return "SELECT u.last_name, u.first_name, (sum(a.price) * 0.1) as revenue FROM user u, " +
                    "(SELECT a.user as user , MAX(amount) as price FROM auction a, bid b WHERE b.auction = a.id GROUP BY a.id, a.user) as a " +
                    "WHERE a.user = u.id " +
                    "GROUP BY u.id, u.last_name, u.first_name " +
                    "ORDER BY revenue desc LIMIT 100";
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }

    }

}
