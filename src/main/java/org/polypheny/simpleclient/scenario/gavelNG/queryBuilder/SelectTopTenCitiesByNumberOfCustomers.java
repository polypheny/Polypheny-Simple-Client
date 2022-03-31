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
import kong.unirest.Unirest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SelectTopTenCitiesByNumberOfCustomers extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;
    private final QueryMode queryMode;


    public SelectTopTenCitiesByNumberOfCustomers( QueryMode queryMode ) {
        this.queryMode = queryMode;
    }


    @Override
    public Query getNewQuery() {
        return new SelectTopTenCitiesByNumberOfCustomersQuery( queryMode );
    }


    private static class SelectTopTenCitiesByNumberOfCustomersQuery extends Query {

        private final String tableName;


        public SelectTopTenCitiesByNumberOfCustomersQuery( QueryMode queryMode ) {
            super( EXPECT_RESULT );

            if ( queryMode.equals( QueryMode.VIEW ) ) {
                tableName = "user_view";
            } else if ( queryMode.equals( QueryMode.MATERIALIZED ) ) {
                tableName = "user_materialized";
            } else {
                tableName = "\"user\"";
            }
        }


        @Override
        public String getSql() {
            return "SELECT city, COUNT(city) as number FROM " + tableName + " GROUP BY city ORDER BY number desc LIMIT 10";
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
            return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.user" )
                    .queryString( "_project", "public." + tableName + ".city@city,public." + tableName + ".city@number(COUNT)" )
                    .queryString( "_groupby", "city" )
                    .queryString( "_sort", "number@DESC" )
                    .queryString( "_limit", 10 );
        }


        @Override
        public String getMongoQl() {
            return "db." + tableName + ".aggregate(["
                    + "{\"$group\":{\"_id\":\"city\",\"number\": {\"$sum\": 1 }}},"
                    + "{\"$project\":{\"number\":1,\"city\":\"$_id\"}},"
                    + "{\"$sort\":{\"number\": -1 }},"
                    + "{\"$limit\":10}])";
        }

    }

}
