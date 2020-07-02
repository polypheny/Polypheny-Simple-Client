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


import kong.unirest.HttpRequest;
import kong.unirest.Unirest;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SelectTopTenCitiesByNumberOfCustomers extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;


    @Override
    public Query getNewQuery() {
        return new SelectTopTenCitiesByNumberOfCustomersQuery();
    }


    private static class SelectTopTenCitiesByNumberOfCustomersQuery extends Query {

        public SelectTopTenCitiesByNumberOfCustomersQuery() {
            super( EXPECT_RESULT );
        }


        @Override
        public String getSql() {
            return "SELECT city, COUNT(city) as number FROM \"user\" GROUP BY city ORDER BY number desc LIMIT 10";
        }


        @Override
        public HttpRequest<?> getRest() {
            return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.user" )
                    .queryString( "_project", "public.user.city@city,public.user.city@number(COUNT)" )
                    .queryString( "_groupby", "city" )
                    .queryString( "_sort", "number@DESC" )
                    .queryString( "_limit", 10 );
        }
    }
}
