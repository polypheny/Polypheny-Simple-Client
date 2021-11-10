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

package org.polypheny.simpleclient.scenario.gavel.queryBuilder;


import java.util.HashMap;
import java.util.Map;
import kong.unirest.HttpRequest;
import kong.unirest.Unirest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class CountAuction extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;
    private final QueryMode queryMode;


    public CountAuction( QueryMode queryMode ) {
        this.queryMode = queryMode;
    }


    @Override
    public Query getNewQuery() {
        return new CountAuctionQuery( queryMode );
    }


    private static class CountAuctionQuery extends Query {

        private final QueryMode queryMode;


        public CountAuctionQuery( QueryMode queryMode ) {
            super( EXPECT_RESULT );
            this.queryMode = queryMode;
        }


        @Override
        public String getSql() {
            if ( queryMode.equals( QueryMode.VIEW ) ) {
                return "SELECT * FROM countAuction";
            } else if ( queryMode.equals( QueryMode.MATERIALIZED ) ) {
                return "SELECT * FROM countAuction_materialized";
            } else {
                return "SELECT count(*) as NUMBER FROM auction";
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
            if ( queryMode.equals( QueryMode.VIEW ) ) {
                return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.countAuction" );
            } else if ( queryMode.equals( QueryMode.MATERIALIZED ) ) {
                return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.countAuction_materialized" );
            } else {
                return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.auction" )
                        .queryString( "_project", "public.auction.id@num(COUNT)" );
            }
        }


        @Override
        public String getMongoQl() {
            if ( queryMode.equals( QueryMode.VIEW ) ) {
                return "db.countAuction.find({})";
            } else if ( queryMode.equals( QueryMode.MATERIALIZED ) ) {
                return "db.countAuction_materialized.find({})";
            } else {
                return "db.auction.count({})";
            }
        }

    }

}
