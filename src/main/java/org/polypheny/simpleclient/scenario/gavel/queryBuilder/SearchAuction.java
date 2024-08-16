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


import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.text.TextProducer;
import java.util.HashMap;
import java.util.Map;
import kong.unirest.core.HttpRequest;
import kong.unirest.core.Unirest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SearchAuction extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final TextProducer text;
    private final QueryMode queryMode;


    public SearchAuction( QueryMode queryMode ) {
        text = Fairy.create().textProducer();
        this.queryMode = queryMode;
    }


    @Override
    public Query getNewQuery() {
        return new SearchAuctionQuery( text.latinWord( 2 ), queryMode );
    }


    private static class SearchAuctionQuery extends Query {

        private final String searchString;
        private final String tableName;


        public SearchAuctionQuery( String searchString, QueryMode queryMode ) {
            super( EXPECT_RESULT );
            this.searchString = searchString;

            if ( queryMode.equals( QueryMode.VIEW ) ) {
                tableName = "auction_view";
            } else if ( queryMode.equals( QueryMode.MATERIALIZED ) ) {
                tableName = "auction_materialized";
            } else {
                tableName = "auction";
            }
        }


        @Override
        public String getSql() {
            return "SELECT a.title, a.start_date, a.end_date FROM " + tableName + " a "
                    + "WHERE a.title LIKE '%" + searchString + "%' "
                    + "ORDER BY end_date desc "
                    + "LIMIT 100";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return "SELECT a.title, a.start_date, a.end_date FROM " + tableName + " a "
                    + "WHERE a.title LIKE ? "
                    + "ORDER BY end_date desc "
                    + "LIMIT 100";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.VARCHAR, "%" + searchString + "%" ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.auction" )
                    .queryString( "_project", "public." + tableName + ".title,public." + tableName + ".start_date,public." + tableName + ".end_date" )
                    .queryString( "public." + tableName + ".title", "%%" + searchString + "%" )
                    .queryString( "_sort", "public." + tableName + ".end_date@DESC" )
                    .queryString( "_limit", 100 );
        }


        @Override
        public String getMongoQl() {
            return "db." + tableName + ".aggregate(["
                    + "{\"$match\":{\"title\":{\"$regex\": \"/$" + searchString + "$/\"}}},"
                    + "{\"$sort\":{\"end_data\": -1}},"
                    + "{\"$limit\": 100}])";
        }

    }

}
