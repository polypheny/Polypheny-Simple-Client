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


import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.text.TextProducer;
import java.util.HashMap;
import java.util.Map;
import kong.unirest.HttpRequest;
import kong.unirest.Unirest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.QueryView;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SearchAuction extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final TextProducer text;
    private final QueryView queryView;


    public SearchAuction( QueryView queryView ) {
        text = Fairy.create().textProducer();
        this.queryView = queryView;
    }


    @Override
    public Query getNewQuery() {
        return new SearchAuctionQuery( text.latinWord( 2 ), queryView );
    }


    private static class SearchAuctionQuery extends Query {

        private final String searchString;
        private final String tablename;


        public SearchAuctionQuery( String searchString, QueryView queryView ) {
            super( EXPECT_RESULT );
            this.searchString = searchString;

            if ( queryView.equals( QueryView.VIEW ) ) {
                tablename = "auction_view";
            } else if ( queryView.equals( QueryView.MATERIALIZED ) ) {
                tablename = "auction_materialized";
            } else {
                tablename = "auction";
            }
        }


        @Override
        public String getSql() {

            return "SELECT a.title, a.start_date, a.end_date FROM " + tablename + " a "
                    + "WHERE a.title LIKE '%" + searchString + "%' "
                    + "ORDER BY end_date desc "
                    + "LIMIT 100";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return "SELECT a.title, a.start_date, a.end_date FROM " + tablename + " a "
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
                    .queryString( "_project", "public." + tablename + ".title,public." + tablename + ".start_date,public." + tablename + ".end_date" )
                    .queryString( "public." + tablename + ".title", "%%" + searchString + "%" )
                    .queryString( "_sort", "public." + tablename + ".end_date@DESC" )
                    .queryString( "_limit", 100 );
        }

    }

}
