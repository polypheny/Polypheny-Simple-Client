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
import com.devskiller.jfairy.producer.DateProducer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import kong.unirest.HttpRequest;
import kong.unirest.Unirest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.gavel.GavelConfig;


public class SelectTheHundredNextEndingAuctionsOfRandomCategoryView extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final int numberOfCategories;
    private final int auctionDateMaxYearsInPast;

    private final DateProducer dateProducer;


    public SelectTheHundredNextEndingAuctionsOfRandomCategoryView( int numberOfCategories, GavelConfig config ) {
        this.numberOfCategories = numberOfCategories;
        this.auctionDateMaxYearsInPast = config.auctionDateMaxYearsInPast;
        this.dateProducer = Fairy.create().dateProducer();
    }


    @Override
    public Query getNewQuery() {
        LocalDateTime date = dateProducer.randomDateInThePast( auctionDateMaxYearsInPast );
        date = date.withNano( 0 );
        int categoryId = ThreadLocalRandom.current().nextInt( 1, numberOfCategories + 1 );
        return new SelectTheHundredNextEndingAuctionsOfRandomCategoryViewQuery( date, categoryId );
    }


    private static class SelectTheHundredNextEndingAuctionsOfRandomCategoryViewQuery extends Query {

        private final LocalDateTime date;
        private final int categoryId;


        public SelectTheHundredNextEndingAuctionsOfRandomCategoryViewQuery( LocalDateTime date, int categoryId ) {
            super( EXPECT_RESULT );
            this.date = date;
            this.categoryId = categoryId;
        }


        @Override
        public String getSql() {
            return "SELECT a.id, a.title, a.end_date FROM auction_view a WHERE a.category =" + categoryId + " AND " +
                    "a.end_date > timestamp '" + date.format( DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss" ) ) + "' ORDER BY end_date desc LIMIT 100";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return "SELECT a.id, a.title, a.end_date FROM auction_view a WHERE a.category = ? AND " +
                    "a.end_date > timestamp '" + date.format( DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss" ) ) + "' ORDER BY end_date desc LIMIT 100";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, categoryId ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.auction_view" )
                    .queryString( "_project", "public.auction.id,public.auction.title,public.auction_view.end_date" )
                    .queryString( "public.auction_view.category", "=" + categoryId )
                    .queryString( "public.auction_view.end_date", ">" + date.format( DateTimeFormatter.ISO_LOCAL_DATE_TIME ) )
                    .queryString( "_sort", "public.auction_view.end_date@DESC" )
                    .queryString( "_limit", 100 );
        }

    }

}
