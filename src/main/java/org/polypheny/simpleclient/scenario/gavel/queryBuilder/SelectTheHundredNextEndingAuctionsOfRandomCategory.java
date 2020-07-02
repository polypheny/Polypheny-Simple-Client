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
import java.util.concurrent.ThreadLocalRandom;
import kong.unirest.HttpRequest;
import kong.unirest.Unirest;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.gavel.Config;


public class SelectTheHundredNextEndingAuctionsOfRandomCategory extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final int numberOfCategories;
    private final int auctionDateMaxYearsInPast;

    private final DateProducer dateProducer;


    public SelectTheHundredNextEndingAuctionsOfRandomCategory( int numberOfCategories, Config config ) {
        this.numberOfCategories = numberOfCategories;
        this.auctionDateMaxYearsInPast = config.auctionDateMaxYearsInPast;
        this.dateProducer = Fairy.create().dateProducer();
    }


    @Override
    public Query getNewQuery() {
        LocalDateTime date = dateProducer.randomDateInThePast( auctionDateMaxYearsInPast );
        date = date.withNano( 0 );
        int categoryId = ThreadLocalRandom.current().nextInt( 1, numberOfCategories + 1 );
        return new SelectTheHundredNextEndingAuctionsOfRandomCategoryQuery( date, categoryId );
    }


    private static class SelectTheHundredNextEndingAuctionsOfRandomCategoryQuery extends Query {

        private final LocalDateTime date;
        private final int categoryId;


        public SelectTheHundredNextEndingAuctionsOfRandomCategoryQuery( LocalDateTime date, int categoryId ) {
            super( EXPECT_RESULT );
            this.date = date;
            this.categoryId = categoryId;
        }


        @Override
        public String getSql() {
            return "SELECT a.id, a.title, a.end_date FROM auction a WHERE a.category =" + categoryId + " AND " +
                    "a.end_date > '" + date.format( DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss" ) ) + "' ORDER BY end_date desc LIMIT 100";
        }


        @Override
        public HttpRequest<?> getRest() {
            return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.auction" )
                    .queryString( "_project", "public.auction.id,public.auction.title,public.auction.end_date" )
                    .queryString( "public.auction.category", "=" + categoryId )
                    .queryString( "public.auction.end_date", ">" + date.format( DateTimeFormatter.ISO_LOCAL_DATE_TIME ) )
                    .queryString( "_sort", "public.auction.end_date@DESC" )
                    .queryString( "_limit", 100 );
        }
    }
}
