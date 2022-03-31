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

package org.polypheny.simpleclient.scenario.gavelNG.queryBuilder.not_used;


import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.DateProducer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.gavelNG.GavelNGConfig;


public class SelectTenMostPopularAuctionsOfCategoryByNumberOfBids extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final int numberOfCategories;
    private final int auctionDateMaxYearsInPast;

    private final DateProducer dateProducer;


    public SelectTenMostPopularAuctionsOfCategoryByNumberOfBids( int numberOfCategories, GavelNGConfig config ) {
        this.numberOfCategories = numberOfCategories;
        this.auctionDateMaxYearsInPast = config.auctionDateMaxYearsInPast;

        dateProducer = Fairy.create().dateProducer();
    }


    @Override
    public Query getNewQuery() {
        LocalDateTime date = dateProducer.randomDateInThePast( auctionDateMaxYearsInPast );
        int categoryId = ThreadLocalRandom.current().nextInt( 1, numberOfCategories + 1 );
        return new SelectTenMostPopularAuctionsOfCategoryByNumberOfBidsQuery( date, categoryId );
    }


    private static class SelectTenMostPopularAuctionsOfCategoryByNumberOfBidsQuery extends Query {

        private final LocalDateTime date;
        private final int categoryId;


        public SelectTenMostPopularAuctionsOfCategoryByNumberOfBidsQuery( LocalDateTime date, int categoryId ) {
            super( EXPECT_RESULT );
            this.date = date;
            this.categoryId = categoryId;
        }


        @Override
        public String getSql() {
            return "SELECT a.id, COUNT(b.auction) as number FROM auction a, bid b WHERE a.id = b.auction AND a.category = " + categoryId +
                    " AND a.end_date > '" + date.format( DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss" ) ) + "' GROUP BY a.id ORDER BY number desc LIMIT 10";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return "SELECT a.id, COUNT(b.auction) as number FROM auction a, bid b WHERE a.id = b.auction AND a.category = ?" +
                    " AND a.end_date > ? GROUP BY a.id ORDER BY number desc LIMIT 10";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, categoryId ) );
            map.put( 2, new ImmutablePair<>( DataTypes.TIMESTAMP, date ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }


        @Override
        public String getMongoQl() {
            return null;
        }

    }

}
