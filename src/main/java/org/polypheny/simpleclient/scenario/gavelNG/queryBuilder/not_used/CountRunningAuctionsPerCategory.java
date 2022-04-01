/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2022 The Polypheny Project
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
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.gavelNG.GavelNGConfig;


public class CountRunningAuctionsPerCategory extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final int auctionDateMaxYearsInPast;
    private final DateProducer dateProducer;


    public CountRunningAuctionsPerCategory( GavelNGConfig config ) {
        this.auctionDateMaxYearsInPast = config.auctionDateMaxYearsInPast;
        dateProducer = Fairy.create().dateProducer();
    }


    @Override
    public Query getNewQuery() {
        LocalDateTime date = dateProducer.randomDateInThePast( auctionDateMaxYearsInPast );
        return new CountRunningAuctionsPerCategoryQuery( date );
    }


    private static class CountRunningAuctionsPerCategoryQuery extends Query {

        private final LocalDateTime date;


        public CountRunningAuctionsPerCategoryQuery( LocalDateTime dateTime ) {
            super( EXPECT_RESULT );
            this.date = dateTime;
        }


        @Override
        public String getSql() {
            return "SELECT c.name, count(a.id) " +
                    " FROM auction a, category c " +
                    " WHERE a.category = c.id" +
                    " AND a.end_date > '" + date.format( DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss" ) ) + "'" +
                    " GROUP BY c.name";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return "SELECT c.name, count(a.id) " +
                    " FROM auction a, category c " +
                    " WHERE a.category = c.id" +
                    " AND a.end_date > ?" +
                    " GROUP BY c.name";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.TIMESTAMP, date ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }


        @Override
        public String getMongoQl() {
            return "db.auction.find({\"end_date\":{\"$gt\":" + date.toLocalDate().toEpochDay() + "}}, {\"id\":1})";
        }

    }

}
