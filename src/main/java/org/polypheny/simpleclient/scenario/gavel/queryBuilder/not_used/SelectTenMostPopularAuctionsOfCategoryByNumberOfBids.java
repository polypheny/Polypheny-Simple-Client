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


import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.DateProducer;
import java.util.concurrent.ThreadLocalRandom;
import org.joda.time.DateTime;
import org.polypheny.simpleclient.main.QueryBuilder;
import org.polypheny.simpleclient.scenario.gavel.Config;


public class SelectTenMostPopularAuctionsOfCategoryByNumberOfBids extends QueryBuilder {

    private final int numberOfCategories;
    private final int auctionDateMaxYearsInPast;

    private final DateProducer dateProducer;


    public SelectTenMostPopularAuctionsOfCategoryByNumberOfBids( int numberOfCategories, Config config ) {
        super( true );
        this.numberOfCategories = numberOfCategories;
        this.auctionDateMaxYearsInPast = config.auctionDateMaxYearsInPast;

        dateProducer = Fairy.create().dateProducer();
    }


    @Override
    public String generateSql() {
        DateTime date = dateProducer.randomDateInThePast( auctionDateMaxYearsInPast );
        int category = ThreadLocalRandom.current().nextInt( 1, numberOfCategories + 1 );
        return "SELECT a.id, COUNT(b.auction) as number FROM auction a, bid b WHERE a.id = b.auction AND a.category = " + category +
                " AND a.end_date > '" + date.toString( "yyyy-MM-dd HH:mm:ss" ) + "' GROUP BY a.id ORDER BY number desc LIMIT 10";
    }
}
