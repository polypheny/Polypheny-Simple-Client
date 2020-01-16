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


import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.DateProducer;
import io.codearte.jfairy.producer.text.TextProducer;
import java.util.concurrent.ThreadLocalRandom;
import org.joda.time.DateTime;
import org.polypheny.simpleclient.main.QueryBuilder;
import org.polypheny.simpleclient.scenario.gavel.Config;


public class InsertRandomAuction extends QueryBuilder {

    private final int numberOfUsers;
    private final int numberOfCategories;
    private final int auctionTitleMinLength;
    private final int auctionTitleMaxLength;
    private final int auctionDescriptionMinLength;
    private final int auctionDescriptionMaxLength;
    private final int auctionDateMaxYearsInPast;
    private final int auctionNumberOfDays;

    private final TextProducer text;
    private final DateProducer dateProducer;

    private static int nextId = 1;


    public InsertRandomAuction( int numberOfUsers, int numberOfCategories, Config config ) {
        super( false );
        this.numberOfUsers = numberOfUsers;
        this.numberOfCategories = numberOfCategories;
        this.auctionTitleMinLength = config.auctionTitleMinLength;
        this.auctionTitleMaxLength = config.auctionTitleMaxLength;
        this.auctionDescriptionMinLength = config.auctionDescriptionMinLength;
        this.auctionDescriptionMaxLength = config.auctionDescriptionMaxLength;
        this.auctionDateMaxYearsInPast = config.auctionDateMaxYearsInPast;
        this.auctionNumberOfDays = config.auctionNumberOfDays;

        text = Fairy.create().textProducer();
        dateProducer = Fairy.create().dateProducer();
    }


    @Override
    public String generateSql() {
        String title = text.latinWord( ThreadLocalRandom.current().nextInt( auctionTitleMinLength, auctionTitleMaxLength + 1 ) );
        String description = text.paragraph( ThreadLocalRandom.current().nextInt( auctionDescriptionMinLength, auctionDescriptionMaxLength + 1 ) );
        DateTime startDate = dateProducer.randomDateInThePast( auctionDateMaxYearsInPast );
        DateTime endDate = startDate.plusDays( auctionNumberOfDays );

        StringBuilder sb = new StringBuilder();
        sb.append( "INSERT INTO auction(id, title, description, start_date, end_date, category, \"user\") VALUES (" );
        sb.append( nextId++ ).append( "," );
        sb.append( "'" ).append( title ).append( "'," );
        sb.append( "'" ).append( description ).append( "'," );
        sb.append( "timestamp '" ).append( startDate.toString( "yyyy-MM-dd HH:mm:ss" ) ).append( "'," );
        sb.append( "timestamp '" ).append( endDate.toString( "yyyy-MM-dd HH:mm:ss" ) ).append( "'," );
        sb.append( ThreadLocalRandom.current().nextInt( 1, numberOfCategories + 1 ) ).append( "," );
        sb.append( ThreadLocalRandom.current().nextInt( 1, numberOfUsers + 1 ) );
        sb.append( ")" );
        return sb.toString();
    }


    public static void setNextId( int nextId ) {
        InsertRandomAuction.nextId = nextId;
    }
}
