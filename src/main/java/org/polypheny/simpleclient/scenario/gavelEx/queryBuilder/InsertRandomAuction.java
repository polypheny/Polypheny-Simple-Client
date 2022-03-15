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

package org.polypheny.simpleclient.scenario.gavelEx.queryBuilder;


import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.DateProducer;
import com.devskiller.jfairy.producer.text.TextProducer;
import java.time.LocalDateTime;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.gavelEx.queryBuilder.InsertAuction.InsertAuctionQuery;
import org.polypheny.simpleclient.scenario.gavelEx.GavelExConfig;


public class InsertRandomAuction extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private final int numberOfUsers;
    private final int numberOfCategories;
    private final int numberOfConditions;
    private final int auctionTitleMinLength;
    private final int auctionTitleMaxLength;
    private final int auctionDescriptionMinLength;
    private final int auctionDescriptionMaxLength;
    private final int auctionDateMaxYearsInPast;
    private final int auctionNumberOfDays;

    private final TextProducer text;
    private final DateProducer dateProducer;

    private static final AtomicInteger nextAuctionId = new AtomicInteger( 1 );


    public InsertRandomAuction( int numberOfUsers, int numberOfCategories, int numberOfConditions, GavelExConfig config ) {
        this.numberOfUsers = numberOfUsers;
        this.numberOfCategories = numberOfCategories;
        this.numberOfConditions = numberOfConditions;
        this.auctionTitleMinLength = config.auctionTitleMinLength;
        this.auctionTitleMaxLength = config.auctionTitleMaxLength;
        this.auctionDescriptionMinLength = config.auctionDescriptionMinLength;
        this.auctionDescriptionMaxLength = config.auctionDescriptionMaxLength;
        this.auctionDateMaxYearsInPast = config.auctionDateMaxYearsInPast;
        this.auctionNumberOfDays = config.auctionNumberOfDays;

        text = Fairy.create().textProducer();
        dateProducer = Fairy.create().dateProducer();
    }


    public static void setNextId( int nextId ) {
        InsertRandomAuction.nextAuctionId.set( nextId );
    }


    @Override
    public Query getNewQuery() {
        String title = text.latinWord( ThreadLocalRandom.current().nextInt( auctionTitleMinLength, auctionTitleMaxLength + 1 ) );
        String description = text.paragraph( ThreadLocalRandom.current().nextInt( auctionDescriptionMinLength, auctionDescriptionMaxLength + 1 ) );
        LocalDateTime startDate = dateProducer.randomDateInThePast( auctionDateMaxYearsInPast );
        LocalDateTime endDate = startDate.plusDays( auctionNumberOfDays );

        return new InsertRandomAuctionQuery(
                nextAuctionId.getAndIncrement(),
                title,
                description,
                startDate,
                endDate,
                ThreadLocalRandom.current().nextInt( 1, numberOfUsers + 1 ),
                ThreadLocalRandom.current().nextInt( 1, numberOfCategories + 1 ),
                ThreadLocalRandom.current().nextInt(1, numberOfConditions + 1)
        );
    }


    private static class InsertRandomAuctionQuery extends InsertAuctionQuery {


        public InsertRandomAuctionQuery( int auctionId, String title, String description, LocalDateTime startDate, LocalDateTime endDate, int userId, int categoryId, int conditionsId ) {
            super( auctionId, userId, categoryId, conditionsId, startDate, endDate, title, description );
        }

    }

}
