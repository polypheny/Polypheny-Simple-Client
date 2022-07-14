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

package org.polypheny.simpleclient.scenario.gavelNG.queryBuilder;


import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.DateProducer;
import java.time.LocalDateTime;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.gavelNG.queryBuilder.InsertBid.InsertBidQuery;


public class InsertRandomBid extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private final int numberOfAuctions;
    private final int numberOfUsers;

    private static final AtomicInteger nextBidId = new AtomicInteger( 1 );


    public InsertRandomBid( int numberOfAuctions, int numberOfUsers ) {
        this.numberOfAuctions = numberOfAuctions;
        this.numberOfUsers = numberOfUsers;
    }


    public static void setNextId( int nextId ) {
        InsertRandomBid.nextBidId.set( nextId );
    }


    @Override
    public Query getNewQuery() {
        DateProducer dateProducer = Fairy.create().dateProducer();
        return new InsertRandomBidQuery(
                nextBidId.getAndIncrement(),
                ThreadLocalRandom.current().nextInt( 1, 1000 ),
                dateProducer.randomDateInThePast( 5 ),
                ThreadLocalRandom.current().nextInt( 1, numberOfUsers + 1 ),
                ThreadLocalRandom.current().nextInt( 1, numberOfAuctions + 1 )
        );
    }


    private static class InsertRandomBidQuery extends InsertBidQuery {

        public InsertRandomBidQuery( int bidId, int amount, LocalDateTime timestamp, int userId, int auctionId ) {
            super( bidId, auctionId, userId, amount, timestamp );
        }

    }

}
