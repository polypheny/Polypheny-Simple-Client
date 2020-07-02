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


import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.HttpRequest;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;


public class InsertBid extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private final int auctionId;
    private final int userId;
    private final int amount;
    private final LocalDateTime date;
    private static final AtomicInteger nextBidId = new AtomicInteger( 1 );


    public InsertBid( int auctionId, int userId, int amount, LocalDateTime date ) {
        this.auctionId = auctionId;
        this.userId = userId;
        this.amount = amount;
        this.date = date;
    }


    @Override
    public BatchableInsert getNewQuery() {
        return new InsertBidQuery(
                nextBidId.getAndIncrement(),
                auctionId,
                userId,
                amount,
                date );
    }


    static class InsertBidQuery extends BatchableInsert {

        private final int bidId;
        private final int auctionId;
        private final int userId;
        private final int amount;
        private final LocalDateTime date;


        public InsertBidQuery( int bidId, int auctionId, int userId, int amount, LocalDateTime date ) {
            super( EXPECT_RESULT );
            this.bidId = bidId;
            this.auctionId = auctionId;
            this.userId = userId;
            this.amount = amount;
            this.date = date;
        }


        @Override
        public String getSql() {
            return "INSERT INTO bid(id, amount, \"timestamp\", \"user\", auction) VALUES " + getSqlRowExpression();
        }


        @Override
        public String getSqlRowExpression() {
            return "("
                    + bidId + ","
                    + amount + ","
                    + "timestamp '" + date.format( DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss" ) ) + "',"
                    + userId + ","
                    + auctionId // This could gets a bug if e.g. parallelized
                    + ")";
        }


        @Override
        public HttpRequest<?> getRest() {
            return buildRestInsert( "public.bid", ImmutableList.of( getRestRowExpression() ) );
        }


        @Override
        public JsonObject getRestRowExpression() {
            JsonObject row = new JsonObject();
            row.add( "public.bid.id", new JsonPrimitive( bidId ) );
            row.add( "public.bid.amount", new JsonPrimitive( amount ) );
            row.add( "public.bid.timestamp", new JsonPrimitive( date.format( DateTimeFormatter.ISO_LOCAL_DATE_TIME ) ) );
            row.add( "public.bid.user", new JsonPrimitive( userId ) );
            row.add( "public.bid.auction", new JsonPrimitive( auctionId ) );
            return row;
        }


        @Override
        public String getTable() {
            return "public.bid";
        }
    }
}
