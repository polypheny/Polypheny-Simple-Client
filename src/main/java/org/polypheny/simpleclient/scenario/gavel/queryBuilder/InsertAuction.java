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


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.HttpRequest;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class InsertAuction extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private final int userId;
    private final int categoryId;
    private final LocalDateTime startDate;
    private final LocalDateTime endDate;
    private final String title;
    private final String description;
    private static final AtomicInteger nextAuctionId = new AtomicInteger( 1 );


    public InsertAuction( int userId, int categoryId, LocalDateTime startDate, LocalDateTime endDate, String title, String description ) {
        this.userId = userId;
        this.categoryId = categoryId;
        this.startDate = startDate;
        this.endDate = endDate;
        this.title = title;
        this.description = description;
    }


    @Override
    public Query getNewQuery() {
        return new InsertAuctionQuery(
                nextAuctionId.getAndIncrement(),
                userId,
                categoryId,
                startDate,
                endDate,
                title,
                description );
    }


    private static class InsertAuctionQuery extends Query {

        private final int auctionId;
        private final int userId;
        private final int categoryId;
        private final LocalDateTime startDate;
        private final LocalDateTime endDate;
        private final String title;
        private final String description;


        public InsertAuctionQuery( int auctionId, int userId, int categoryId, LocalDateTime startDate, LocalDateTime endDate, String title, String description ) {
            super( EXPECT_RESULT );
            this.auctionId = auctionId;
            this.userId = userId;
            this.categoryId = categoryId;
            this.startDate = startDate;
            this.endDate = endDate;
            this.title = title;
            this.description = description;
        }


        @Override
        public String getSql() {
            return "INSERT INTO auction(id, title, description, start_date, end_date, category, \"user\") VALUES ("
                    + auctionId + ","
                    + "'" + title + "',"
                    + "'" + description + "',"
                    + "timestamp '" + startDate.format( DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss" ) ) + "',"
                    + "timestamp '" + endDate.format( DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss" ) ) + "',"
                    + categoryId + ","
                    + userId
                    + ")";
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }
    }

}
