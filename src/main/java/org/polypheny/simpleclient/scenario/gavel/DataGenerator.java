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

package org.polypheny.simpleclient.scenario.gavel;


import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.DateProducer;
import io.codearte.jfairy.producer.text.TextProducer;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.main.Query;
import org.polypheny.simpleclient.main.QueryBuilder;
import org.polypheny.simpleclient.scenario.gavel.Gavel.DataGenerationThreadMonitor;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.InsertAuction;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.InsertBid;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.InsertCategory;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.InsertPicture;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.InsertUser;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.TruncateAuction;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.TruncateBid;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.TruncateCategory;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.TruncatePicture;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.TruncateUser;


@Slf4j
class DataGenerator {

    private final Executor theExecutor;
    private final Config config;
    private final ProgressReporter progressReporter;

    private final List<Query> batchList;

    @Getter
    private DataGenerationThreadMonitor threadMonitor;
    private boolean aborted;


    DataGenerator( Executor executor, Config config, ProgressReporter progressReporter, DataGenerationThreadMonitor threadMonitor ) {
        theExecutor = executor;
        this.config = config;
        this.progressReporter = progressReporter;
        batchList = new LinkedList<>();
        this.threadMonitor = threadMonitor;
        threadMonitor.registerDataGenerator( this );
        aborted = false;
    }


    void truncateTables() throws SQLException {
        log.info( "Truncate Tables" );
        theExecutor.executeStatement( (new TruncateCategory()).getNewQuery() );
        theExecutor.executeStatement( (new TruncateUser()).getNewQuery() );
        theExecutor.executeStatement( (new TruncateAuction()).getNewQuery() );
        theExecutor.executeStatement( (new TruncateBid()).getNewQuery() );
        theExecutor.executeStatement( (new TruncatePicture()).getNewQuery() );
    }


    void generateCategories() throws SQLException {
        int numberOfCategories = config.numberOfCategories;
        QueryBuilder queryBuilder = new InsertCategory();
        for ( int i = 0; i < numberOfCategories; i++ ) {
            if ( aborted ) {
                break;
            }
            addToMultiInsert( queryBuilder.getNewQuery() );
        }
        executeMultiInsert();
    }


    void generateUsers( int numberOfUsers ) throws SQLException {
        int mod = numberOfUsers / progressReporter.base;
        QueryBuilder queryBuilder = new InsertUser();
        for ( int i = 0; i < numberOfUsers; i++ ) {
            if ( aborted ) {
                break;
            }
            addToMultiInsert( queryBuilder.getNewQuery() );
            if ( (i % mod) == 0 ) {
                progressReporter.updateProgress();
            }
        }
        executeMultiInsert();
    }


    void generateAuctions( int start, int end ) throws SQLException {
        int mod = ((end - start) + 1) / progressReporter.base;

        int numberOfCategories = config.numberOfCategories;
        int numberOfUsers = config.numberOfUsers;
        int auctionTitleMinLength = config.auctionTitleMinLength;
        int auctionTitleMaxLength = config.auctionTitleMaxLength;
        int auctionDescriptionMinLength = config.auctionDescriptionMinLength;
        int auctionDescriptionMaxLength = config.auctionDescriptionMaxLength;
        int auctionDateMaxYearsInPast = config.auctionDateMaxYearsInPast;
        int auctionNumberOfDays = config.auctionNumberOfDays;
        int minNumberOfBidsPerAuction = config.minNumberOfBidsPerAuction;
        int maxNumberOfBidsPerAuction = config.maxNumberOfBidsPerAuction;
        int minNumberOfPicturesPerAuction = config.minNumberOfPicturesPerAuction;
        int maxNumberOfPicturesPerAuction = config.maxNumberOfPicturesPerAuction;

        TextProducer text = Fairy.create().textProducer();
        DateProducer dateProducer = Fairy.create().dateProducer();

        DateTime startDate;
        DateTime endDate;
        int user;
        int numberOfBids;
        int numberOfPictures;
        int category;
        String title;
        String description;
        for ( int i = start; i <= end; i++ ) {
            if ( aborted ) {
                break;
            }
            if ( (i % mod) == 0 ) {
                progressReporter.updateProgress();
            }
            // create an auction
            startDate = dateProducer.randomDateInThePast( auctionDateMaxYearsInPast );
            endDate = startDate.plusDays( auctionNumberOfDays );
            user = ThreadLocalRandom.current().nextInt( 1, numberOfUsers + 1 );
            category = ThreadLocalRandom.current().nextInt( 1, numberOfCategories + 1 );
            title = text.latinWord( ThreadLocalRandom.current().nextInt( auctionTitleMinLength, auctionTitleMaxLength + 1 ) );
            description = text.paragraph( ThreadLocalRandom.current().nextInt( auctionDescriptionMinLength, auctionDescriptionMaxLength + 1 ) );
            addToMultiInsert( (new InsertAuction( user, category, startDate, endDate, title, description )).getNewQuery() );
            executeMultiInsert();

            // create bids for that auction
            numberOfBids = ThreadLocalRandom.current().nextInt( minNumberOfBidsPerAuction, maxNumberOfBidsPerAuction );
            int lastBid = 0;
            int amount;
            int u;
            DateTime dt;
            DateTime dtLast = startDate;
            for ( int j = 0; j < numberOfBids; j++ ) {
                amount = ThreadLocalRandom.current().nextInt( lastBid + 1, lastBid + 1000 );
                u = ThreadLocalRandom.current().nextInt( 1, numberOfUsers + 1 );
                if ( u == user ) {
                    // no bids on your own auction
                    if ( u < numberOfUsers ) {
                        u++;
                    } else {
                        u--;
                    }
                }
                dt = dateProducer.randomDateBetweenTwoDates( dtLast, endDate );
                lastBid = amount;
                amount = amount * 100;
                addToMultiInsert( new InsertBid( i, u, amount, dt ).getNewQuery() );
                dtLast = dt;
            }
            executeMultiInsert();

            // create pictures
            QueryBuilder pictureBuilder = new InsertPicture( i );
            numberOfPictures = ThreadLocalRandom.current().nextInt( minNumberOfPicturesPerAuction, maxNumberOfPicturesPerAuction );
            for ( int j = 0; j < numberOfPictures; j++ ) {
                addToMultiInsert( pictureBuilder.getNewQuery() );
            }
            executeMultiInsert();
        }

    }


    private void addToMultiInsert( Query query ) throws SQLException {
        batchList.add( query );
        if ( batchList.size() >= config.maxBatchSize ) {
            executeMultiInsert();
        }
    }


    private void executeMultiInsert() throws SQLException {
        if ( batchList.size() > 0 ) {
            StringBuilder stringBuilder = new StringBuilder();
            boolean first = true;
            for ( Query query : batchList ) {
                if ( first ) {
                    stringBuilder.append( query.sqlQuery );
                    first = false;
                } else {
                    String statement = query.sqlQuery;
                    String[] split = statement.split( "VALUES" );
                    stringBuilder.append( "," ).append( split[split.length - 1] );
                }
            }
            theExecutor.executeStatement( new Query( stringBuilder.toString(), false ) );
            theExecutor.executeCommit();
            batchList.clear();
        }
    }


    public void abort() {
        aborted = true;
    }
}
