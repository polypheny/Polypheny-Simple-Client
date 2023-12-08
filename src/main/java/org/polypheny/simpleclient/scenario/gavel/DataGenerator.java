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

package org.polypheny.simpleclient.scenario.gavel;

import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.DateProducer;
import com.devskiller.jfairy.producer.text.TextProducer;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.BatchableInsert;
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

    private static final AtomicInteger nextAuctionId = new AtomicInteger( 1 );

    private final Executor theExecutor;
    private final GavelConfig config;
    private final ProgressReporter progressReporter;

    private final List<BatchableInsert> batchList;

    @Getter
    private final DataGenerationThreadMonitor threadMonitor;
    private boolean aborted;


    DataGenerator( Executor executor, GavelConfig config, ProgressReporter progressReporter, DataGenerationThreadMonitor threadMonitor ) {
        theExecutor = executor;
        this.config = config;
        this.progressReporter = progressReporter;
        batchList = new LinkedList<>();
        this.threadMonitor = threadMonitor;
        threadMonitor.registerDataGenerator( this );
        aborted = false;
    }


    void truncateTables() throws ExecutorException {
        log.info( "Truncate Tables" );
        theExecutor.executeQuery( (new TruncateCategory()).getNewQuery() );
        theExecutor.executeQuery( (new TruncateUser()).getNewQuery() );
        theExecutor.executeQuery( (new TruncateAuction()).getNewQuery() );
        theExecutor.executeQuery( (new TruncateBid()).getNewQuery() );
        theExecutor.executeQuery( (new TruncatePicture()).getNewQuery() );
    }


    void generateCategories() throws ExecutorException {
        int numberOfCategories = config.numberOfCategories;
        InsertCategory queryBuilder = new InsertCategory();
        for ( int i = 0; i < numberOfCategories; i++ ) {
            if ( aborted ) {
                break;
            }
            addToInsertList( queryBuilder.getNewQuery() );
        }
        executeInsertList();
    }


    void generateUsers( int numberOfUsers ) throws ExecutorException {
        int mod = numberOfUsers / progressReporter.base;
        if ( mod == 0 ) {
            mod++; // Avoid division by zero
        }
        InsertUser queryBuilder = new InsertUser();
        for ( int i = 0; i < numberOfUsers; i++ ) {
            if ( aborted ) {
                break;
            }
            addToInsertList( queryBuilder.getNewQuery() );
            if ( (i % mod) == 0 ) {
                progressReporter.updateProgress();
            }
        }
        executeInsertList();
    }


    void generateAuctions( int start, int end ) throws ExecutorException {
        int mod = ((end - start) + 1) / progressReporter.base;
        if ( mod == 0 ) {
            mod++; // Avoid division by zero
        }

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

        Fairy fairy = Fairy.create();
        TextProducer text = fairy.textProducer();
        DateProducer dateProducer = fairy.dateProducer();

        LocalDateTime startDate;
        LocalDateTime endDate;
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
            startDate = dateProducer.randomDateInThePast( auctionDateMaxYearsInPast ).withNano( 0 );
            endDate = startDate.plusDays( auctionNumberOfDays );
            user = ThreadLocalRandom.current().nextInt( 1, numberOfUsers + 1 );
            category = ThreadLocalRandom.current().nextInt( 1, numberOfCategories + 1 );
            title = text.latinWord( ThreadLocalRandom.current().nextInt( auctionTitleMinLength, auctionTitleMaxLength + 1 ) );
            description = text.paragraph( ThreadLocalRandom.current().nextInt( auctionDescriptionMinLength, auctionDescriptionMaxLength + 1 ) );
            int auctionId = nextAuctionId.getAndIncrement();
            addToInsertList( new InsertAuction( auctionId, user, category, startDate, endDate, title, description ).getNewQuery() );
            executeInsertList();

            // create bids for that auction
            numberOfBids = ThreadLocalRandom.current().nextInt( minNumberOfBidsPerAuction, maxNumberOfBidsPerAuction );
            int lastBid = 0;
            int amount;
            int u;
            LocalDateTime dt;
            LocalDateTime dtLast = startDate;

            // Bids should be in a temporal order. In order to avoid the case where it randomly picks the end date we calculate
            // an offset of seconds we need to add to the end data in every iteration. This leeds to a somehow even distribution
            // of the bits over time.
            long difference = ChronoUnit.SECONDS.between( startDate, endDate );
            long offset = difference / numberOfBids;

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
                dt = dateProducer.randomDateBetweenTwoDates( dtLast, endDate.minusSeconds( offset * (numberOfBids - (j + 1)) ) );
                lastBid = amount;
                amount = amount * 100;
                addToInsertList( new InsertBid( auctionId, u, amount, dt ).getNewQuery() );
                dtLast = dt;
            }
            executeInsertList();

            // create pictures
            InsertPicture pictureBuilder = new InsertPicture( auctionId );
            numberOfPictures = ThreadLocalRandom.current().nextInt( minNumberOfPicturesPerAuction, maxNumberOfPicturesPerAuction );
            for ( int j = 0; j < numberOfPictures; j++ ) {
                addToInsertList( pictureBuilder.getNewQuery() );
            }
            executeInsertList();
        }
    }


    private void addToInsertList( BatchableInsert query ) throws ExecutorException {
        batchList.add( query );
        if ( batchList.size() >= config.maxBatchSize ) {
            executeInsertList();
        }
    }


    private void executeInsertList() throws ExecutorException {
        theExecutor.executeInsertList( batchList, config );
        theExecutor.executeCommit();
        batchList.clear();
    }


    public void abort() {
        aborted = true;
    }

}
