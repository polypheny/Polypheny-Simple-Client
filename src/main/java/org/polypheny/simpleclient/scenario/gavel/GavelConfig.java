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
 */

package org.polypheny.simpleclient.scenario.gavel;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.scenario.AbstractConfig;


@Slf4j
public class GavelConfig extends AbstractConfig {

    public int numberOfAddUserQueries;
    public int numberOfChangePasswordQueries;
    public int numberOfAddAuctionQueries;
    public int numberOfAddBidQueries;
    public int numberOfChangeAuctionQueries;
    public int numberOfGetAuctionQueries;
    public int numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries;
    public int numberOfSearchAuctionQueries;
    public int numberOfCountAuctionsQueries;
    public int numberOfTopTenCitiesByNumberOfCustomersQueries;
    public int numberOfCountBidsQueries;
    public int numberOfGetBidQueries;
    public int numberOfGetUserQueries;
    public int numberOfGetAllBidsOnAuctionQueries;
    public int numberOfGetCurrentlyHighestBidOnAuctionQueries;
    public int totalNumOfPriceBetweenAndNotInCategoryQueries;
    public int totalNumOfHighestOverallBidQueries;
    public int totalNumOfTopHundredSellerByNumberOfAuctionsQueries;

    public int numberOfUsers;
    public int numberOfAuctions;
    public int numberOfCategories;
    public int auctionTitleMinLength;
    public int auctionTitleMaxLength;
    public int auctionDescriptionMinLength;
    public int auctionDescriptionMaxLength;
    public int auctionDateMaxYearsInPast;
    public int auctionNumberOfDays;
    public int minNumberOfBidsPerAuction;
    public int maxNumberOfBidsPerAuction;
    public int minNumberOfPicturesPerAuction;
    public int maxNumberOfPicturesPerAuction;

    public int maxBatchSize;
    public boolean usePreparedBatchForDataInsertion;

    public int numberOfUserGenerationThreads;
    public int numberOfAuctionGenerationThreads;
    public boolean parallelizeUserGenerationAndAuctionGeneration;


    public GavelConfig( Map<String, String> cdl, String system, int multiplier ) {
        super( "gavel", system, cdl );

        numberOfGetAuctionQueries = Integer.parseInt( cdl.get( "numberOfGetAuctionQueries" ) ) * multiplier;
        numberOfGetBidQueries = Integer.parseInt( cdl.get( "numberOfGetBidQueries" ) ) * multiplier;
        numberOfGetUserQueries = Integer.parseInt( cdl.get( "numberOfGetUserQueries" ) ) * multiplier;
        numberOfGetAllBidsOnAuctionQueries = Integer.parseInt( cdl.get( "numberOfGetAllBidsOnAuctionQueries" ) ) * multiplier;
        numberOfGetCurrentlyHighestBidOnAuctionQueries = Integer.parseInt( cdl.get( "numberOfGetCurrentlyHighestBidOnAuctionQueries" ) ) * multiplier;
        numberOfSearchAuctionQueries = Integer.parseInt( cdl.get( "numberOfSearchAuctionQueries" ) ) * multiplier;

        numberOfChangePasswordQueries = Integer.parseInt( cdl.get( "numberOfChangePasswordQueries" ) ) * multiplier;
        numberOfChangeAuctionQueries = Integer.parseInt( cdl.get( "numberOfChangeAuctionQueries" ) ) * multiplier;

        numberOfAddAuctionQueries = Integer.parseInt( cdl.get( "numberOfAddAuctionQueries" ) ) * multiplier;
        numberOfAddUserQueries = Integer.parseInt( cdl.get( "numberOfAddUserQueries" ) ) * multiplier;
        numberOfAddBidQueries = Integer.parseInt( cdl.get( "numberOfAddBidQueries" ) ) * multiplier;

        numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries = Integer.parseInt( cdl.get( "numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries" ) ) * multiplier;
        numberOfCountAuctionsQueries = Integer.parseInt( cdl.get( "numberOfCountAuctionsQueries" ) ) * multiplier;
        numberOfCountBidsQueries = Integer.parseInt( cdl.get( "numberOfCountBidsQueries" ) ) * multiplier;

        numberOfTopTenCitiesByNumberOfCustomersQueries = Integer.parseInt( cdl.get( "numberOfTopTenCitiesByNumberOfCustomersQueries" ) ) * multiplier;

        totalNumOfPriceBetweenAndNotInCategoryQueries = Integer.parseInt( cdl.get( "totalNumOfPriceBetweenAndNotInCategoryQueries" ) ) * multiplier;
        totalNumOfHighestOverallBidQueries = Integer.parseInt( cdl.get( "totalNumOfHighestOverallBidQueries" ) ) * multiplier;
        totalNumOfTopHundredSellerByNumberOfAuctionsQueries = Integer.parseInt( cdl.get( "totalNumOfTopHundredSellerByNumberOfAuctionsQueries" ) ) * multiplier;

        // Data Generation
        numberOfUsers = Integer.parseInt( cdl.get( "numberOfUsers" ) ) * multiplier;
        numberOfAuctions = Integer.parseInt( cdl.get( "numberOfAuctions" ) ) * multiplier;
        numberOfCategories = Integer.parseInt( cdl.get( "numberOfCategories" ) );
        auctionTitleMinLength = Integer.parseInt( cdl.get( "auctionTitleMinLength" ) );
        auctionTitleMaxLength = Integer.parseInt( cdl.get( "auctionTitleMaxLength" ) );
        auctionDescriptionMinLength = Integer.parseInt( cdl.get( "auctionDescriptionMinLength" ) );
        auctionDescriptionMaxLength = Integer.parseInt( cdl.get( "auctionDescriptionMaxLength" ) );
        auctionDateMaxYearsInPast = Integer.parseInt( cdl.get( "auctionDateMaxYearsInPast" ) );
        auctionNumberOfDays = Integer.parseInt( cdl.get( "auctionNumberOfDays" ) );
        minNumberOfBidsPerAuction = Integer.parseInt( cdl.get( "minNumberOfBidsPerAuction" ) );
        maxNumberOfBidsPerAuction = Integer.parseInt( cdl.get( "maxNumberOfBidsPerAuction" ) ) * multiplier;
        minNumberOfPicturesPerAuction = Integer.parseInt( cdl.get( "minNumberOfPicturesPerAuction" ) );
        maxNumberOfPicturesPerAuction = Integer.parseInt( cdl.get( "maxNumberOfPicturesPerAuction" ) ) * multiplier;

        maxBatchSize = Integer.parseInt( cdl.get( "maxBatchSize" ) );
        usePreparedBatchForDataInsertion = true;

        numberOfUserGenerationThreads = Integer.parseInt( cdl.get( "numberOfUserGenerationThreads" ) );
        numberOfAuctionGenerationThreads = Integer.parseInt( cdl.get( "numberOfAuctionGenerationThreads" ) );

        parallelizeUserGenerationAndAuctionGeneration = Boolean.parseBoolean( cdl.get( "parallelizeUserGenerationAndAuctionGeneration" ) );
    }


    public GavelConfig( Map<String, String> cdl ) {
        this( cdl, cdl.get( "store" ), 1 );
    }


    // For MultiBench
    protected GavelConfig( String scenario, String system, Map<String, String> cdl ) {
        super( scenario, system, cdl );
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return this.usePreparedBatchForDataInsertion;
    }

}
