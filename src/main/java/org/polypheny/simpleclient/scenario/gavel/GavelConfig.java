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
import java.util.Properties;
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


    public GavelConfig( Properties properties, int multiplier ) {
        super( "gavel", "polypheny-jdbc", properties );

        numberOfAddUserQueries = getIntProperty( properties, "numberOfAddUserQueries" ) * multiplier;
        numberOfChangePasswordQueries = getIntProperty( properties, "numberOfChangePasswordQueries" ) * multiplier;
        numberOfAddAuctionQueries = getIntProperty( properties, "numberOfAddAuctionQueries" ) * multiplier;
        numberOfAddBidQueries = getIntProperty( properties, "numberOfAddBidQueries" ) * multiplier;
        numberOfChangeAuctionQueries = getIntProperty( properties, "numberOfChangeAuctionQueries" ) * multiplier;
        numberOfGetAuctionQueries = getIntProperty( properties, "numberOfGetAuctionQueries" ) * multiplier;
        numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries = getIntProperty( properties, "numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries" ) * multiplier;
        numberOfSearchAuctionQueries = getIntProperty( properties, "numberOfSearchAuctionQueries" ) * multiplier;
        numberOfCountAuctionsQueries = getIntProperty( properties, "numberOfCountAuctionsQueries" ) * multiplier;
        numberOfTopTenCitiesByNumberOfCustomersQueries = getIntProperty( properties, "numberOfTopTenCitiesByNumberOfCustomersQueries" ) * multiplier;
        numberOfCountBidsQueries = getIntProperty( properties, "numberOfCountBidsQueries" ) * multiplier;
        numberOfGetBidQueries = getIntProperty( properties, "numberOfGetBidQueries" ) * multiplier;
        numberOfGetUserQueries = getIntProperty( properties, "numberOfGetUserQueries" ) * multiplier;
        numberOfGetAllBidsOnAuctionQueries = getIntProperty( properties, "numberOfGetAllBidsOnAuctionQueries" ) * multiplier;
        numberOfGetCurrentlyHighestBidOnAuctionQueries = getIntProperty( properties, "numberOfGetCurrentlyHighestBidOnAuctionQueries" ) * multiplier;
        totalNumOfPriceBetweenAndNotInCategoryQueries = getIntProperty( properties, "totalNumOfPriceBetweenAndNotInCategoryQueries" ) * multiplier;
        totalNumOfHighestOverallBidQueries = getIntProperty( properties, "totalNumOfHighestOverallBidQueries" ) * multiplier;
        totalNumOfTopHundredSellerByNumberOfAuctionsQueries = getIntProperty( properties, "totalNumOfTopHundredSellerByNumberOfAuctionsQueries" ) * multiplier;

        numberOfUsers = getIntProperty( properties, "numberOfUsers" ) * multiplier;
        numberOfAuctions = getIntProperty( properties, "numberOfAuctions" ) * multiplier;
        numberOfCategories = getIntProperty( properties, "numberOfCategories" );
        auctionTitleMinLength = getIntProperty( properties, "auctionTitleMinLength" );
        auctionTitleMaxLength = getIntProperty( properties, "auctionTitleMaxLength" );
        auctionDescriptionMinLength = getIntProperty( properties, "auctionDescriptionMinLength" );
        auctionDescriptionMaxLength = getIntProperty( properties, "auctionDescriptionMaxLength" );
        auctionDateMaxYearsInPast = getIntProperty( properties, "auctionDateMaxYearsInPast" );
        auctionNumberOfDays = getIntProperty( properties, "auctionNumberOfDays" );
        minNumberOfBidsPerAuction = getIntProperty( properties, "minNumberOfBidsPerAuction" );
        maxNumberOfBidsPerAuction = getIntProperty( properties, "maxNumberOfBidsPerAuction" ) * multiplier;
        minNumberOfPicturesPerAuction = getIntProperty( properties, "minNumberOfPicturesPerAuction" );
        maxNumberOfPicturesPerAuction = getIntProperty( properties, "maxNumberOfPicturesPerAuction" ) * multiplier;

        maxBatchSize = getIntProperty( properties, "maxBatchSize" );
        usePreparedBatchForDataInsertion = true;

        numberOfUserGenerationThreads = getIntProperty( properties, "numberOfUserGenerationThreads" );
        numberOfAuctionGenerationThreads = getIntProperty( properties, "numberOfAuctionGenerationThreads" );
        parallelizeUserGenerationAndAuctionGeneration = getBooleanProperty( properties, "parallelizeUserGenerationAndAuctionGeneration" );
    }


    public GavelConfig( Map<String, String> cdl ) {
        super( "gavel", cdl.get( "store" ), cdl );

        numberOfGetAuctionQueries = Integer.parseInt( cdl.get( "numberOfGetAuctionQueries" ) );
        numberOfGetBidQueries = Integer.parseInt( cdl.get( "numberOfGetBidQueries" ) );
        numberOfGetUserQueries = Integer.parseInt( cdl.get( "numberOfGetUserQueries" ) );
        numberOfGetAllBidsOnAuctionQueries = Integer.parseInt( cdl.get( "numberOfGetAllBidsOnAuctionQueries" ) );
        numberOfGetCurrentlyHighestBidOnAuctionQueries = Integer.parseInt( cdl.get( "numberOfGetCurrentlyHighestBidOnAuctionQueries" ) );
        numberOfSearchAuctionQueries = Integer.parseInt( cdl.get( "numberOfSearchAuctionQueries" ) );

        numberOfChangePasswordQueries = Integer.parseInt( cdl.get( "numberOfChangePasswordQueries" ) );
        numberOfChangeAuctionQueries = Integer.parseInt( cdl.get( "numberOfChangeAuctionQueries" ) );

        numberOfAddAuctionQueries = Integer.parseInt( cdl.get( "numberOfAddAuctionQueries" ) );
        numberOfAddUserQueries = Integer.parseInt( cdl.get( "numberOfAddUserQueries" ) );
        numberOfAddBidQueries = Integer.parseInt( cdl.get( "numberOfAddBidQueries" ) );

        numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries = Integer.parseInt( cdl.get( "numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries" ) );
        numberOfCountAuctionsQueries = Integer.parseInt( cdl.get( "numberOfCountAuctionsQueries" ) );
        numberOfCountBidsQueries = Integer.parseInt( cdl.get( "numberOfCountBidsQueries" ) );

        numberOfTopTenCitiesByNumberOfCustomersQueries = Integer.parseInt( cdl.get( "numberOfTopTenCitiesByNumberOfCustomersQueries" ) );

        totalNumOfPriceBetweenAndNotInCategoryQueries = Integer.parseInt( cdl.get( "totalNumOfPriceBetweenAndNotInCategoryQueries" ) );
        totalNumOfHighestOverallBidQueries = Integer.parseInt( cdl.get( "totalNumOfHighestOverallBidQueries" ) );
        totalNumOfTopHundredSellerByNumberOfAuctionsQueries = Integer.parseInt( cdl.get( "totalNumOfTopHundredSellerByNumberOfAuctionsQueries" ) );

        // Data Generation
        numberOfUsers = Integer.parseInt( cdl.get( "numberOfUsers" ) );
        numberOfAuctions = Integer.parseInt( cdl.get( "numberOfAuctions" ) );
        numberOfCategories = Integer.parseInt( cdl.get( "numberOfCategories" ) );
        auctionTitleMinLength = Integer.parseInt( cdl.get( "auctionTitleMinLength" ) );
        auctionTitleMaxLength = Integer.parseInt( cdl.get( "auctionTitleMaxLength" ) );
        auctionDescriptionMinLength = Integer.parseInt( cdl.get( "auctionDescriptionMinLength" ) );
        auctionDescriptionMaxLength = Integer.parseInt( cdl.get( "auctionDescriptionMaxLength" ) );
        auctionDateMaxYearsInPast = Integer.parseInt( cdl.get( "auctionDateMaxYearsInPast" ) );
        auctionNumberOfDays = Integer.parseInt( cdl.get( "auctionNumberOfDays" ) );
        minNumberOfBidsPerAuction = Integer.parseInt( cdl.get( "minNumberOfBidsPerAuction" ) );
        maxNumberOfBidsPerAuction = Integer.parseInt( cdl.get( "maxNumberOfBidsPerAuction" ) );
        minNumberOfPicturesPerAuction = Integer.parseInt( cdl.get( "minNumberOfPicturesPerAuction" ) );
        maxNumberOfPicturesPerAuction = Integer.parseInt( cdl.get( "maxNumberOfPicturesPerAuction" ) );

        maxBatchSize = Integer.parseInt( cdl.get( "maxBatchSize" ) );
        usePreparedBatchForDataInsertion = true;

        numberOfUserGenerationThreads = Integer.parseInt( cdl.get( "numberOfUserGenerationThreads" ) );
        numberOfAuctionGenerationThreads = Integer.parseInt( cdl.get( "numberOfAuctionGenerationThreads" ) );

        parallelizeUserGenerationAndAuctionGeneration = Boolean.parseBoolean( cdl.get( "parallelizeUserGenerationAndAuctionGeneration" ) );
    }


    // For MultiBench
    protected GavelConfig( String scenario, String system, Map<String, String> cdl ) {
        super( scenario, system, cdl );
    }


    // For MultiBench
    protected GavelConfig( String scenario, String system, Properties properties ) {
        super( scenario, system, properties );
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return this.usePreparedBatchForDataInsertion;
    }

}
