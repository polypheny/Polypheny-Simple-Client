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

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.scenario.AbstractConfig;


@Slf4j
public class GavelConfig extends AbstractConfig {

    public final int numberOfAddUserQueries;
    public final int numberOfChangePasswordQueries;
    public final int numberOfAddAuctionQueries;
    public final int numberOfAddBidQueries;
    public final int numberOfChangeAuctionQueries;
    public final int numberOfGetAuctionQueries;
    public final int numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries;
    public final int numberOfSearchAuctionQueries;
    public final int numberOfCountAuctionsQueries;
    public final int numberOfTopTenCitiesByNumberOfCustomersQueries;
    public final int numberOfCountBidsQueries;
    public final int numberOfGetBidQueries;
    public final int numberOfGetUserQueries;
    public final int numberOfGetAllBidsOnAuctionQueries;
    public final int numberOfGetCurrentlyHighestBidOnAuctionQueries;
    public final int totalNumOfPriceBetweenAndNotInCategoryQueries;
    public final int totalNumOfHighestOverallBidQueries;
    public final int totalNumOfTopHundredSellerByNumberOfAuctionsQueries;

    public final int numberOfUsers;
    public final int numberOfAuctions;
    public final int numberOfCategories;
    public final int auctionTitleMinLength;
    public final int auctionTitleMaxLength;
    public final int auctionDescriptionMinLength;
    public final int auctionDescriptionMaxLength;
    public final int auctionDateMaxYearsInPast;
    public final int auctionNumberOfDays;
    public final int minNumberOfBidsPerAuction;
    public final int maxNumberOfBidsPerAuction;
    public final int minNumberOfPicturesPerAuction;
    public final int maxNumberOfPicturesPerAuction;

    public final int maxBatchSize;
    public final boolean usePreparedBatchForDataInsertion;

    public final int numberOfUserGenerationThreads;
    public final int numberOfAuctionGenerationThreads;
    public final boolean parallelizeUserGenerationAndAuctionGeneration;

    public final int numberOfWarmUpIterations;


    public GavelConfig( Properties properties, int multiplier ) {
        super( "gavel", "polypheny" );

        pdbBranch = null;
        puiBranch = null;
        buildUi = false;
        resetCatalog = false;
        memoryCatalog = false;
        deployStoresUsingDocker = false;

        dataStores.add( "hsqldb" );
        planAndImplementationCaching = "Both";

        router = "icarus"; // For old routing, to be removed

        routers = new String[]{ "Simple", "Icarus", "FullPlacement" };
        newTablePlacementStrategy = "Single";
        planSelectionStrategy = "Best";
        preCostRatio = 50;
        postCostRatio = 50;
        routingCache = true;
        postCostAggregation = "onWarmup";

        progressReportBase = getIntProperty( properties, "progressReportBase" );
        numberOfThreads = getIntProperty( properties, "numberOfThreads" );
        numberOfWarmUpIterations = getIntProperty( properties, "numberOfWarmUpIterations" );

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
        super( "gavel", cdl.get( "store" ) );

        pdbBranch = cdl.get( "pdbBranch" );
        puiBranch = cdl.get( "puiBranch" );
        buildUi = Boolean.parseBoolean( cdlGetOrDefault( cdl, "buildUi", "false" ) );

        deployStoresUsingDocker = Boolean.parseBoolean( cdlGetOrDefault( cdl, "deployStoresUsingDocker", "false" ) );

        resetCatalog = Boolean.parseBoolean( cdl.get( "resetCatalog" ) );
        memoryCatalog = Boolean.parseBoolean( cdl.get( "memoryCatalog" ) );

        numberOfThreads = Integer.parseInt( cdl.get( "numberOfThreads" ) );
        numberOfWarmUpIterations = Integer.parseInt( cdlGetOrDefault( cdl, "numberOfWarmUpIterations", "4" ) );

        dataStores.addAll( Arrays.asList( cdl.get( "dataStore" ).split( "_" ) ) );
        planAndImplementationCaching = cdlGetOrDefault( cdl, "planAndImplementationCaching", "Both" );

        router = cdl.get( "router" ); // For old routing, to be removed

        routers = cdlGetOrDefault( cdl, "routers", "Simple_Icarus_FullPlacement" ).split( "_" );
        newTablePlacementStrategy = cdlGetOrDefault( cdl, "newTablePlacementStrategy", "Single" );
        planSelectionStrategy = cdlGetOrDefault( cdl, "planSelectionStrategy", "Best" );

        preCostRatio = Integer.parseInt( cdlGetOrDefault( cdl, "preCostRatio", "50%" ).replace( "%", "" ).trim() );
        postCostRatio = Integer.parseInt( cdlGetOrDefault( cdl, "postCostRatio", "50%" ).replace( "%", "" ).trim() );
        routingCache = Boolean.parseBoolean( cdlGetOrDefault( cdl, "routingCache", "true" ) );
        postCostAggregation = cdlGetOrDefault( cdl, "postCostAggregation", "onWarmup" );

        // Benchmark
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

        progressReportBase = 100;
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return this.usePreparedBatchForDataInsertion;
    }

}
