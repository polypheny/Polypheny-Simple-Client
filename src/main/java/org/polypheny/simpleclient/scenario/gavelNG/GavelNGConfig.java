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

package org.polypheny.simpleclient.scenario.gavelNG;


import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.scenario.AbstractConfig;


@Slf4j
public class GavelNGConfig extends AbstractConfig {

    public final int numberOfUsers;
    public final int numberOfAuctions;
    public final int numberOfCategories;
    public final int numberOfConditions;
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


    public GavelNGConfig( Properties properties, int multiplier ) {
        super( "gavel", "polypheny" );

        pdbBranch = null;
        puiBranch = null;
        buildUi = false;
        resetCatalog = true;
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

        numberOfUsers = getIntProperty( properties, "numberOfUsers" ) * multiplier;
        numberOfAuctions = getIntProperty( properties, "numberOfAuctions" ) * multiplier;
        numberOfCategories = getIntProperty( properties, "numberOfCategories" );
        numberOfConditions = getIntProperty( properties, "numberOfConditions" );
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
        usePreparedBatchForDataInsertion = getBooleanProperty( properties, "usePreparedBatchForDataInsertion" );

        numberOfUserGenerationThreads = getIntProperty( properties, "numberOfUserGenerationThreads" );
        numberOfAuctionGenerationThreads = getIntProperty( properties, "numberOfAuctionGenerationThreads" );
        parallelizeUserGenerationAndAuctionGeneration = getBooleanProperty( properties, "parallelizeUserGenerationAndAuctionGeneration" );
    }


    public GavelNGConfig( Map<String, String> cdl ) {
        super( "gavelng", cdl.get( "store" ) );

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

        // Data Generation
        numberOfUsers = Integer.parseInt( cdl.get( "numberOfUsers" ) );
        numberOfAuctions = Integer.parseInt( cdl.get( "numberOfAuctions" ) );
        numberOfCategories = Integer.parseInt( cdl.get( "numberOfCategories" ) );
        numberOfConditions = Integer.parseInt( cdl.get( "numberOfConditions" ) );
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

        // Policy Settings
        storePolicies.addAll( Arrays.asList( cdl.get( "storePolicy" ).split( "," ) ) );
        selfAdaptingPolicies.addAll( Arrays.asList( cdl.get( "selfAdaptingPolicy" ).split( "," ) ) );
        multipleDataStores.addAll( Arrays.asList( cdl.get( "multipleDataStores" ).split( "," ) ) );
        usePolicies = cdl.get( "policySelfAdaptiveness" );
        statisticActiveTracking = Boolean.parseBoolean( cdl.get( "statisticActiveTracking" ) );

    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return this.usePreparedBatchForDataInsertion;
    }

}
