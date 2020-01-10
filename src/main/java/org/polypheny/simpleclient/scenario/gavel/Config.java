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


import java.util.Map;
import java.util.Properties;
import org.slf4j.LoggerFactory;


public class Config {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger( Config.class );

    public final String store;

    public final String resultMode;
    public final String warmupResultMode;

    public final int numberOfThreads;
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

    public final int numberOfUserGenerationThreads;
    public final int numberOfAuctionGenerationThreads;
    public final boolean parallelizeUserGenerationAndAuctionGeneration;

    public final int progressReportBase;


    public Config( Properties properties ) {
        resultMode = "DEBUG";

        warmupResultMode = getStringProperty( properties, "warmupResultMode" );

        numberOfThreads = getIntProperty( properties, "numberOfThreads" );

        store = "polypheny";

        numberOfAddUserQueries = 0;
        numberOfChangePasswordQueries = 0;
        numberOfAddAuctionQueries = 0;
        numberOfAddBidQueries = 0;
        numberOfChangeAuctionQueries = 0;
        numberOfGetAuctionQueries = 0;
        numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries = 0;
        numberOfSearchAuctionQueries = 0;
        numberOfCountAuctionsQueries = 0;
        numberOfTopTenCitiesByNumberOfCustomersQueries = 0;
        numberOfCountBidsQueries = 0;
        numberOfGetBidQueries = 0;
        numberOfGetUserQueries = 0;
        numberOfGetAllBidsOnAuctionQueries = 0;
        numberOfGetCurrentlyHighestBidOnAuctionQueries = 0;

        numberOfUsers = getIntProperty( properties, "numberOfUsers" );
        numberOfAuctions = getIntProperty( properties, "numberOfAuctions" );
        numberOfCategories = getIntProperty( properties, "numberOfCategories" );
        auctionTitleMinLength = getIntProperty( properties, "auctionTitleMinLength" );
        auctionTitleMaxLength = getIntProperty( properties, "auctionTitleMaxLength" );
        auctionDescriptionMinLength = getIntProperty( properties, "auctionDescriptionMinLength" );
        auctionDescriptionMaxLength = getIntProperty( properties, "auctionDescriptionMaxLength" );
        auctionDateMaxYearsInPast = getIntProperty( properties, "auctionDateMaxYearsInPast" );
        auctionNumberOfDays = getIntProperty( properties, "auctionNumberOfDays" );
        minNumberOfBidsPerAuction = getIntProperty( properties, "minNumberOfBidsPerAuction" );
        maxNumberOfBidsPerAuction = getIntProperty( properties, "maxNumberOfBidsPerAuction" );
        minNumberOfPicturesPerAuction = getIntProperty( properties, "minNumberOfPicturesPerAuction" );
        maxNumberOfPicturesPerAuction = getIntProperty( properties, "maxNumberOfPicturesPerAuction" );
        maxBatchSize = getIntProperty( properties, "maxBatchSize" );

        numberOfUserGenerationThreads = getIntProperty( properties, "numberOfUserGenerationThreads" );
        numberOfAuctionGenerationThreads = getIntProperty( properties, "numberOfAuctionGenerationThreads" );
        parallelizeUserGenerationAndAuctionGeneration = getBooleanProperty( properties, "parallelizeUserGenerationAndAuctionGeneration" );

        progressReportBase = getIntProperty( properties, "progressReportBase" );

    }


    public Config( Map<String, String> cdl ) {
        if ( !cdl.containsKey( "type" ) ) {
            throw new RuntimeException( "Invalid CDL: Attribute \"type\" is missing" );
        }
        if ( cdl.get( "type" ).equalsIgnoreCase( "Benchmark" ) ) {
            resultMode = cdl.get( "resultMode" );
            warmupResultMode = cdl.get( "warmupResultMode" );

            numberOfThreads = Integer.parseInt( cdl.get( "numberOfThreads" ) );
            store = cdl.get( "store" );

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

            numberOfUsers = 0;
            numberOfAuctions = 0;
            numberOfCategories = 0;
            auctionTitleMinLength = 2;
            auctionTitleMaxLength = 8;
            auctionDescriptionMinLength = 5;
            auctionDescriptionMaxLength = 15;
            auctionDateMaxYearsInPast = 4;
            auctionNumberOfDays = 10;
            minNumberOfBidsPerAuction = 30;
            maxNumberOfBidsPerAuction = 200;
            minNumberOfPicturesPerAuction = 1;
            maxNumberOfPicturesPerAuction = 6;
            maxBatchSize = 0;
            numberOfUserGenerationThreads = 0;
            numberOfAuctionGenerationThreads = 0;
            parallelizeUserGenerationAndAuctionGeneration = false;
        } else if ( cdl.get( "type" ).equalsIgnoreCase( "DataGeneration" ) ) {
            resultMode = "DEBUG";
            warmupResultMode = "DEBUG";
            store = "polypheny";
            numberOfThreads = 0;
            numberOfAddUserQueries = 0;
            numberOfChangePasswordQueries = 0;
            numberOfAddAuctionQueries = 0;
            numberOfAddBidQueries = 0;
            numberOfChangeAuctionQueries = 0;
            numberOfGetAuctionQueries = 0;
            numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries = 0;
            numberOfSearchAuctionQueries = 0;
            numberOfCountAuctionsQueries = 0;
            numberOfTopTenCitiesByNumberOfCustomersQueries = 0;
            numberOfCountBidsQueries = 0;
            numberOfGetBidQueries = 0;
            numberOfGetUserQueries = 0;
            numberOfGetAllBidsOnAuctionQueries = 0;
            numberOfGetCurrentlyHighestBidOnAuctionQueries = 0;

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
            numberOfUserGenerationThreads = Integer.parseInt( cdl.get( "numberOfUserGenerationThreads" ) );
            numberOfAuctionGenerationThreads = Integer.parseInt( cdl.get( "numberOfAuctionGenerationThreads" ) );

            parallelizeUserGenerationAndAuctionGeneration = Boolean.parseBoolean( cdl.get( "parallelizeUserGenerationAndAuctionGeneration" ) );
        } else {
            throw new RuntimeException( "Unknown value of attribute \"type\": " + cdl.get( "type" ) );
        }
        progressReportBase = 100;

    }


    private String getStringProperty( Properties properties, String name ) {
        String str = getProperty( properties, name );
        if ( str == null ) {
            LOGGER.error( "Property '" + name + "' not found in config" );
            throw new RuntimeException( "Property '" + name + "' not found in config" );
        }
        return str;
    }


    private int getIntProperty( Properties properties, String name ) {
        String str = getProperty( properties, name );
        if ( str == null ) {
            LOGGER.error( "Property '" + name + "' not found in config" );
            throw new RuntimeException( "Property '" + name + "' not found in config" );
        }
        return Integer.parseInt( str );
    }


    private boolean getBooleanProperty( Properties properties, String name ) {
        String str = getStringProperty( properties, name );
        switch ( str ) {
            case "true":
                return true;
            case "false":
                return false;
            default:
                LOGGER.error( "Value for config property '" + name + "' is unknown. " + "Supported values are 'true' and 'false'. Current value is: " + str );
                throw new RuntimeException( "Value for config property '" + name + "' is unknown. " + "Supported values are 'true' and 'false'. Current value is: " + str );
        }
    }


    private String getProperty( Properties properties, String name ) {
        return properties.getProperty( name );
    }
}
