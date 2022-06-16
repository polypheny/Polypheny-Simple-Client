/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-14.06.22, 09:42 The Polypheny Project
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

package org.polypheny.simpleclient.scenario.multibench;

import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import org.polypheny.simpleclient.scenario.AbstractConfig;
import org.polypheny.simpleclient.scenario.docbench.DocBenchConfig;
import org.polypheny.simpleclient.scenario.gavel.GavelConfig;
import org.polypheny.simpleclient.scenario.graph.GraphBenchConfig;
import org.polypheny.simpleclient.scenario.knnbench.KnnBenchConfig;


public class MultiBenchConfig extends AbstractConfig {

    @Getter
    private final MultiBenchDocBenchConfig docBenchConfig;
    @Getter
    private final MultiBenchGraphBenchConfig graphBenchConfig;
    @Getter
    private final MultiBenchKnnBenchConfig knnBenchConfig;
    @Getter
    private final MultiBenchGavelConfig gavelConfig;


    public final long seed;

    public final int batchSize;

    public double writeRatio;

    public final int numberOfDocBenchQueries;
    public final int numberOfKnnBenchQueries;
    public final int numberOfGraphBenchQueries;
    public final int numberOfGavelQueries;


    public MultiBenchConfig( Properties properties, int multiplier ) {
        super( "multibench", "polypheny", properties );

        seed = getLongProperty( properties, "seed" );
        batchSize = getIntProperty( properties, "batchSize" );

        writeRatio = Double.parseDouble( properties.getProperty( "writeRatio" ) ) / 100.0;

        numberOfDocBenchQueries = getIntProperty( properties, "numberOfDocBenchQueries" );
        numberOfGraphBenchQueries = getIntProperty( properties, "numberOfGraphBenchQueries" );
        numberOfKnnBenchQueries = getIntProperty( properties, "numberOfKnnBenchQueries" );
        numberOfGavelQueries = getIntProperty( properties, "numberOfGavelQueries" );

        if ( numberOfDocBenchQueries > 0 ) {
            docBenchConfig = new MultiBenchDocBenchConfig( properties, multiplier );
        } else {
            docBenchConfig = null;
        }
        if ( numberOfGraphBenchQueries > 0 ) {
            graphBenchConfig = new MultiBenchGraphBenchConfig( properties, multiplier );
        } else {
            graphBenchConfig = null;
        }
        if ( numberOfKnnBenchQueries > 0 ) {
            knnBenchConfig = new MultiBenchKnnBenchConfig( properties, multiplier );
        } else {
            knnBenchConfig = null;
        }
        if ( numberOfGavelQueries > 0 ) {
            gavelConfig = new MultiBenchGavelConfig( properties, multiplier );
        } else {
            gavelConfig = null;
        }
    }


    public MultiBenchConfig( Map<String, String> cdl ) {
        super( "multibench", cdl.get( "store" ), cdl );

        seed = Integer.parseInt( cdl.get( "seed" ) );
        batchSize = Integer.parseInt( cdl.get( "batchSize" ) );

        writeRatio = Double.parseDouble( cdl.get( "writeRatio" ) ) / 100.0;

        numberOfDocBenchQueries = Integer.parseInt( cdl.get( "numberOfDocBenchQueries" ) );
        numberOfGraphBenchQueries = Integer.parseInt( cdl.get( "numberOfGraphBenchQueries" ) );
        numberOfKnnBenchQueries = Integer.parseInt( cdl.get( "numberOfKnnBenchQueries" ) );
        numberOfGavelQueries = Integer.parseInt( cdl.get( "numberOfGavelQueries" ) );

        if ( numberOfDocBenchQueries > 0 ) {
            docBenchConfig = new MultiBenchDocBenchConfig( cdl );
        } else {
            docBenchConfig = null;
        }
        if ( numberOfGraphBenchQueries > 0 ) {
            graphBenchConfig = new MultiBenchGraphBenchConfig( cdl );
        } else {
            graphBenchConfig = null;
        }
        if ( numberOfKnnBenchQueries > 0 ) {
            knnBenchConfig = new MultiBenchKnnBenchConfig( cdl );
        } else {
            knnBenchConfig = null;
        }
        if ( numberOfGavelQueries > 0 ) {
            gavelConfig = new MultiBenchGavelConfig( cdl );
        } else {
            gavelConfig = null;
        }
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return false;
    }


    public class MultiBenchDocBenchConfig extends DocBenchConfig {

        protected MultiBenchDocBenchConfig( Properties properties, int multiplier ) {
            super( "docbench", "polypheny-mongoql", properties );
            settings();
        }


        protected MultiBenchDocBenchConfig( Map<String, String> cdl ) {
            super( "docbench", "polypheny-mongoql", cdl );
            settings();
        }


        private void settings() {
            seed = MultiBenchConfig.this.seed;
            batchSize = MultiBenchConfig.this.batchSize;

            numberOfUpdateQueries = Double.valueOf( MultiBenchConfig.this.numberOfDocBenchQueries * writeRatio ).intValue();
            numberOfFindQueries = numberOfDocBenchQueries - numberOfUpdateQueries;

            numberOfDocuments = 1_000_000;
            minNumberOfAttributes = 5;
            maxNumberOfAttributes = 25;
            sizeOfAttributesPool = 500;
            sizeOfValuesPool = 500;
            valuesStringMinLength = 8;
            valuesStringMaxLength = 15;
        }

    }


    public class MultiBenchKnnBenchConfig extends KnnBenchConfig {

        protected MultiBenchKnnBenchConfig( Properties properties, int multiplier ) {
            super( "knnBench", "polypheny-jdbc", properties );
            settings();
        }


        protected MultiBenchKnnBenchConfig( Map<String, String> cdl ) {
            super( "knnBench", "polypheny-jdbc", cdl );
            settings();
        }


        private void settings() {
            //dataStoreFeature
            //dataStoreMetadata

            randomSeedInsert = MultiBenchConfig.this.seed;
            randomSeedQuery = MultiBenchConfig.this.seed + 1;

            batchSizeInserts = 2500;
            batchSizeQueries = 10;

            dimensionFeatureVectors = 10;
            numberOfEntries = 100000;

            numberOfSimpleKnnIntFeatureQueries = numberOfKnnBenchQueries / 4;
            numberOfSimpleKnnRealFeatureQueries = 0;
            numberOfSimpleMetadataQueries = numberOfKnnBenchQueries / 4;
            numberOfSimpleKnnIdIntFeatureQueries = numberOfKnnBenchQueries / 4;
            numberOfSimpleKnnIdRealFeatureQueries = 0;
            numberOfMetadataKnnIntFeatureQueries = numberOfKnnBenchQueries - (numberOfSimpleKnnIntFeatureQueries + numberOfSimpleMetadataQueries + numberOfSimpleKnnIdIntFeatureQueries);
            numberOfMetadataKnnRealFeatureQueries = 0;

            limitKnnQueries = 10;
            distanceNorm = "L2";
        }

    }


    public class MultiBenchGraphBenchConfig extends GraphBenchConfig {

        protected MultiBenchGraphBenchConfig( Properties properties, int multiplier ) {
            super( "graph", "polypheny-cypher", properties );
            settings();
        }


        protected MultiBenchGraphBenchConfig( Map<String, String> cdl ) {
            super( "graph", "polypheny-cypher", cdl );
            settings();
        }


        private void settings() {
            seed = MultiBenchConfig.this.seed;
            clusterSeed = MultiBenchConfig.this.seed + 1;

            // logistics
            batchSizeCreates = 5;

            // clusters
            clusters = 50;
            minClusterSize = 10;
            maxClusterSize = 20;
            minClusterConnections = 1;
            maxClusterConnections = 2;

            // paths
            paths = 100;
            minPathLength = 1;
            maxPathLength = 6;

            // content
            usedLabels = 1;
            this.properties = 3;
            listSize = 3;

            int numberOfWriteQueries = Double.valueOf( MultiBenchConfig.this.numberOfGraphBenchQueries * writeRatio ).intValue();
            int numberOfReadQueries = numberOfGraphBenchQueries - numberOfWriteQueries;

            // queries
            numberOfNodeFilterQueries = numberOfReadQueries / 4;
            numberOfEdgeMatchQueries = numberOfReadQueries / 4;

            numberOfPropertyCountQueries = numberOfReadQueries / 4;
            numberOfUnwindQueries = 0;

            numberOfFindNeighborsQueries = numberOfReadQueries - (numberOfNodeFilterQueries + numberOfEdgeMatchQueries + numberOfPropertyCountQueries);
            numberOfDifferentLengthQueries = 0;
            numberOfShortestPathQueries = 0;

            // DML
            numberOfDeleteQueries = numberOfWriteQueries / 2;
            numberOfDeleteQueries = numberOfWriteQueries / 4;
            numberOfInsertQueries = numberOfWriteQueries - (numberOfDeleteQueries + numberOfDeleteQueries);

            if ( maxPathLength - 1 <= 0 ) {
                this.highestLabel = 1;
            } else {
                this.highestLabel = maxPathLength - 1;
            }

            if ( usedLabels - 1 <= 0 ) {
                this.highestProperty = 1;
            } else {
                this.highestProperty = usedLabels - 1;
            }
        }

    }


    public class MultiBenchGavelConfig extends GavelConfig {

        protected MultiBenchGavelConfig( Properties properties, int multiplier ) {
            super( "gavel", "polypheny-jdbc", properties );
            settings();
        }


        protected MultiBenchGavelConfig( Map<String, String> cdl ) {
            super( "gavel", "polypheny-jdbc", cdl );
            settings();
        }


        private void settings() {
            int numberOfWriteQueries = Double.valueOf( MultiBenchConfig.this.numberOfGavelQueries * writeRatio ).intValue();
            int numberOfReadQueries = numberOfGavelQueries - numberOfWriteQueries;

            numberOfGetAuctionQueries = numberOfReadQueries / 5;
            numberOfGetBidQueries = numberOfReadQueries / 5;
            numberOfGetUserQueries = numberOfReadQueries / 5;
            numberOfGetCurrentlyHighestBidOnAuctionQueries = numberOfReadQueries / 5;

            int remaining = numberOfReadQueries - (numberOfGetAuctionQueries + numberOfGetBidQueries + numberOfGetUserQueries + numberOfGetCurrentlyHighestBidOnAuctionQueries);

            numberOfGetAllBidsOnAuctionQueries = remaining / 8;
            numberOfSearchAuctionQueries = remaining / 8;

            numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries = 0;
            numberOfCountAuctionsQueries = remaining / 8;
            numberOfCountBidsQueries = remaining / 8;
            numberOfTopTenCitiesByNumberOfCustomersQueries = remaining / 8;

            totalNumOfPriceBetweenAndNotInCategoryQueries = remaining / 8;
            totalNumOfHighestOverallBidQueries = remaining / 8;
            totalNumOfTopHundredSellerByNumberOfAuctionsQueries = remaining - (numberOfGetAllBidsOnAuctionQueries + numberOfSearchAuctionQueries + numberOfCountAuctionsQueries + numberOfCountBidsQueries + numberOfTopTenCitiesByNumberOfCustomersQueries + totalNumOfPriceBetweenAndNotInCategoryQueries + totalNumOfHighestOverallBidQueries);

            // DML
            numberOfAddAuctionQueries = (numberOfWriteQueries / 4) * 3;
            remaining = numberOfWriteQueries - numberOfAddAuctionQueries;
            numberOfChangePasswordQueries = remaining / 4;
            numberOfChangeAuctionQueries = remaining / 4;
            numberOfAddUserQueries = remaining / 4;
            numberOfAddBidQueries = remaining - (numberOfChangePasswordQueries + numberOfChangeAuctionQueries + numberOfAddUserQueries + numberOfAddBidQueries);

            // Data Generation
            numberOfUsers = 1_000;
            numberOfAuctions = 1_000;
            numberOfCategories = 35;
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

            maxBatchSize = 1000;
            usePreparedBatchForDataInsertion = true;

            numberOfUserGenerationThreads = 2;
            numberOfAuctionGenerationThreads = 2;

            parallelizeUserGenerationAndAuctionGeneration = false;
        }

    }

}