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

    public final int numberOfDocBenchQueries;


    public MultiBenchConfig( Properties properties, int multiplier ) {
        super( "multibench", "polypheny", properties );

        seed = getLongProperty( properties, "seed" );
        batchSize = getIntProperty( properties, "batchSize" );

        numberOfDocBenchQueries = 1_000_000;

        docBenchConfig = new MultiBenchDocBenchConfig( properties, multiplier );
        graphBenchConfig = new MultiBenchGraphBenchConfig( properties, multiplier );
        knnBenchConfig = new MultiBenchKnnBenchConfig( properties, multiplier );
        gavelConfig = new MultiBenchGavelConfig( properties, multiplier );
    }


    public MultiBenchConfig( Map<String, String> cdl ) {
        super( "multibench", cdl.get( "store" ), cdl );

        seed = Integer.parseInt( cdl.get( "seed" ) );
        batchSize = Integer.parseInt( cdl.get( "batchSize" ) );

        numberOfDocBenchQueries = 1_000_000;

        docBenchConfig = new MultiBenchDocBenchConfig( cdl );
        graphBenchConfig = new MultiBenchGraphBenchConfig( cdl );
        knnBenchConfig = new MultiBenchKnnBenchConfig( cdl );
        gavelConfig = new MultiBenchGavelConfig( cdl );
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

            numberOfQueries = MultiBenchConfig.this.numberOfDocBenchQueries;

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

            numberOfSimpleKnnIntFeatureQueries = 100;
            numberOfSimpleKnnRealFeatureQueries = 0;
            numberOfSimpleMetadataQueries = 100;
            numberOfSimpleKnnIdIntFeatureQueries = 100;
            numberOfSimpleKnnIdRealFeatureQueries = 0;
            numberOfMetadataKnnIntFeatureQueries = 100;
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

            // queries
            numberOfNodeFilterQueries = 30;
            numberOfEdgeMatchQueries = 30;

            numberOfPropertyCountQueries = 30;
            numberOfUnwindQueries = 0;

            numberOfFindNeighborsQueries = 30;
            numberOfDifferentLengthQueries = 0;
            numberOfShortestPathQueries = 0;

            numberOfSetPropertyQueries = 20;
            numberOfDeleteQueries = 5;
            numberOfInsertQueries = 10;

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
            numberOfGetAuctionQueries = 200;
            numberOfGetBidQueries = 200;
            numberOfGetUserQueries = 200;
            numberOfGetAllBidsOnAuctionQueries = 20;
            numberOfGetCurrentlyHighestBidOnAuctionQueries = 200;
            numberOfSearchAuctionQueries = 20;

            numberOfChangePasswordQueries = 20;
            numberOfChangeAuctionQueries = 20;

            numberOfAddAuctionQueries = 100;
            numberOfAddUserQueries = 10;
            numberOfAddBidQueries = 10;

            numberOfGetTheNextHundredEndingAuctionsOfACategoryQueries = 0;
            numberOfCountAuctionsQueries = 20;
            numberOfCountBidsQueries = 20;

            numberOfTopTenCitiesByNumberOfCustomersQueries = 20;

            totalNumOfPriceBetweenAndNotInCategoryQueries = 20;
            totalNumOfHighestOverallBidQueries = 5;
            totalNumOfTopHundredSellerByNumberOfAuctionsQueries = 5;

            // Data Generation
            numberOfUsers = 1_000;
            numberOfAuctions = 10_000;
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