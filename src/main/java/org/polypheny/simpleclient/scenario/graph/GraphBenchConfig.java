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

package org.polypheny.simpleclient.scenario.graph;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.scenario.AbstractConfig;


@Slf4j
public class GraphBenchConfig extends AbstractConfig {

    //public final long randomSeedInsert;
    //public final long randomSeedQuery;
    public final int clusters;
    public final int minClusterSize;
    public final int maxClusterSize;
    public final int minPathLength;
    public final int maxPathLength;
    public final int usedLabels;
    public final int properties;
    public final int paths;
    public final int batchSizeCreates;
    public final int minClusterConnections;
    public final int maxClusterConnections;
    public final int clusterSeed;
    public final int numberOfEdgeMatchQueries;
    public final int seed;
    public final int numberOfPropertyCountQueries;
    public final int numberOfFindNeighborsQueries;
    public final int listSize;
    public final int numberOfUnwindQueries;
    public final int numberOfNodeFilterQueries;
    public final int numberOfDifferentLengthQueries;
    public final int numberOfShortestPathQueries;
    public final int numberOfSetPropertyQueries;
    public final int numberOfDeleteQueries;
    public final int numberOfInsertQueries;

    public final int highestLabel;
    public final int highestProperty;

    public final int numberOfWarmUpIterations;


    public GraphBenchConfig( Properties properties, int multiplier ) {
        super( "graph", "polypheny" );

        pdbBranch = null;
        puiBranch = null;
        buildUi = false;
        resetCatalog = false;
        memoryCatalog = false;
        deployStoresUsingDocker = false;

        router = "icarus"; // For old routing, to be removed

        routers = new String[]{ "Simple", "Icarus", "FullPlacement" };
        newTablePlacementStrategy = "Single";
        planSelectionStrategy = "Best";
        preCostRatio = 50;
        postCostRatio = 50;
        routingCache = true;
        postCostAggregation = "onWarmup";

        planAndImplementationCaching = "Both";

        clusterSeed = getIntProperty( properties, "clusterSeed" );
        seed = getIntProperty( properties, "seed" );

        clusters = getIntProperty( properties, "amountClusters" );
        minClusterSize = getIntProperty( properties, "minClusterSize" );
        maxClusterSize = getIntProperty( properties, "maxClusterSize" );
        minPathLength = getIntProperty( properties, "minPathLength" );
        maxPathLength = getIntProperty( properties, "maxPathLength" );

        paths = getIntProperty( properties, "amountPaths" );
        batchSizeCreates = getIntProperty( properties, "batchSizeCreates" );
        minClusterConnections = getIntProperty( properties, "minClusterConnections" );
        maxClusterConnections = getIntProperty( properties, "maxClusterConnections" );

        usedLabels = getIntProperty( properties, "usedLabels" );
        this.properties = getIntProperty( properties, "amountProperties" );
        listSize = getIntProperty( properties, "listSize" );

        progressReportBase = getIntProperty( properties, "progressReportBase" );
        numberOfThreads = getIntProperty( properties, "numberOfThreads" );
        numberOfWarmUpIterations = getIntProperty( properties, "numberOfWarmUpIterations" );

        numberOfEdgeMatchQueries = getIntProperty( properties, "numberOfEdgeMatchQueries" ) * multiplier;
        numberOfPropertyCountQueries = getIntProperty( properties, "numberOfPropertyCountQueries" ) * multiplier;
        numberOfFindNeighborsQueries = getIntProperty( properties, "numberOfFindNeighborsQueries" ) * multiplier;
        numberOfUnwindQueries = getIntProperty( properties, "numberOfUnwindQueries" ) * multiplier;
        numberOfNodeFilterQueries = getIntProperty( properties, "numberOfNodeFilterQueries" ) * multiplier;
        numberOfDifferentLengthQueries = getIntProperty( properties, "numberOfDifferentLengthQueries" ) * multiplier;
        numberOfShortestPathQueries = getIntProperty( properties, "numberOfShortestPathQueries" ) * multiplier;
        numberOfSetPropertyQueries = getIntProperty( properties, "numberOfSetPropertyQueries" ) * multiplier;
        numberOfDeleteQueries = getIntProperty( properties, "numberOfDeleteQueries" ) * multiplier;
        numberOfInsertQueries = getIntProperty( properties, "numberOfInsertQueries" ) * multiplier;

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


    public GraphBenchConfig( Map<String, String> cdl ) {
        super( "graph", cdl.get( "store" ) );

        pdbBranch = cdl.get( "pdbBranch" );
        puiBranch = cdl.get( "puiBranch" );
        buildUi = Boolean.parseBoolean( cdlGetOrDefault( cdl, "buildUi", "false" ) );
        resetCatalog = Boolean.parseBoolean( cdl.get( "resetCatalog" ) );
        memoryCatalog = Boolean.parseBoolean( cdl.get( "memoryCatalog" ) );

        dataStores.addAll( Arrays.asList( cdl.get( "dataStore" ).split( "_" ) ) );
        deployStoresUsingDocker = Boolean.parseBoolean( cdlGetOrDefault( cdl, "deployStoresUsingDocker", "false" ) );

        router = cdl.get( "router" ); // For old routing, to be removed

        routers = cdlGetOrDefault( cdl, "routers", "Simple_Icarus_FullPlacement" ).split( "_" );
        newTablePlacementStrategy = cdlGetOrDefault( cdl, "newTablePlacementStrategy", "Single" );
        planSelectionStrategy = cdlGetOrDefault( cdl, "planSelectionStrategy", "Best" );

        preCostRatio = Integer.parseInt( cdlGetOrDefault( cdl, "preCostRatio", "50%" ).replace( "%", "" ).trim() );
        postCostRatio = Integer.parseInt( cdlGetOrDefault( cdl, "postCostRatio", "50%" ).replace( "%", "" ).trim() );
        routingCache = Boolean.parseBoolean( cdlGetOrDefault( cdl, "routingCache", "true" ) );
        postCostAggregation = cdlGetOrDefault( cdl, "postCostAggregation", "onWarmup" );

        planAndImplementationCaching = cdlGetOrDefault( cdl, "planAndImplementationCaching", "Both" );

        progressReportBase = 100;
        numberOfThreads = Integer.parseInt( cdl.get( "numberOfThreads" ) );
        numberOfWarmUpIterations = Integer.parseInt( cdl.get( "numberOfWarmUpIterations" ) );

        clusters = Integer.parseInt( cdl.get( "amountClusters" ) );
        minClusterSize = Integer.parseInt( cdl.get( "minClusterSize" ) );
        maxClusterSize = Integer.parseInt( cdl.get( "maxClusterSize" ) );
        minPathLength = Integer.parseInt( cdl.get( "minPathLength" ) );
        maxPathLength = Integer.parseInt( cdl.get( "maxPathLength" ) );
        paths = Integer.parseInt( cdl.get( "amountPaths" ) );
        batchSizeCreates = Integer.parseInt( cdl.get( "batchSizeCreates" ) );
        minClusterConnections = Integer.parseInt( cdl.get( "minClusterConnections" ) );
        maxClusterConnections = Integer.parseInt( cdl.get( "maxClusterConnections" ) );

        usedLabels = Integer.parseInt( cdl.get( "usedLabels" ) );
        this.properties = Integer.parseInt( cdl.get( "amountProperties" ) );
        listSize = Integer.parseInt( cdl.get( "listSize" ) );

        clusterSeed = Integer.parseInt( cdl.get( "clusterSeed" ) );
        seed = Integer.parseInt( cdl.get( "seed" ) );

        numberOfEdgeMatchQueries = Integer.parseInt( cdl.get( "numberOfEdgeMatchQueries" ) );
        numberOfPropertyCountQueries = Integer.parseInt( cdl.get( "numberOfPropertyCountQueries" ) );
        numberOfFindNeighborsQueries = Integer.parseInt( cdl.get( "numberOfFindNeighborsQueries" ) );
        numberOfUnwindQueries = Integer.parseInt( cdl.get( "numberOfUnwindQueries" ) );
        numberOfNodeFilterQueries = Integer.parseInt( cdl.get( "numberOfNodeFilterQueries" ) );
        numberOfDifferentLengthQueries = Integer.parseInt( cdl.get( "numberOfDifferentLengthQueries" ) );
        numberOfShortestPathQueries = Integer.parseInt( cdl.get( "numberOfShortestPathQueries" ) );
        numberOfSetPropertyQueries = Integer.parseInt( cdl.get( "numberOfSetPropertyQueries" ) );
        numberOfDeleteQueries = Integer.parseInt( cdl.get( "numberOfDeleteQueries" ) );
        numberOfInsertQueries = Integer.parseInt( cdl.get( "numberOfInsertQueries" ) );

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


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return true;
    }

}
