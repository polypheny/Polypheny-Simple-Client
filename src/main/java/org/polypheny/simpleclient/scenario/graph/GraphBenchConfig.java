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

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.scenario.AbstractConfig;


@Slf4j
public class GraphBenchConfig extends AbstractConfig {

    public int clusters;
    public int minClusterSize;
    public int maxClusterSize;
    public int minPathLength;
    public int maxPathLength;
    public int usedLabels;
    public int properties;
    public int paths;
    public int batchSizeCreates;
    public int minClusterConnections;
    public int maxClusterConnections;
    public long clusterSeed;
    public int numberOfEdgeMatchQueries;
    public long seed;
    public int numberOfPropertyCountQueries;
    public int numberOfFindNeighborsQueries;
    public int listSize;
    public int numberOfUnwindQueries;
    public int numberOfNodeFilterQueries;
    public int numberOfDifferentLengthQueries;
    public int numberOfShortestPathQueries;
    public int numberOfSetPropertyQueries;
    public int numberOfDeleteQueries;
    public int numberOfInsertQueries;

    public int highestLabel;
    public int highestProperty;


    public GraphBenchConfig( Map<String, String> cdl, String system, int multiplier ) {
        super( "graph", system, cdl );

        clusterSeed = Long.parseLong( cdl.get( "clusterSeed" ) );
        seed = Long.parseLong( cdl.get( "seed" ) );

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

        numberOfEdgeMatchQueries = Integer.parseInt( cdl.get( "numberOfEdgeMatchQueries" ) ) * multiplier;
        numberOfPropertyCountQueries = Integer.parseInt( cdl.get( "numberOfPropertyCountQueries" ) ) * multiplier;
        numberOfFindNeighborsQueries = Integer.parseInt( cdl.get( "numberOfFindNeighborsQueries" ) ) * multiplier;
        numberOfUnwindQueries = Integer.parseInt( cdl.get( "numberOfUnwindQueries" ) ) * multiplier;
        numberOfNodeFilterQueries = Integer.parseInt( cdl.get( "numberOfNodeFilterQueries" ) ) * multiplier;
        numberOfDifferentLengthQueries = Integer.parseInt( cdl.get( "numberOfDifferentLengthQueries" ) ) * multiplier;
        numberOfShortestPathQueries = Integer.parseInt( cdl.get( "numberOfShortestPathQueries" ) ) * multiplier;
        numberOfSetPropertyQueries = Integer.parseInt( cdl.get( "numberOfSetPropertyQueries" ) ) * multiplier;
        numberOfDeleteQueries = Integer.parseInt( cdl.get( "numberOfDeleteQueries" ) ) * multiplier;
        numberOfInsertQueries = Integer.parseInt( cdl.get( "numberOfInsertQueries" ) ) * multiplier;

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
        this( cdl, cdl.get( "store" ), 1 );
    }


    // For MultiBench
    protected GraphBenchConfig( String scenario, String system, Map<String, String> cdl ) {
        super( scenario, system, cdl );
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return true;
    }

}
