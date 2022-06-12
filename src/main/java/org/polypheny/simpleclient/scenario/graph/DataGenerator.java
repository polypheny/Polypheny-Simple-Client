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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.scenario.graph.queryBuilder.GraphInsert.SimpleGraphInsert;


@Slf4j
public class DataGenerator {

    private final Executor theExecutor;
    private final GraphBenchConfig config;
    private final ProgressReporter progressReporter;

    private final List<BatchableInsert> batchList;

    private boolean aborted;


    DataGenerator( Executor executor, GraphBenchConfig config, ProgressReporter progressReporter ) {
        theExecutor = executor;
        this.config = config;
        this.progressReporter = progressReporter;
        batchList = new LinkedList<>();

        aborted = false;
    }


    private void addToInsertList( BatchableInsert query ) throws ExecutorException {
        batchList.add( query );
        if ( batchList.size() >= config.batchSizeCreates ) {
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


    private String getNode( String identifier, int i, int labelsAmount, int propertiesAmount ) {
        String labels = getLabels( i ).stream().map( l -> ":" + l ).collect( Collectors.joining( "," ) );
        String properties = String.join( ", ", getProperties( i, propertiesAmount ) );

        return String.format( "(%s%s {%s})", identifier == null ? "" : identifier, labels, properties );
    }


    private String getEdge( int i, int propertiesAmount ) {
        String label = getLabels( i ).get( 0 );
        String properties = String.join( ", ", getProperties( i, propertiesAmount ) );

        return String.format( "-[:%s {%s}]->", label, properties );
    }


    private List<String> getLabels( int index ) {
        String label = "Label";
        List<String> labels = new ArrayList<>();
        labels.add( label + index );
        return labels;
    }


    private List<String> getProperties( int index, int amount ) {
        List<String> properties = new ArrayList<>();
        for ( int i = 0; i < amount; i++ ) {
            properties.add( String.format( "key_%s_%s:'value_%s_%s'", index, i, index, i ) );
        }
        List<String> list = Collections.nCopies( config.listSize, "'el'" );
        properties.add( "list:[" + String.join( ",", list ) + "]" );

        return properties;
    }


    private String getPath( int length, int properties ) {
        StringBuilder path = new StringBuilder();
        for ( int i = 0; i < (length + length - 1); i++ ) {
            if ( i % 2 == 0 ) {
                path.append( getNode( null, i, config.usedLabels, properties ) );
            } else {
                path.append( getEdge( i, properties ) );
            }
        }
        return path.toString();

    }


    public void generatePaths( int paths, int minPathLength, int maxPathLength ) throws ExecutorException {
        int diff = maxPathLength - minPathLength;
        for ( int i = 0; i < paths; i++ ) {
            if ( aborted ) {
                break;
            }

            addToInsertList( new SimpleGraphInsert( getPath( minPathLength + (i % diff), config.properties ) ) );
        }
        executeInsertList();
    }


    public void generateClusters( int clusters, int minClusterSize, int maxClusterSize ) throws ExecutorException {
        int diff = maxClusterSize - minClusterSize;

        Random random = new Random( config.seed );

        for ( int i = 0; i < clusters; i++ ) {
            if ( aborted ) {
                break;
            }

            addToInsertList( new SimpleGraphInsert( getCluster( minClusterSize + (i % diff), config.properties, random ) ) );
        }
        executeInsertList();

    }


    private String getCluster( int size, int properties, Random random ) {
        List<String> cluster = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for ( int i = 0; i < size; i++ ) {
            String id = GraphBench.getUniqueIdentifier();
            cluster.add( getNode( id, i, config.usedLabels, properties ) );
            names.add( id );
        }

        assert config.minClusterConnections <= size;
        assert config.maxClusterConnections <= size;
        assert config.minClusterConnections <= config.maxClusterConnections;

        int diff = config.maxClusterConnections - config.minClusterConnections;

        for ( int i = 0; i < size; i++ ) {
            String name = names.get( i );
            List<String> partnerList = new ArrayList<>( names );
            partnerList.remove( name );
            for ( int i1 = 0; i1 < config.minClusterConnections + i % diff; i1++ ) {
                int target = random.nextInt( partnerList.size() );
                cluster.add( String.format( "(%s)", name ) + getEdge( i, properties ) + String.format( "(%s)", partnerList.get( target ) ) );
                partnerList.remove( target );
            }
        }
        return String.join( ", ", cluster );
    }


}
