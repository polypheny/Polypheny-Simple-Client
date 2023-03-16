/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/11/23, 11:54 AM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.coms.simulation;

import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import jdk.internal.net.http.common.Pair;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.scenario.graph.GraphQuery;

@Value
public class Graph {

    Map<Long, Node> nodes;
    Map<Long, Edge> edges;


    public List<Query> getGraphQueries( int graphCreateBatch ) {

        Pair<List<String>, List<String>> nodes = buildNodeQueries( graphCreateBatch );
        //Pair<List<String>, List<String>> edges = buildN

        return Streams.zip( nodes.first.stream(), nodes.second.stream(), GraphQuery::new ).collect( Collectors.toList() );
    }


    Pair<List<String>, List<String>> buildNodeQueries( int graphCreateBatch ) {
        List<String> cyphers = new ArrayList<>();
        List<String> surrealDBs = new ArrayList<>();
        StringBuilder cypher;
        StringBuilder surreal;
        for ( List<Node> nodes : Lists.partition( new ArrayList<>( this.nodes.values() ), graphCreateBatch ) ) {
            cypher = new StringBuilder( "CREATE " );
            surreal = new StringBuilder( "INSERT INTO node [" );
            int i = 0;
            for ( Node node : nodes ) {
                if ( i != 0 ) {
                    cypher.append( ", " );
                    surreal.append( ", " );
                }
                //// Cypher CREATE (n:[label][:label] {})
                cypher.append( String.format(
                        "(n%d:%s{%s})",
                        i, String.join( ":", node.getLabels() ),
                        node.dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ).collect( Collectors.joining( "," ) ) ) );

                //// SurrealDB INSERT INTO dev [{name: 'Amy'}, {name: 'Mary', id: 'Mary'}];
                surreal.append( String.format(
                        "{ labels: [ %s ], %s }",
                        String.join( ",", node.getLabels() ),
                        node.dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ).collect( Collectors.joining( "," ) ) ) );

                i++;
            }
            cyphers.add( cypher.toString() );
            surrealDBs.add( surreal.append( "]" ).toString() );
        }
        return Pair.pair( cyphers, surrealDBs );
    }


    public List<Query> getDocQueries() {
        List<Query> queries = new ArrayList<>();

        return queries;
    }


    public List<Query> getRelQueries() {
        List<Query> queries = new ArrayList<>();

        return queries;
    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    @NonFinal
    public static class Node extends GraphElement {

        public JsonObject nestedQueries;


        public Node(
                Map<String, PropertyType> types,
                Map<String, String> fixedProperties,
                Map<String, String> dynProperties,
                JsonObject nestedQueries ) {
            super( types, fixedProperties, dynProperties );
            this.nestedQueries = nestedQueries;
        }

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    @NonFinal
    public static class Edge extends GraphElement {

        public Edge( Map<String, PropertyType> types, Map<String, String> fixedProperties, Map<String, String> dynProperties, long from, long to, boolean directed ) {
            super( types, fixedProperties, dynProperties );
            this.from = from;
            this.to = to;
            this.directed = directed;
        }


        long from;
        long to;
        boolean directed;

    }

}
