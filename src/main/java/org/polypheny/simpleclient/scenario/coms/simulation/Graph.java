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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import kotlin.Pair;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.jetbrains.annotations.NotNull;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.query.RawQuery.RawQueryBuilder;
import org.polypheny.simpleclient.scenario.graph.GraphQuery;

@Value
public class Graph {

    Map<Long, Node> nodes;
    Map<Long, Edge> edges;


    public List<Query> getGraphQueries( int graphCreateBatch ) {

        Pair<List<String>, List<String>> nodes = buildNodeQueries( graphCreateBatch );
        Pair<List<String>, List<String>> edges = buildEdgeQueries( graphCreateBatch );

        return Streams.zip(
                Stream.concat( nodes.getFirst().stream(), edges.getFirst().stream() ),
                Stream.concat( nodes.getSecond().stream(), edges.getSecond().stream() ), GraphQuery::new ).collect( Collectors.toList() );
    }


    private Pair<List<String>, List<String>> buildEdgeQueries( int graphCreateBatch ) {
        List<String> cyphers = new ArrayList<>();
        List<String> surrealDBs = new ArrayList<>();
        StringBuilder cypher;
        StringBuilder surreal;
        for ( List<Edge> edges : Lists.partition( new ArrayList<>( this.edges.values() ), graphCreateBatch ) ) {
            cypher = new StringBuilder( "CREATE " );
            int i = 0;
            for ( Edge edge : edges ) {
                surreal = new StringBuilder( "RELATE " ); // no batch update possible
                if ( i != 0 ) {
                    cypher.append( ", " );
                }
                //// Cypher CREATE (n:[label][:label] {})
                cypher.append( String.format(
                        "(n%s_%s{_id:%s})-[r%d:%s{%s}]-(n%s_%s{_id:%s})",
                        edge.from, edge.from,
                        edge.from,
                        i,
                        String.join( ":", edge.getLabels() ),
                        edge.dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ).collect( Collectors.joining( "," ) ),
                        edge.to,
                        edge.to, edge.to ) );

                //// SurrealDB INSERT INTO dev [{name: 'Amy'}, {name: 'Mary', id: 'Mary'}]; ARE ALWAYS DIRECTED
                surreal.append( String.format(
                        " (SELECT * FROM node WHERE _id = %s)->write->(SELECT * FROM node WHERE _id = %s) CONTENT{ labels= [ %s ], %s ",
                        edge.from,
                        edge.to,
                        edge.getLabels().stream().map( l -> "\"" + l + "\"" ).collect( Collectors.joining( "," ) ),
                        edge.dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ).collect( Collectors.joining( "," ) ) ) );

                surrealDBs.add( surreal.append( "};" ).toString() );
                i++;
            }
            cyphers.add( cypher.toString() );
        }
        return new Pair<>( cyphers, surrealDBs );

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
                        Stream.concat(
                                        node.dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ),
                                        Stream.of( "_id: " + node.getId() ) )
                                .collect( Collectors.joining( "," ) ) ) );

                //// SurrealDB INSERT INTO dev [{name: 'Amy'}, {name: 'Mary', id: 'Mary'}];
                surreal.append( String.format(
                        "{ labels: [ %s ], %s }",
                        String.join( ",", node.getLabels() ),
                        Stream.concat(
                                node.dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ),
                                Stream.of( "_id: " + node.getId() ) ).collect( Collectors.joining( "," ) ) ) );

                i++;
            }
            cyphers.add( cypher.toString() );
            surrealDBs.add( surreal.append( "]" ).toString() );
        }
        return new Pair<>( cyphers, surrealDBs );
    }


    public List<Query> getDocQueries( int docCreateBatch ) {
        List<Query> queries = new ArrayList<>();

        for ( List<Node> nodes : Lists.partition( new ArrayList<>( this.nodes.values() ), docCreateBatch ) ) {
            StringBuilder mongo = new StringBuilder( "db." + nodes.get( 0 ).getLabels().get( 0 ) + ".insertMany(" );
            StringBuilder surreal = new StringBuilder( "CREATE " );
            for ( Node node : nodes ) {
                mongo.append( "{" );
                mongo.append( node.asMongo() );
                mongo.append( "}" );
                surreal.append( "{" );
                surreal.append( node.asDynSurreal() );
                surreal.append( "}" );
            }
            queries.add( RawQuery.builder()
                    .mongoQl( mongo.append( ")" ).toString() )
                    .surrealQl( surreal.toString() ).build() );
        }

        return queries;
    }


    public List<Query> getRelQueries( int relCreateBatch ) {
        List<Query> queries = new ArrayList<>();

        List<GraphElement> list = Stream.concat( this.nodes.values().stream(), this.edges.values().stream() ).collect( Collectors.toList() );
        for ( List<GraphElement> elements : Lists.partition( list, relCreateBatch ) ) {
            StringBuilder sql = new StringBuilder( "INSERT INTO " );
            sql.append( "(" ).append( String.join( ",", elements.get( 0 ).getFixedProperties().keySet() ) ).append( ")" );
            sql.append( "VALUES" );
            int i = 0;
            for ( GraphElement element : elements ) {
                if ( i != 0 ) {
                    sql.append( " ," );
                }
                sql.append( "(" ).append( element.asSql() ).append( ")" );
                i++;
            }
            queries.add( RawQuery.builder()
                    .sql( sql.append( ")" ).toString() )
                    .surrealQl( sql.toString() ).build() );
        }

        return queries;
    }


    public List<Query> getSchemaGraphQueries( String onStore, String namespace ) {
        RawQueryBuilder builder = RawQuery.builder();

        if ( onStore != null ) {
            builder.cypher( String.format( "CREATE DATABASE %s IF NOT EXISTS ON STORE %s", namespace, onStore ) );
        } else {
            builder.cypher( String.format( "CREATE DATABASE %s IF NOT EXISTS", namespace ) );
        }

        builder.surrealQl( "DEFINE DATABASE " + namespace );

        return Collections.singletonList( builder.build() );
    }


    public List<Query> getSchemaDocQueries( String onStore, String namespace ) {
        List<Query> queries = new ArrayList<>();
        queries.add( RawQuery.builder()
                .mongoQl( "use " + namespace )
                .surrealQl( "DEFINE DATABASE " + namespace ).build() );

        for ( String collection : getGraphElementStream().flatMap( n -> n.getLabels().stream() ).collect( Collectors.toSet() ) ) {
            RawQuery.builder()
                    .mongoQl( "db.createCollection(" + collection + ")" + onStore )
                    .surrealQl( "DEFINE TABLE " + collection ).build();
        }

        return queries;
    }


    @NotNull
    private Stream<GraphElement> getGraphElementStream() {
        return Stream.concat( nodes.values().stream(), edges.values().stream() );
    }


    public List<Query> getSchemaRelQueries( String onStore, String namespace ) {
        List<Query> queries = new ArrayList<>();
        queries.add( RawQuery.builder()
                .sql( "CREATE SCHEMA " + namespace )
                .surrealQl( "DEFINE DATABASE " + namespace ).build() );

        Set<String> labels = new HashSet<>();

        for ( GraphElement element : getGraphElementStream().collect( Collectors.toSet() ) ) {
            if ( labels.contains( element.getLabels().get( 0 ) ) ) {
                continue;
            }
            StringBuilder sql = new StringBuilder( "CREATE TABLE " ).append( element.getLabels().get( 0 ) ).append( "(" );
            StringBuilder surreal = new StringBuilder( "DEFINE TABLE " ).append( element.getLabels().get( 0 ) ).append( " SCHEMAFULL;" );

            int i = 0;
            for ( Entry<String, PropertyType> entry : element.getTypes().entrySet() ) {
                sql.append( entry.getKey() ).append( " " ).append( entry.getValue().asSql() ).append( "," );
                surreal.append( "DEFINE FIELD " ).append( entry.getKey() ).append( " ON TABLE " ).append( entry.getValue().asSurreal() ).append( ";" );
                i++;
            }

            sql.append( "PRIMARY KEY(" ).append( element.getTypes().keySet().stream().findFirst().get() ).append( "));" );

            queries.add( RawQuery.builder().sql( sql.toString() ).surrealQl( surreal.toString() ).build() );

            labels.add( element.getLabels().get( 0 ) );
        }

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
