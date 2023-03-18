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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import kotlin.Pair;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.query.RawQuery.RawQueryBuilder;
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator.Network;
import org.polypheny.simpleclient.scenario.graph.GraphQuery;

@Slf4j
@Value
public class Graph {

    public static final String DOC_POSTFIX = "_doc";
    public static final String REL_POSTFIX = "_rel";

    public static final String GRAPH_POSTFIX = "_graph";
    Map<Long, Node> nodes;
    Map<Long, Edge> edges;


    public List<Query> getGraphQueries( int graphCreateBatch ) {

        Pair<List<String>, List<String>> nodes = buildNodeQueries( this.nodes, graphCreateBatch );
        Pair<List<String>, List<String>> edges = buildEdgeQueries( this.edges, graphCreateBatch );

        return Streams.zip(
                Stream.concat( nodes.getFirst().stream(), edges.getFirst().stream() ),
                Stream.concat( nodes.getSecond().stream(), edges.getSecond().stream() ), GraphQuery::new ).collect( Collectors.toList() );
    }


    public static Pair<List<String>, List<String>> buildEdgeQueries( Map<Long, Edge> sources, int graphCreateBatch ) {
        List<String> cyphers = new ArrayList<>();
        List<String> surrealDBs = new ArrayList<>();
        StringBuilder cypher;
        StringBuilder surreal;
        for ( List<Edge> edges : Lists.partition( new ArrayList<>( sources.values() ), graphCreateBatch ) ) {
            cypher = new StringBuilder( "CREATE " );
            int i = 0;
            for ( Edge edge : edges ) {
                surreal = new StringBuilder( "RELATE " ); // no batch update possible
                if ( i != 0 ) {
                    cypher.append( ", " );
                }
                //// Cypher CREATE (n1 {})-[]-(n2 {})
                cypher.append( String.format(
                        "(n%s_%s{_id:%s})-[r%d:%s{%s}]->(n%s_%s{_id:%s})",
                        edge.from, edge.from,
                        edge.from,
                        i,
                        String.join( ":", edge.getLabels() ),
                        edge.dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ).collect( Collectors.joining( "," ) ),
                        edge.to,
                        edge.to, edge.to ) );

                //// SurrealDB INSERT INTO dev [{name: 'Amy'}, {name: 'Mary', id: 'Mary'}]; ARE ALWAYS DIRECTED
                surreal.append( String.format(
                        " (SELECT * FROM node WHERE _id = %s)->write->(SELECT * FROM node WHERE _id = %s) CONTENT { labels: [ %s ], %s ",
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


    public static Pair<List<String>, List<String>> buildNodeQueries( Map<Long, Node> sources, int graphCreateBatch ) {
        List<String> cyphers = new ArrayList<>();
        List<String> surrealDBs = new ArrayList<>();
        StringBuilder cypher;
        StringBuilder surreal;
        for ( List<Node> nodes : Lists.partition( new ArrayList<>( sources.values() ), graphCreateBatch ) ) {
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
                        node.getLabels().stream().map( l -> "\"" + l + "\"" ).collect( Collectors.joining( "," ) ),
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
            String collection = nodes.get( 0 ).getLabels().get( 0 ) + DOC_POSTFIX;
            StringBuilder mongo = new StringBuilder( "db." + collection + ".insertMany([" );
            StringBuilder surreal = new StringBuilder( "INSERT INTO " + collection + " [" );
            int i = 0;
            for ( Node node : nodes ) {
                if ( node.nestedQueries.isEmpty() ) {
                    continue;
                }
                if ( i != 0 ) {
                    mongo.append( ", " );
                    surreal.append( ", " );
                }

                mongo.append( node.asMongos().get( 0 ) );
                surreal.append( node.asMongos().get( 0 ) );
                i++;
            }
            if ( i == 0 ) {
                continue;
            }

            queries.add( RawQuery.builder()
                    .mongoQl( mongo.append( "])" ).toString() )
                    .surrealQl( surreal.append( "]" ).toString() ).build() );
        }

        return queries;
    }


    public List<Query> getRelQueries( int relCreateBatch ) {
        List<Query> queries = new ArrayList<>();

        List<GraphElement> list = Stream.concat( this.nodes.values().stream(), this.edges.values().stream() ).collect( Collectors.toList() );
        for ( List<GraphElement> elements : Lists.partition( list, relCreateBatch ) ) {
            StringBuilder sql = new StringBuilder( "INSERT INTO " );
            sql.append( GraphElement.namespace + REL_POSTFIX ).append( "." ).append( elements.get( 0 ).getLabels().get( 0 ) ).append( REL_POSTFIX );
            sql.append( " (" ).append( String.join( ",", elements.get( 0 ).getFixedProperties().keySet() ) ).append( ")" );
            sql.append( " VALUES " );
            int i = 0;
            for ( GraphElement element : elements ) {
                if ( i != 0 ) {
                    sql.append( ", " );
                }
                sql.append( "(" ).append( element.asSql() ).append( ")" );
                i++;
            }
            queries.add( RawQuery.builder()
                    .sql( sql.toString() )
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

        String store = onStore != null ? ".store(" + onStore + ")" : "";

        for ( String collection : getGraphElementStream().flatMap( n -> n.getLabels().stream().map( l -> l + DOC_POSTFIX ) ).collect( Collectors.toSet() ) ) {
            queries.add( RawQuery.builder()
                    .mongoQl( "db.createCollection(" + collection + ")" + store )
                    .surrealQl( "DEFINE TABLE " + collection + " SCHEMALESS" ).build() );
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

        String store = onStore != null ? " ON STORE " + onStore : "";

        Set<String> labels = new HashSet<>();

        for ( GraphElement element : getGraphElementStream().collect( Collectors.toSet() ) ) {
            String label = namespace + "." + element.getLabels().get( 0 );
            if ( labels.contains( label ) ) {
                continue;
            }
            StringBuilder sql = new StringBuilder( "CREATE TABLE " ).append( label ).append( REL_POSTFIX ).append( "(" );
            StringBuilder surreal = new StringBuilder( "DEFINE TABLE " ).append( label ).append( REL_POSTFIX ).append( " SCHEMAFULL;" );

            for ( Entry<String, PropertyType> entry : element.getTypes().entrySet() ) {
                sql.append( entry.getKey() ).append( " " ).append( entry.getValue().asSql() ).append( " NOT NULL," );
                surreal.append( "DEFINE FIELD " ).append( entry.getKey() ).append( " ON " ).append( label ).append( REL_POSTFIX ).append( " TYPE " ).append( entry.getValue().asSurreal() ).append( ";" );
            }

            sql.append( "PRIMARY KEY(" ).append( element.getTypes().keySet().stream().findFirst().orElseThrow( RuntimeException::new ) ).append( "))" )
                    .append( store );

            queries.add( RawQuery.builder().sql( sql.toString() ).surrealQl( surreal.toString() ).build() );

            labels.add( label );
        }

        return queries;
    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    @NonFinal
    public abstract static class Node extends GraphElement {

        public List<JsonObject> nestedQueries;


        public Node(
                Map<String, PropertyType> types,
                Map<String, String> fixedProperties,
                Map<String, String> dynProperties,
                JsonObject nestedQueries ) {
            super( types, fixedProperties, dynProperties );
            this.nestedQueries = new ArrayList<>( Collections.singletonList( nestedQueries ) );
        }


        @Override
        public Query getGraphQuery() {
            StringBuilder surreal = new StringBuilder( "INSERT INTO node [" );

            String cypher = "CREATE " + String.format(
                    "(n%d:%s{%s})",
                    getId(), String.join( ":", getLabels() ),
                    Stream.concat(
                                    dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ),
                                    Stream.of( "_id: " + getId() ) )
                            .collect( Collectors.joining( "," ) ) );

            surreal.append( String.format(
                    "{ labels: [ %s ], %s }",
                    getLabels().stream().map( l -> "\"" + l + "\"" ).collect( Collectors.joining( "," ) ),
                    Stream.concat(
                            dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ),
                            Stream.of( "_id: " + getId() ) ).collect( Collectors.joining( "," ) ) ) );

            return RawQuery.builder().cypher( cypher ).surrealQl( surreal.append( "]" ).toString() ).build();
        }


        public List<Query> addLog( Random random, int nestingDepth ) {
            JsonObject log = Network.generateNestedProperties( random, nestingDepth );

            nestedQueries.add( log );

            String collection = getCollection();

            String docs = String.join( ",", asMongos() );

            String surreal = "INSERT INTO " + collection + " [" + docs + "]";
            String mongo = "db." + collection + ".insertMany([" + docs + "])";

            return Collections.singletonList( RawQuery.builder().mongoQl( mongo ).surrealQl( surreal ).build() );

        }


        public List<String> asMongos() {
            return nestedQueries.stream().map( JsonElement::toString ).collect( Collectors.toList() );
        }


        public List<Query> deleteLogs( Random random ) {
            if ( nestedQueries.size() == 0 ) {
                log.debug( "no logs yet" );
                return Collections.emptyList();
            }
            int i = random.nextInt( nestedQueries.size() );
            String collection = getCollection();

            JsonObject nestedLog = nestedQueries.remove( i );

            JsonObject filter = new JsonObject();

            if ( nestedLog.size() == 0 ) {
                log.debug( "empty log" );
                return Collections.emptyList();
            }

            for ( int j = 0; j < random.nextInt( nestedLog.size() + 1 ); j++ ) {
                String key = new ArrayList<>( nestedLog.keySet() ).get( j );
                filter.add( key, nestedLog.get( key ) );
            }
            if ( filter.size() == 0 ) {
                return Collections.emptyList();
            }

            String surreal = "DELETE " + collection + " WHERE " + filter.entrySet().stream().map( e -> e.getKey() + "=" + e.getValue() ).collect( Collectors.joining( " AND " ) );
            String mongo = "db." + collection + ".remove(" + filter + ")";

            return Collections.singletonList( RawQuery.builder().mongoQl( mongo ).surrealQl( surreal ).build() );

        }


        @NotNull
        private String getCollection() {
            return getLabels().get( 0 ) + DOC_POSTFIX;
        }


        public List<Query> readFullLog() {
            String collection = getCollection();

            String surreal = "SELECT * FROM " + collection;
            String mongo = "db." + collection + ".find({})";

            return Collections.singletonList( RawQuery.builder().mongoQl( mongo ).surrealQl( surreal ).build() );
        }


        public List<Query> readPartialLog( Random random ) {
            int prop = random.nextInt( 10 );

            String collection = getCollection();

            String surreal = "SELECT * FROM " + collection;
            String mongo = "db." + collection + ".find({},{ key" + prop + ":1})";

            return Collections.singletonList( RawQuery.builder().mongoQl( mongo ).surrealQl( surreal ).build() );
        }


        public Query getDocQuery() {
            String collection = getLabels().get( 0 ) + DOC_POSTFIX;
            StringBuilder mongo = new StringBuilder( "db." + collection + ".insertMany([" );
            StringBuilder surreal = new StringBuilder( "INSERT INTO " + collection ).append( " [" );
            mongo.append( String.join( ",", asMongos() ) );
            surreal.append( String.join( ",", asMongos() ) );

            return RawQuery.builder().mongoQl( mongo.append( "])" ).toString() ).surrealQl( surreal.append( "]" ).toString() ).build();
        }

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    @NonFinal
    public abstract static class Edge extends GraphElement {

        public Edge( Map<String, PropertyType> types, Map<String, String> fixedProperties, Map<String, String> dynProperties, long from, long to, boolean directed ) {
            super( types, fixedProperties, dynProperties );
            this.from = from;
            this.to = to;
            this.directed = directed;
        }


        long from;
        long to;
        boolean directed;


        @Override
        public Query getGraphQuery() {
            StringBuilder surreal = new StringBuilder( "RELATE " ); // no batch update possible

            String cypher = "CREATE " + String.format(
                    "(n%s_%s{_id:%s})-[r%d:%s{%s}]->(n%s_%s{_id:%s})",
                    from, from,
                    from,
                    id,
                    String.join( ":", getLabels() ),
                    dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ).collect( Collectors.joining( "," ) ),
                    to,
                    to, to );

            //// SurrealDB INSERT INTO dev [{name: 'Amy'}, {name: 'Mary', id: 'Mary'}]; ARE ALWAYS DIRECTED
            surreal.append( String.format(
                    " (SELECT * FROM node WHERE _id = %s)->write->(SELECT * FROM node WHERE _id = %s) CONTENT { labels: [ %s ], %s ",
                    from,
                    to,
                    getLabels().stream().map( l -> "\"" + l + "\"" ).collect( Collectors.joining( "," ) ),
                    dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ).collect( Collectors.joining( "," ) ) ) );

            surreal.append( "};" );

            return RawQuery.builder().cypher( cypher ).surrealQl( surreal.toString() ).build();
        }


    }

}
