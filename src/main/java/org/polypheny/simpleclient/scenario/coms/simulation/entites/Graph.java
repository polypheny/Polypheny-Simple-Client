/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/28/23, 11:12 AM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.coms.simulation.entites;

import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
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
import org.polypheny.simpleclient.scenario.coms.simulation.GraphElement;
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator.Network;
import org.polypheny.simpleclient.scenario.coms.simulation.PropertyType;
import org.polypheny.simpleclient.scenario.coms.simulation.User;
import org.polypheny.simpleclient.scenario.coms.simulation.User.Login;
import org.polypheny.simpleclient.scenario.graph.GraphQuery;

@Slf4j
@Value
public class Graph {

    public static final String DOC_POSTFIX = "Doc";
    public static final String REL_POSTFIX = "Rel";

    public static final String GRAPH_POSTFIX = "Graph";
    Map<Long, Node> nodes;
    Map<Long, Edge> edges;

    Map<Long, User> users;


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
            int i = 0;
            for ( Edge edge : edges ) {
                cypher = new StringBuilder( "MATCH " ).append( String.format( "(n1{id:%s})", edge.from ) )
                        .append( "," ).append( String.format( "(n2{id:%s})", edge.to ) );
                cypher.append( " CREATE " );
                surreal = new StringBuilder( "RELATE " ); // no batch update possible
                //// Cypher CREATE (n1 {})-[]-(n2 {})
                cypher.append( String.format(
                        "(n1)-[r%d:%s{%s}]->(n2)",
                        i,
                        String.join( ":", edge.getLabels() ),
                        edge.asDyn() ) );

                //// SurrealDB INSERT INTO dev [{name: 'Amy'}, {name: 'Mary', id: 'Mary'}]; ARE ALWAYS DIRECTED
                surreal.append( String.format(
                        " (SELECT * FROM node WHERE id = %s)->write->(SELECT * FROM node WHERE id = %s) CONTENT { labels: [ %s ], %s ",
                        edge.from,
                        edge.to,
                        edge.getLabels().stream().map( l -> "\"" + l + "\"" ).collect( Collectors.joining( "," ) ),
                        edge.asDyn() ) );

                surrealDBs.add( surreal.append( "};" ).toString() );
                cyphers.add( cypher.toString() );
                i++;
            }

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
                        node.asDyn() ) );

                //// SurrealDB INSERT INTO dev [{name: 'Amy'}, {name: 'Mary', id: 'Mary'}];
                surreal.append( String.format(
                        "{ labels: [ %s ], %s }",
                        node.getLabels().stream().map( l -> "\"" + l + "\"" ).collect( Collectors.joining( "," ) ),
                        node.asDyn() ) );

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
            String collection = nodes.get( 0 ).getCollection();
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
        return buildRelInserts( relCreateBatch, new ArrayList<>( users.values() ), new ArrayList<>( nodes.values() ) );
    }


    @NotNull
    public static List<Query> buildRelInserts( int relCreateBatch, List<User> users, List<Node> nodes ) {
        List<Query> queries = new ArrayList<>();

        String label = "user" + REL_POSTFIX;
        String sqlLabel = "coms" + REL_POSTFIX + "." + label;
        //// BUILD USER TABLE INSERTS
        queries.add( buildRelInsert( label, sqlLabel, User.getSql( users, User::userToSql ), User.userTypes ) );

        queries.add( buildRelInsert( label, sqlLabel, Node.getLoginAsSql( nodes ), User.loginTypes ) );

        return queries;
    }


    private static Query buildRelInsert( String label, String sqlLabel, List<String> values, Map<String, PropertyType> types ) {
        StringBuilder sql = new StringBuilder();

        sql.append( " (" ).append( String.join( ", ", types.keySet() ) ).append( ")" );
        sql.append( " VALUES " );

        sql.append( values.stream().map( u -> "(" + u + ")" ).collect( Collectors.joining( ", " ) ) );

        return RawQuery.builder()
                .sql( "INSERT INTO " + sqlLabel + sql )
                .surrealQl( "INSERT INTO " + label + sql ).build();
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


    public List<Query> getSchemaRelQueries( String onStore, String namespace, Map<Long, User> users ) {
        List<Query> queries = new ArrayList<>();
        queries.add( RawQuery.builder()
                .sql( "CREATE SCHEMA " + namespace )
                .surrealQl( "DEFINE DATABASE " + namespace ).build() );

        String store = onStore != null ? " ON STORE " + onStore : "";

        String label = "user";
        String sqlLabel = namespace + "." + label;

        //// BUILD USER TABLE
        queries.add( buildTable( store, label, sqlLabel, User.userTypes, Collections.singletonList( User.userPK ) ) );

        //// BUILD LOGIN TABLE
        queries.add( buildTable( store, label, sqlLabel, User.loginTypes, User.loginPK ) );

        return queries;
    }


    private static RawQuery buildTable( String store, String label, String sqlLabel, Map<String, PropertyType> types, List<String> primaries ) {
        StringBuilder sql = new StringBuilder( "CREATE TABLE " ).append( sqlLabel ).append( REL_POSTFIX ).append( "(" );
        StringBuilder surreal = new StringBuilder( "DEFINE TABLE " ).append( label ).append( REL_POSTFIX ).append( " SCHEMAFULL;" );

        for ( Entry<String, PropertyType> entry : types.entrySet() ) {
            sql.append( entry.getKey() ).append( " " ).append( entry.getValue().asSql() ).append( " NOT NULL," );
            surreal.append( "DEFINE FIELD " ).append( entry.getKey() ).append( " ON " ).append( label ).append( REL_POSTFIX ).append( " TYPE " ).append( entry.getValue().asSurreal() ).append( ";" );
        }

        sql.append( "PRIMARY KEY(" ).append( String.join( ",", primaries ) ).append( "))" ).append( store );

        return RawQuery.builder().sql( sql.toString() ).surrealQl( surreal.toString() ).build();
    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    @NonFinal
    public abstract static class Node extends GraphElement {

        public List<JsonObject> nestedQueries;

        public List<Login> logins;


        public Node(
                Map<String, String> dynProperties,
                JsonObject nestedQueries,
                Network network,
                boolean isAccessedFrequently ) {
            super( dynProperties );
            this.nestedQueries = new ArrayList<>( Collections.singletonList( nestedQueries ) );
            this.logins = network.generateAccess( id, network.getUsers(), isAccessedFrequently );
        }


        public static List<String> getLoginAsSql( List<Node> nodes ) {
            List<String> logins = new ArrayList<>();

            for ( Node node : nodes ) {
                for ( Login login : node.logins ) {
                    logins.add( String.join( ", ", login.asStrings().values() ) );
                }
            }
            return logins;
        }


        @Override
        public Query getGraphQuery() {
            StringBuilder surreal = new StringBuilder( "INSERT INTO node [" );

            String cypher = "CREATE " + String.format(
                    "(n%d:%s{%s})",
                    getId(), String.join( ":", getLabels() ),
                    Stream.concat(
                                    dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ),
                                    Stream.of( "id: " + getId() ) )
                            .collect( Collectors.joining( "," ) ) );

            surreal.append( String.format(
                    "{ labels: [ %s ], %s }",
                    getLabels().stream().map( l -> "\"" + l + "\"" ).collect( Collectors.joining( "," ) ),
                    Stream.concat(
                            dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ),
                            Stream.of( "id: " + getId() ) ).collect( Collectors.joining( "," ) ) ) );

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

            for ( int j = 0; j < random.nextInt( nestedLog.size() ) + 1; j++ ) {
                String key = new ArrayList<>( nestedLog.keySet() ).get( j );
                if ( nestedLog.get( key ).isJsonObject() && nestedLog.get( key ).getAsJsonObject().size() == 0 ) {
                    continue;
                }
                filter.add( key, nestedLog.get( key ) );
            }

            if ( filter.size() == 0 ) {
                return Collections.emptyList();
            }

            String surreal = "DELETE " + collection + " WHERE " + filter.entrySet().stream().map( e -> e.getKey() + "=" + e.getValue() ).collect( Collectors.joining( " AND " ) );
            String mongo = "db." + collection + ".deleteMany(" + filter + ")";

            return Collections.singletonList( RawQuery.builder().mongoQl( mongo ).surrealQl( surreal ).build() );

        }


        @Override
        public List<Query> readFullLog() {
            String collection = getCollection();

            String surreal = "SELECT * FROM " + collection;
            String mongo = "db." + collection + ".find({})";

            return Collections.singletonList( RawQuery.builder().mongoQl( mongo ).surrealQl( surreal ).build() );
        }


        @Override
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


        @Override
        public List<Query> getRemoveQuery() {
            String cypher = String.format( "MATCH (n {id: %s}) DELETE n", getId() );
            String mongo = String.format( "db.%s.deleteMany({id: %s})", getLabels().get( 0 ) + DOC_POSTFIX, getId() );
            String sql = String.format( "DELETE FROM %s WHERE id = %s", getTable( true ), getId() );

            String surrealGraph = String.format( "DELETE node WHERE id = %s;", getId() );
            String surrealDoc = String.format( "DELETE %s WHERE id = %s;", getLabels().get( 0 ) + DOC_POSTFIX, getId() );
            String surrealRel = String.format( "DELETE FROM %s WHERE id = %s;", getTable( false ), getId() );

            return Arrays.asList(
                    RawQuery.builder().cypher( cypher ).surrealQl( surrealGraph ).build(),
                    RawQuery.builder().mongoQl( mongo ).surrealQl( surrealDoc ).build(),
                    RawQuery.builder().sql( sql ).surrealQl( surrealRel ).build()
            );
        }


        @Override
        public List<Query> getReadAllNested() {
            return Collections.singletonList( RawQuery.builder()
                    .surrealQl( "SELECT * FROM " + getCollection() + " WHERE id = " + getId() )
                    .mongoQl( "db." + getCollection() + ".find({ id: " + getId() + "})" ).build() );
        }


        public List<Query> addAccess( User user, Network network ) {
            Login login = Login.createRandomLogin( user.id, id, network );
            logins.add( login );

            String label = "user" + REL_POSTFIX;
            String sqlLabel = "coms" + REL_POSTFIX + "." + label;

            return Collections.singletonList( buildRelInsert( label, sqlLabel, Node.getLoginAsSql( Collections.singletonList( this ) ), User.loginTypes ) );
        }

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    @NonFinal
    public abstract static class Edge extends GraphElement {

        public Edge( Map<String, String> dynProperties, long from, long to, boolean directed ) {
            super( dynProperties );
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
                    "(n%s_%s{id:%s})-[r%d:%s{%s}]->(n%s_%s{id:%s})",
                    from, from,
                    from,
                    id,
                    String.join( ":", getLabels() ),
                    dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ).collect( Collectors.joining( "," ) ),
                    to,
                    to, to );

            //// SurrealDB INSERT INTO dev [{name: 'Amy'}, {name: 'Mary', id: 'Mary'}]; ARE ALWAYS DIRECTED
            surreal.append( String.format(
                    " (SELECT * FROM node WHERE id = %s)->write->(SELECT * FROM node WHERE id = %s) CONTENT { labels: [ %s ], %s ",
                    from,
                    to,
                    getLabels().stream().map( l -> "\"" + l + "\"" ).collect( Collectors.joining( "," ) ),
                    dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ).collect( Collectors.joining( "," ) ) ) );

            surreal.append( "};" );

            return RawQuery.builder().cypher( cypher ).surrealQl( surreal.toString() ).build();
        }


        @Override
        public List<Query> getRemoveQuery() {
            String cypher = String.format( "MATCH ()-[n {id: %s}]-() DELETE n", getId() );
            String mongo = String.format( "db.%s.deleteMany({id: %s})", getLabels().get( 0 ) + DOC_POSTFIX, getId() );
            String sql = String.format( "DELETE FROM %s WHERE id = %s", getTable( true ), getId() );

            String surrealGraph = String.format( "DELETE node WHERE id = %s;", getId() );
            String surrealDoc = String.format( "DELETE %s WHERE id = %s;", getLabels().get( 0 ) + DOC_POSTFIX, getId() );
            String surrealRel = String.format( "DELETE FROM %s WHERE id = %s;", getTable( false ), getId() );

            return Arrays.asList(
                    RawQuery.builder().cypher( cypher ).surrealQl( surrealGraph ).build(),
                    RawQuery.builder().mongoQl( mongo ).surrealQl( surrealDoc ).build(),
                    RawQuery.builder().sql( sql ).surrealQl( surrealRel ).build()
            );

        }

    }

}
