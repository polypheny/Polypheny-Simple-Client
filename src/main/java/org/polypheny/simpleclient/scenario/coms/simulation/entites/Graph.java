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
import com.google.gson.JsonPrimitive;
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
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.simpleclient.cli.Mode;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.query.RawQuery.RawQueryBuilder;
import org.polypheny.simpleclient.scenario.coms.ComsConfig;
import org.polypheny.simpleclient.scenario.coms.QueryTypes;
import org.polypheny.simpleclient.scenario.coms.simulation.GraphElement;
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator.Network;
import org.polypheny.simpleclient.scenario.coms.simulation.PropertyType;
import org.polypheny.simpleclient.scenario.coms.simulation.User;
import org.polypheny.simpleclient.scenario.coms.simulation.User.Login;

@Slf4j
@Value
public class Graph {

    public static final String DOC_POSTFIX = "doc";
    public static final String REL_POSTFIX = "rel";

    public static final String GRAPH_POSTFIX = "graph";
    Map<Long, Node> nodes;
    Map<Long, Edge> edges;

    Map<Long, User> users;


    public List<Query> getGraphQueries( int graphCreateBatch ) {

        Pair<List<String>, List<String>> nodes = buildNodeQueries( this.nodes, graphCreateBatch );
        Pair<List<String>, List<String>> edges = buildEdgeQueries( this.edges, graphCreateBatch );

        return Streams.zip(
                Stream.concat( nodes.getFirst().stream(), edges.getFirst().stream() ),
                Stream.concat( nodes.getSecond().stream(), edges.getSecond().stream() ), ( l, r ) -> RawQuery.builder()
                        .cypher( l )
                        .surrealQl( r )
                        .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.GRAPH ) )
                        .build() ).collect( Collectors.toList() );
    }


    public static Pair<List<String>, List<String>> buildEdgeQueries( Map<Long, Edge> sources, int graphCreateBatch ) {
        List<String> cyphers = new ArrayList<>();
        List<String> surrealDBs = new ArrayList<>();
        StringBuilder cypher;
        StringBuilder surreal;
        for ( List<Edge> edges : Lists.partition( new ArrayList<>( sources.values() ), graphCreateBatch ) ) {
            int i = 0;
            for ( Edge edge : edges ) {
                cypher = new StringBuilder( "MATCH " ).append( String.format( "(n1{i:%s})", edge.from ) )
                        .append( "," ).append( String.format( "(n2{i:%s})", edge.to ) );
                cypher.append( " CREATE " );
                surreal = new StringBuilder( "RELATE " ); // no batch update possible
                //// Cypher CREATE (n1 {})-[]-(n2 {})
                cypher.append( String.format(
                        "(n1)-[r%d:%s{%s}]->(n2)",
                        i,
                        String.join( ":", edge.getLabels() ),
                        edge.asDynamic() ) );

                //// SurrealDB INSERT INTO dev [{name: 'Amy'}, {name: 'Mary', id: 'Mary'}]; ARE ALWAYS DIRECTED
                surreal.append( String.format(
                        " (SELECT * FROM node WHERE i = %s)->edge->(SELECT * FROM node WHERE i = %s) CONTENT { labels: [ %s ], %s ",
                        edge.from,
                        edge.to,
                        edge.getLabels().stream().map( l -> "\"" + l + "\"" ).collect( Collectors.joining( "," ) ),
                        edge.asDynamic() ) );

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
                        node.asDynamic() ) );

                //// SurrealDB INSERT INTO dev [{name: 'Amy'}, {name: 'Mary', id: 'Mary'}];
                surreal.append( String.format(
                        "{ labels: [ %s ], %s }",
                        node.getLabels().stream().map( l -> "\"" + l + "\"" ).collect( Collectors.joining( "," ) ),
                        node.asDynamic() ) );

                i++;
            }
            cyphers.add( cypher.toString() );
            surrealDBs.add( surreal.append( "]" ).toString() );
        }
        return new Pair<>( cyphers, surrealDBs );
    }


    public List<Query> getDocQueries( int docCreateBatch ) {
        List<Query> queries = new ArrayList<>();

        StringBuilder mongo = new StringBuilder( "db." + Network.LOGS_NAME + ".insertMany([" );
        StringBuilder surreal = new StringBuilder( "INSERT INTO " + Network.LOGS_NAME + " [" );
        int i = 0;
        for ( Node node : this.nodes.values() ) {
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
            return Collections.emptyList();
        }

        queries.add( RawQuery.builder()
                .mongoQl( mongo.append( "])" ).toString() )
                .surrealQl( surreal.append( "]" ).toString() )
                .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.DOCUMENT ) )
                .build() );

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
        RawQueryBuilder builder = RawQuery.builder()
                .sql( buildRelInsert( sqlLabel, User.getSql( users, User::userToSql ), User.userTypes ) )
                .surrealQl( buildRelInsert( label, User.getSql( users, User::userToSql ), User.userTypes ) )
                .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.RELATIONAL ) );
        queries.add( builder.build() );

        label = "login" + REL_POSTFIX;
        sqlLabel = "coms" + REL_POSTFIX + "." + label;
        builder = RawQuery.builder()
                .sql( buildRelInsert( sqlLabel, Node.getLoginAsSql( nodes, Mode.POLYPHENY ), User.loginTypes ) )
                .surrealQl( buildRelInsert( label, Node.getLoginAsSql( nodes, Mode.SURREALDB ), User.loginTypes ) )
                .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.RELATIONAL ) );
        queries.add( builder.build() );

        return queries;
    }


    private static String buildRelInsert( String label, List<String> values, Map<String, PropertyType> types ) {

        String sql = " (" + String.join( ", ", types.keySet() ) + ")"
                + " VALUES "
                + values.stream().map( u -> "(" + u + ")" ).collect( Collectors.joining( ", " ) );

        return "INSERT INTO " + label + sql;
    }


    public List<Query> getSchemaGraphQueries( String onStore, String namespace ) {
        List<Query> queries = new ArrayList<>();
        RawQueryBuilder builder = RawQuery.builder();

        if ( onStore != null ) {
            builder.cypher( String.format( "CREATE DATABASE %s IF NOT EXISTS ON STORE %s", namespace, onStore ) );
        } else {
            builder.cypher( String.format( "CREATE DATABASE %s IF NOT EXISTS", namespace ) );
        }

        builder.surrealQl( "DEFINE DATABASE " + namespace + "; DEFINE TABLE node SCHEMALESS;" );

        queries.add( builder.build() );

        return queries;
    }


    public List<Query> getSchemaDocQueries( String onStore, String namespace ) {
        List<Query> queries = new ArrayList<>();
        queries.add( RawQuery.builder()
                .mongoQl( "use " + namespace )
                .surrealQl( "DEFINE DATABASE " + namespace ).build() );

        String store = onStore != null ? ".store(" + onStore + ")" : "";

        queries.add( RawQuery.builder()
                .mongoQl( "db.createCollection(" + Network.LOGS_NAME + ")" + store )
                .surrealQl( "DEFINE TABLE " + Network.LOGS_NAME + " SCHEMALESS" )
                .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.DOCUMENT ) )
                .build() );

        return queries;
    }


    @NotNull
    private Stream<GraphElement> getGraphElementStream() {
        return Stream.concat( nodes.values().stream(), edges.values().stream() );
    }


    public List<Query> getSchemaRelQueries( String onStore, String namespace, Map<Long, User> users, ComsConfig config ) {
        List<Query> queries = new ArrayList<>();
        queries.add( RawQuery.builder()
                .sql( "CREATE SCHEMA " + namespace )
                .surrealQl( "DEFINE DATABASE " + namespace ).build() );

        String store = onStore != null ? " ON STORE " + onStore : "";

        String label = "user";
        String sqlLabel = namespace + "." + label;

        //// BUILD USER TABLE
        queries.add( buildTable( store, label, sqlLabel, User.userTypes, Collections.singletonList( User.userPK ) ) );

        if ( config.createIndexes ) {
            createIndex( queries, label, sqlLabel, User.userPK, true );
        }

        label = "login";
        sqlLabel = namespace + "." + label;

        //// BUILD LOGIN TABLE
        queries.add( buildTable( store, label, sqlLabel, User.loginTypes, User.loginPK ) );

        if ( config.createIndexes ) {
            createIndex( queries, label, sqlLabel, User.loginPK.get( 0 ), false );
        }

        return queries;
    }


    private static void createIndex( List<Query> queries, String label, String sqlLabel, String column, boolean isUnique ) {
        RawQueryBuilder builder;
        builder = RawQuery.builder()
                .sql( String.format( "ALTER TABLE %s ADD %s INDEX idx_%s ON (%s)", sqlLabel + Graph.REL_POSTFIX, isUnique ? " UNIQUE " : "", column, column ) )
                .surrealQl( String.format( "DEFINE INDEX idx_%s ON TABLE %s COLUMNS %s", column, label + Graph.REL_POSTFIX, column ) );
        queries.add( builder.build() );
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

        @Getter
        public List<Login> logins;


        public Node(
                Map<String, String> dynProperties,
                JsonObject nestedQueries,
                Network network,
                boolean isAccessedFrequently ) {
            super( dynProperties );
            nestedQueries.add( "deviceid", new JsonPrimitive( id ) );
            this.nestedQueries = new ArrayList<>( Collections.singletonList( nestedQueries ) );
            this.logins = network.generateAccess( id, network.getUsers(), isAccessedFrequently );
        }


        public static List<String> getLoginAsSql( List<Node> nodes, Mode mode ) {
            List<String> logins = new ArrayList<>();

            for ( Node node : nodes ) {
                for ( Login login : node.logins ) {
                    logins.add( String.join( ", ", login.asStrings( mode ).values() ) );
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
                                    Stream.of( "i: " + getId() ) )
                            .collect( Collectors.joining( "," ) ) );

            surreal.append( String.format(
                    "{ labels: [ %s ], %s }",
                    getLabels().stream().map( l -> "\"" + l + "\"" ).collect( Collectors.joining( "," ) ),
                    Stream.concat(
                            dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ),
                            Stream.of( "i: " + getId() ) ).collect( Collectors.joining( "," ) ) ) );

            return RawQuery.builder()
                    .cypher( cypher )
                    .surrealQl( surreal.append( "]" ).toString() )
                    .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.GRAPH ) )
                    .build();
        }


        public List<Query> addLog( Random random, int nestingDepth ) {
            JsonObject log = Network.generateNestedLogProperties( random, nestingDepth );

            nestedQueries.add( log );

            String collection = getCollection();

            String docs = String.join( ",", asMongos() );

            String surreal = "INSERT INTO " + collection + " [" + docs + "]";
            String mongo = "db." + collection + ".insertMany([" + docs + "]);";

            return Collections.singletonList( RawQuery.builder()
                    .mongoQl( mongo )
                    .surrealQl( surreal )
                    .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.DOCUMENT ) )
                    .build() );

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

            String surreal = "DELETE " + Network.LOGS_NAME + " WHERE " + filter.entrySet().stream().map( e -> e.getKey() + "=" + e.getValue() ).collect( Collectors.joining( " AND " ) );
            String mongo = "db." + Network.LOGS_NAME + ".deleteMany(" + filter + ")";

            return Collections.singletonList( RawQuery.builder()
                    .mongoQl( mongo )
                    .surrealQl( surreal )
                    .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.DOCUMENT ) )
                    .build() );

        }


        @Override
        public List<Query> readFullLog() {
            String collection = getCollection();

            String surreal = "SELECT * FROM " + collection;
            String mongo = "db." + collection + ".find({})";

            return Collections.singletonList( RawQuery.builder()
                    .mongoQl( mongo )
                    .surrealQl( surreal )
                    .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.DOCUMENT ) )
                    .build() );
        }


        @Override
        public List<Query> readPartialLog( Random random ) {
            int prop = random.nextInt( 10 );

            String collection = getCollection();

            String surreal = "SELECT * FROM " + collection;
            String mongo = "db." + collection + ".find({},{ key" + prop + ":1})";

            return Collections.singletonList( RawQuery.builder()
                    .mongoQl( mongo )
                    .surrealQl( surreal )
                    .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.DOCUMENT ) )
                    .build() );
        }


        public Query getDocQuery() {
            StringBuilder mongo = new StringBuilder( "db." + getCollection() + ".insertMany([" );
            StringBuilder surreal = new StringBuilder( "INSERT INTO " + getCollection() ).append( " [" );
            mongo.append( String.join( ",", asMongos() ) );
            surreal.append( String.join( ",", asMongos() ) );

            return RawQuery.builder()
                    .mongoQl( mongo.append( "])" ).toString() )
                    .surrealQl( surreal.append( "]" ).toString() )
                    .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.DOCUMENT ) )
                    .build();
        }


        @Override
        public List<Query> getRemoveQuery() {
            String cypher = String.format( "MATCH (n {i: %s}) DELETE n", getId() );
            String mongo = String.format( "db.%s.deleteMany({deviceid: %s})", getCollection(), getId() );
            String sql = String.format( "DELETE FROM %s WHERE deviceid = %s", getTable( true, Network.LOGINS_NAME ), getId() );

            String surrealGraph = String.format( "DELETE node WHERE i = %s;", getId() );
            String surrealDoc = String.format( "DELETE %s WHERE deviceid = %s;", getCollection(), getId() );
            String surrealRel = String.format( "DELETE FROM %s WHERE deviceid = %s;", getTable( false, Network.LOGINS_NAME ), getId() );

            return Arrays.asList(
                    RawQuery.builder()
                            .cypher( cypher )
                            .surrealQl( surrealGraph )
                            .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.GRAPH ) )
                            .build(),
                    RawQuery.builder()
                            .mongoQl( mongo )
                            .surrealQl( surrealDoc )
                            .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.DOCUMENT ) )
                            .build(),
                    RawQuery.builder()
                            .sql( sql )
                            .surrealQl( surrealRel )
                            .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.RELATIONAL ) )
                            .build()
            );
        }


        @Override
        public List<Query> getReadAllNested() {
            return Collections.singletonList( RawQuery.builder()
                    .surrealQl( "SELECT * FROM " + getCollection() + " WHERE id = " + getId() )
                    .mongoQl( "db." + getCollection() + ".find({ id: " + getId() + "})" )
                    .types( Arrays.asList( QueryTypes.QUERY, QueryTypes.DOCUMENT ) )
                    .build() );
        }


        /**
         * Identify the top 10 most common errors
         *
         * @return
         */
        public List<Query> getComplex1() {
            String mongo = "db.%s.aggregate([\n"
                    + "  { $group: { _id: \"$error.message\", count: { $sum: 1 } } },\n"
                    + "  { $sort: { count: -1 } },\n"
                    + "  { $limit: 10 }\n"
                    + "])";

            String surreal = "SELECT errorMessage, count() AS counted\n"
                    + "FROM %s\n"
                    + "GROUP BY errorMessage\n"
                    + "ORDER BY counted DESC\n"
                    + "LIMIT 10";

            return Collections.singletonList( RawQuery.builder()
                    .surrealQl( String.format( surreal, getCollection() ) )
                    .mongoQl( String.format( mongo, getCollection() ) )
                    .types( Arrays.asList( QueryTypes.COMPLEX_LOGIN_1, QueryTypes.DOCUMENT ) )
                    .build() );
        }


        /**
         * Calculate the percentage of errors caused by each user
         *
         * @return
         */
        public List<Query> getComplex2() {
            String mongo = "db.%s.aggregate([\n"
                    + "  { $match: { error: { message: { $regex: '/memory/i' } } } },\n"
                    + "  { $group: { \"_id\": \"$user.id\", count: { $sum: 1 } } },\n"
                    + "  { $sort: { count: -1 } },\n"
                    + "  { $limit: 10 }\n"
                    + "])\n";

            String surreal = "SELECT user.id AS id, count() AS counted\n"
                    + "FROM " + getCollection() + "\n"
                    + "WHERE error.message CONTAINS 'memory'\n"
                    + "GROUP BY id\n"
                    + "ORDER BY counted DESC\n"
                    + "LIMIT 10;\n";

            return Collections.singletonList( RawQuery.builder()
                    .surrealQl( surreal )
                    .mongoQl( String.format( mongo, getCollection() ) )
                    .types( Arrays.asList( QueryTypes.COMPLEX_LOGIN_2, QueryTypes.DOCUMENT ) )
                    .build() );


        }


        public List<Query> addAccess( User user, Network network ) {
            Login login = Login.createRandomLogin( user.id, id, network );
            logins.add( login );

            String label = "login" + REL_POSTFIX;
            String sqlLabel = "coms" + REL_POSTFIX + "." + label;
            RawQueryBuilder builder = RawQuery.builder()
                    .sql( buildRelInsert( sqlLabel, Node.getLoginAsSql( Collections.singletonList( this ), Mode.POLYPHENY ), User.loginTypes ) )
                    .surrealQl( buildRelInsert( label, Node.getLoginAsSql( Collections.singletonList( this ), Mode.SURREALDB ), User.loginTypes ) )
                    .types( Arrays.asList( QueryTypes.QUERY, QueryTypes.DOCUMENT ) );

            return Collections.singletonList( builder.build() );
        }


        @Override
        public List<Query> countConnectedSimilars() {
            return Collections.singletonList(
                    RawQuery.builder()
                            .cypher( String.format( "MATCH (n:%s) RETURN COUNT(n)", getLabels().get( 0 ) ) )
                            .surrealQl( String.format( "Select count() FROM %s WHERE labels CONTAINS '%s'", "node", getLabels().get( 0 ) ) )
                            .types( Arrays.asList( QueryTypes.GRAPH, QueryTypes.MODIFY ) )
                            .build() );
        }


        @Override
        public List<Query> findNeighborsOfSpecificType( Network network ) {
            int randomType = network.getRandom().nextInt( network.getEdges().size() );
            List<? extends Node> elements = network.getNodes().get( randomType );
            if ( elements.isEmpty() ) {
                return Collections.emptyList();
            }

            return Collections.singletonList(
                    RawQuery.builder()
                            .cypher( String.format( "MATCH (n:%s)-[rel]-() RETURN rel", elements.get( 0 ).getLabels().get( 0 ) ) )
                            .surrealQl( String.format( "Select count() FROM %s WHERE labels CONTAINS '%s'", "node", elements.get( 0 ).getLabels().get( 0 ) ) )
                            .types( Arrays.asList( QueryTypes.GRAPH, QueryTypes.MODIFY ) )
                            .build() );
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
                    " (SELECT * FROM node WHERE id = %s)->edge->(SELECT * FROM node WHERE id = %s) CONTENT { labels: [ %s ], %s ",
                    from,
                    to,
                    getLabels().stream().map( l -> "\"" + l + "\"" ).collect( Collectors.joining( "," ) ),
                    dynProperties.entrySet().stream().map( e -> e.getKey() + ":" + e.getValue() ).collect( Collectors.joining( "," ) ) ) );

            surreal.append( "};" );

            return RawQuery.builder()
                    .cypher( cypher )
                    .surrealQl( surreal.toString() )
                    .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.GRAPH ) )
                    .build();
        }


        @Override
        public List<Query> getRemoveQuery() {
            String cypher = String.format( "MATCH ()-[n {i: %s}]-() DELETE n", getId() );
            String mongo = String.format( "db.%s.deleteMany({id: %s})", Network.LOGS_NAME, getId() );
            String sql = String.format( "DELETE FROM %s WHERE deviceid = %s", getTable( true, Network.LOGINS_NAME ), getId() );

            String surrealGraph = String.format( "DELETE edges WHERE id = %s;", getId() );
            String surrealDoc = String.format( "DELETE %s WHERE id = %s;", Network.LOGS_NAME, getId() );
            String surrealRel = String.format( "DELETE FROM %s WHERE deviceid = %s;", getTable( false, Network.LOGINS_NAME ), getId() );

            return Arrays.asList(
                    RawQuery.builder()
                            .cypher( cypher )
                            .surrealQl( surrealGraph )
                            .types( Arrays.asList( QueryTypes.GRAPH, QueryTypes.MODIFY ) )
                            .build(),
                    RawQuery.builder()
                            .mongoQl( mongo )
                            .surrealQl( surrealDoc )
                            .types( Arrays.asList( QueryTypes.DOCUMENT, QueryTypes.MODIFY ) )
                            .build(),
                    RawQuery.builder()
                            .sql( sql )
                            .surrealQl( surrealRel )
                            .types( Arrays.asList( QueryTypes.RELATIONAL, QueryTypes.MODIFY ) )
                            .build()
            );

        }


        @Override
        public List<Query> countConnectedSimilars() {
            return Collections.singletonList(
                    RawQuery.builder()
                            .cypher( String.format( "MATCH ()-[n:%s]-() RETURN COUNT(n)", getLabels().get( 0 ) ) )
                            .surrealQl( String.format( "SELECT count(in) + count(out) FROM %s WHERE labels CONTAINS '%s'", "edge", getLabels().get( 0 ) ) )
                            .types( Arrays.asList( QueryTypes.GRAPH, QueryTypes.MODIFY ) )
                            .build() );
        }


        @Override
        public List<Query> findNeighborsOfSpecificType( Network network ) {
            int randomType = network.getRandom().nextInt( network.getNodes().size() );
            List<? extends Node> elements = network.getNodes().get( randomType );
            if ( elements.isEmpty() ) {
                return Collections.emptyList();
            }
            return Collections.singletonList(
                    RawQuery.builder()
                            .cypher( String.format( "MATCH (n:%s)-[rel {i:%s}]-() RETURN rel", elements.get( 0 ).getLabels().get( 0 ), getId() ) )
                            .surrealQl( String.format( "SELECT * FROM %s WHERE i = %s AND (in.labels CONTAINS '%s' OR out.labels CONTAINS '%s' )  ", "edge", getId(), elements.get( 0 ).getLabels().get( 0 ), elements.get( 0 ).getLabels().get( 0 ) ) )
                            .types( Arrays.asList( QueryTypes.GRAPH, QueryTypes.MODIFY ) )
                            .build() );
        }

    }

}
