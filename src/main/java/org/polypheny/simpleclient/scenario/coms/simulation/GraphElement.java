/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/13/23, 10:52 AM The Polypheny Project
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

import static org.polypheny.simpleclient.scenario.coms.simulation.Graph.DOC_POSTFIX;
import static org.polypheny.simpleclient.scenario.coms.simulation.Graph.REL_POSTFIX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.jetbrains.annotations.NotNull;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.coms.simulation.Graph.Node;
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator.Network;
import org.polypheny.simpleclient.scenario.coms.simulation.PropertyType.Type;

@Value
@NonFinal
public abstract class GraphElement {

    @Getter
    public static Map<String, PropertyType> types = new HashMap<String, PropertyType>() {{
        put( "a_stamp", new PropertyType( 5, Type.NUMBER ) );
        put( "id", new PropertyType( 3, Type.NUMBER ) );
        put( "manufactureId", new PropertyType( 12, Type.NUMBER ) );
        put( "manufactureName", new PropertyType( 12, Type.CHAR ) );
        put( "entry", new PropertyType( 12, Type.NUMBER ) );
        put( "description", new PropertyType( 255, Type.NUMBER ) );
    }};

    public long id = Network.idBuilder.getAndIncrement();

    public static final String namespace = "coms";

    public List<Map<String, String>> fixedProperties;

    public Map<String, String> dynProperties;


    public List<String> getLabels() {
        return Collections.singletonList( getClass().getSimpleName().toLowerCase() );
    }


    public GraphElement( Map<String, String> fixedProperties, Map<String, String> dynProperties ) {
        this.fixedProperties = new ArrayList<>( Collections.singletonList( fixedProperties ) );
        this.dynProperties = dynProperties;
    }


    public List<String> asSql() {
        List<String> queries = new ArrayList<>();
        for ( Map<String, String> map : fixedProperties ) {
            List<String> query = new ArrayList<>();
            for ( Entry<String, String> entry : map.entrySet() ) {
                query.add( entry.getValue() );
            }
            queries.add( String.join( ",", query ) );
        }

        return queries;
    }


    public List<Query> getRemoveQuery() {
        String cypher = String.format( "MATCH (n {_id: %s}) DELETE n", getId() );
        String mongo = String.format( "db.%s.deleteMany({_id: %s})", getLabels().get( 0 ) + DOC_POSTFIX, getId() );
        String sql = String.format( "DELETE FROM %s WHERE id = %s", getTable( true ), getId() );

        String surrealGraph = String.format( "DELETE node WHERE _id = %s;", getId() );
        String surrealDoc = String.format( "DELETE %s WHERE _id = %s;", getLabels().get( 0 ) + DOC_POSTFIX, getId() );
        String surrealRel = String.format( "DELETE FROM %s WHERE _id = %s;", getTable( false ), getId() );

        return Arrays.asList(
                RawQuery.builder().cypher( cypher ).surrealQl( surrealGraph ).build(),
                RawQuery.builder().mongoQl( mongo ).surrealQl( surrealDoc ).build(),
                RawQuery.builder().sql( sql ).surrealQl( surrealRel ).build()
        );

    }


    public List<Query> asQuery() {
        List<Query> queries = new ArrayList<>();
        queries.add( getGraphQuery() );
        if ( this instanceof Node ) {
            queries.add( ((Node) this).getDocQuery() );
        }
        queries.add( getRelQuery() );
        return queries;
    }


    public Query getRelQuery() {
        StringBuilder sql = new StringBuilder( "INSERT INTO " );
        StringBuilder surreal = new StringBuilder( "INSERT INTO " );
        sql.append( getTable( true ) );
        surreal.append( getTable( false ) );

        sql.append( " (" ).append( String.join( ",", types.keySet() ) ).append( ")" );
        sql.append( " VALUES " );
        String values = asSql().stream().map( s -> "(" + s + ")" ).collect( Collectors.joining( "," ) );
        sql.append( values );

        surreal.append( " (" ).append( String.join( ",", types.keySet() ) ).append( ")" );
        surreal.append( " VALUES " );
        surreal.append( values );

        return RawQuery.builder()
                .sql( sql.toString() )
                .surrealQl( surreal.toString() ).build();
    }


    @NotNull
    private String getTable( boolean withPrefix ) {
        return (withPrefix ? namespace + REL_POSTFIX + "." : "") + getLabels().get( 0 ) + REL_POSTFIX;
    }


    public abstract Query getGraphQuery();


    public List<Query> changeDevice( Random random ) {
        int changedPerProperties = random.nextInt( types.size() ) + 1;
        int targetProperties = random.nextInt( fixedProperties.size() );

        StringBuilder sql = new StringBuilder( " SET " );

        Map<String, String> updates = new HashMap<>();
        for ( int j = 0; j < changedPerProperties; j++ ) {
            String key = new ArrayList<>( types.keySet() ).get( j );
            PropertyType property = types.get( key );
            updates.put( key, property.getType().asString( random, 0, property.getLength(), Type.OBJECT ) );
        }

        sql.append( updates.entrySet().stream().map( u -> u.getKey() + " = " + u.getValue() ).collect( Collectors.joining( ", " ) ) );
        sql.append( " WHERE " );
        sql.append( updates.keySet().stream().map( u -> u + " = " + fixedProperties.get( targetProperties ).get( u ) ).collect( Collectors.joining( " AND " ) ) );

        // update simulation
        fixedProperties.get( targetProperties ).putAll( updates );

        return Collections.singletonList( RawQuery.builder()
                .sql( "UPDATE " + getTable( true ) + sql )
                .surrealQl( "UPDATE " + getTable( false ) + sql ).build() );
    }


    public List<Query> addDeviceAction( Random random ) {
        int newProps = random.nextInt( 5 ) + 1;

        List<Map<String, String>> adds = new ArrayList<>();

        for ( int i = 0; i < newProps; i++ ) {
            adds.add( Network.generateFixedTypedProperties( random, types ) );
        }
        fixedProperties.addAll( adds );

        return buildQueries( adds );
    }


    private List<Query> buildQueries( List<Map<String, String>> adds ) {

        StringBuilder sql = new StringBuilder();

        sql.append( " (" ).append( String.join( ",", types.keySet() ) ).append( ")" );
        sql.append( " VALUES " );
        int i = 0;
        for ( Map<String, String> element : adds ) {
            if ( i != 0 ) {
                sql.append( ", " );
            }
            sql.append( "(" ).append( String.join( ", ", element.values() ) ).append( ")" );
            i++;
        }
        String label = getLabels().get( 0 ) + REL_POSTFIX;
        String sqlLabel = GraphElement.namespace + REL_POSTFIX + "." + label;

        return Collections.singletonList( RawQuery.builder()
                .sql( "INSERT INTO " + sqlLabel + sql )
                .surrealQl( "INSERT INTO " + label + sql ).build() );
    }

}
