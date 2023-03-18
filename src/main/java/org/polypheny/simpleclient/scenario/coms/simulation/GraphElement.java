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

    public long id = Network.idBuilder.getAndIncrement();

    public Map<String, String> fixedProperties;

    public Map<String, String> dynProperties;
    public Map<String, PropertyType> types;


    public List<String> getLabels() {
        return Collections.singletonList( getClass().getSimpleName() );
    }


    public GraphElement( Map<String, PropertyType> types, Map<String, String> fixedProperties, Map<String, String> dynProperties ) {
        this.fixedProperties = fixedProperties;
        this.dynProperties = dynProperties;
        this.types = types;
    }


    public String asSql() {
        List<String> query = new ArrayList<>();
        for ( Entry<String, String> entry : fixedProperties.entrySet() ) {
            query.add( entry.getValue() );
        }
        return String.join( ",", query );
    }


    public List<Query> getRemoveQuery() {
        String cypher = String.format( "MATCH (n {_id: %s}) DELETE n", getId() );
        String mongo = String.format( "db.%s.remove({_id: %s})", getLabels().get( 0 ) + DOC_POSTFIX, getId() );
        String sql = String.format( "DELETE %s WHERE _id = %s;", getTable(), getId() );

        String surrealGraph = String.format( "DELETE node WHERE _id = %s;", getId() );
        String surrealDoc = String.format( "DELETE %s WHERE _id = %s;", getLabels().get( 0 ) + DOC_POSTFIX, getId() );
        String surrealRel = String.format( "DELETE %s WHERE _id = %s;", getTable(), getId() );

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
        sql.append( getTable() );
        sql.append( " (" ).append( String.join( ",", getFixedProperties().keySet() ) ).append( ")" );
        sql.append( " VALUES " );
        sql.append( "(" ).append( asSql() ).append( ")" );

        return RawQuery.builder()
                .sql( sql.toString() )
                .surrealQl( sql.toString() ).build();
    }


    @NotNull
    private String getTable() {
        return getLabels().get( 0 ) + REL_POSTFIX;
    }


    public abstract Query getGraphQuery();


    public List<Query> changeDevice( Random random ) {
        int i = random.nextInt( fixedProperties.size() ) + 1;

        StringBuilder sql = new StringBuilder( "UPDATE " ).append( getTable() ).append( " SET " );

        Map<String, String> updates = new HashMap<>();
        for ( int j = 0; j < i; j++ ) {
            String key = new ArrayList<>( fixedProperties.keySet() ).get( j );
            PropertyType property = types.get( key );
            updates.put( key, property.getType().asString( random, 0, Type.OBJECT ) );
        }

        sql.append( updates.entrySet().stream().map( u -> u.getKey() + " = " + u.getValue() ).collect( Collectors.joining( ", " ) ) );
        sql.append( " WHERE " );
        sql.append( updates.keySet().stream().map( u -> u + " = " + fixedProperties.get( u ) ).collect( Collectors.joining( " AND " ) ) );

        // update simulation
        fixedProperties.putAll( updates );

        return Collections.singletonList( RawQuery.builder()
                .sql( sql.toString() )
                .surrealQl( sql.toString() ).build() );
    }

}
