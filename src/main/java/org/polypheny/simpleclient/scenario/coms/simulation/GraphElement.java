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

import static org.polypheny.simpleclient.scenario.coms.simulation.entites.Graph.REL_POSTFIX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.jetbrains.annotations.NotNull;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.coms.QueryTypes;
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator.Device;
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator.Network;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.Graph;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.Graph.Node;

@Value
@NonFinal
public abstract class GraphElement {


    public long id = Network.idBuilder.getAndIncrement();

    public static final String namespace = "coms";

    public Map<String, String> dynProperties;


    public List<String> getLabels() {
        return Collections.singletonList( getClass().getSimpleName().toLowerCase() );
    }


    public GraphElement( Map<String, String> dynProperties ) {
        this.dynProperties = dynProperties;
    }


    @NotNull
    public String getCollection() {
        return getLabels().get( 0 ) + Graph.DOC_POSTFIX;
    }


    public List<Query> readFullLog() {
        return Collections.emptyList();
    }


    public List<Query> readPartialLog( Random random ) {
        return Collections.emptyList();
    }


    public abstract List<Query> getRemoveQuery();


    public List<Query> asQuery() {
        List<Query> queries = new ArrayList<>();
        queries.add( getGraphQuery() );
        if ( this instanceof Node ) {
            queries.add( ((Node) this).getDocQuery() );
        }
        return queries;
    }


    public List<Query> getReadAllDynamic() {
        StringBuilder cypher = new StringBuilder( "MATCH " );
        if ( this instanceof Node ) {
            cypher.append( "(n{id:" ).append( getId() ).append( "})" );
        } else {
            cypher.append( "()-[n{id:" ).append( getId() ).append( "}]-()" );
        }
        cypher.append( " RETURN n" );

        return Collections.singletonList( RawQuery.builder()
                .surrealQl( "SELECT * FROM " + getTable( false ) + " WHERE _id =" + getId() )
                .cypher( cypher.toString() )
                .types( Arrays.asList( QueryTypes.QUERY, QueryTypes.GRAPH ) )
                .build() );
    }


    @NotNull
    protected String getTable( boolean withPrefix ) {
        return (withPrefix ? namespace + REL_POSTFIX + "." : "") + "user" + REL_POSTFIX;
    }


    public abstract Query getGraphQuery();


    public String asDynamic() {
        return Stream.concat( new HashMap<String, String>() {{
                    put( "i", String.valueOf( getId() ) );
                }}.entrySet().stream(),
                dynProperties.entrySet().stream() ).map( e -> e.getKey() + ":" + e.getValue() ).collect( Collectors.joining( "," ) );
    }


    public List<Query> getReadAllNested() {
        return Collections.emptyList();
    }


    public abstract List<Device> getPossibleConnectionTypes();


    public abstract List<Query> countConnectedSimilars();


    public abstract List<Query> findNeighborsOfSpecificType( Network network );

}
