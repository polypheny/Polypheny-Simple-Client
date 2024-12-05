/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/16/23, 4:28 PM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.coms;

import java.util.List;
import java.util.Random;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.coms.Coms.PolyphenyAdapters;
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator.Network;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.Graph;

public class SchemaGenerator {

    private ComsConfig config;
    private Random random;
    private Network network;


    public void generateSchema( ComsConfig config, Executor executor, String namespace, String onStore, PolyphenyAdapters adapters ) throws ExecutorException {
        this.config = config;
        this.random = new Random( config.seed );
        this.network = new Network( random, config );

        // generateSchema network (simulation)
        // -> generateSchema structure of network -> nodes
        // -> generateSchema parameters per nodes
        // -> generateSchema configs depending on type of node
        // -> generateSchema logs?

        Graph graph = this.network.toGraph();

        List<Query> graphQueries = graph.getSchemaGraphQueries( adapters.graphAdapter(), namespace + Graph.GRAPH_POSTFIX );
        List<Query> docQueries = graph.getSchemaDocQueries( adapters.docAdapter(), namespace + Graph.DOC_POSTFIX );
        List<Query> relQueries = graph.getSchemaRelQueries( adapters.relAdapter(), namespace + Graph.REL_POSTFIX, graph.getUsers(), config );

        for ( Query query : graphQueries ) {
            executor.executeQuery( query );
        }

        for ( Query query : docQueries ) {
            executor.executeQuery( query );
        }

        for ( Query query : relQueries ) {
            executor.executeQuery( query );
        }
    }


    public void tearDown( String namespace, PolyphenyDbExecutor executor ) throws ExecutorException {
        executor.executeQuery( RawQuery.builder().cypher( "DROP DATABASE " + namespace + Graph.GRAPH_POSTFIX ).build() );
        executor.executeQuery( RawQuery.builder().mongoQl( "db.dropDatabase()" ).build() );
        executor.executeQuery( RawQuery.builder().sql( "DROP SCHEMA " + namespace + Graph.REL_POSTFIX ).build() );
    }

}
