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

package org.polypheny.simpleclient.executor;

import static org.polypheny.simpleclient.scenario.graph.GraphBench.GRAPH_NAMESPACE;

import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import kong.unirest.core.HttpRequest;
import kong.unirest.core.Unirest;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.MultipartInsert;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.AbstractConfig;


@Slf4j
public class PolyphenyDbCypherExecutor extends PolyphenyDbHttpExecutor {


    public PolyphenyDbCypherExecutor( String host, CsvWriter csvWriter, String namespace ) {
        super( "Cypher", Query::getCypher, host, csvWriter );
        this.namespace = namespace;
    }


    @Override
    protected HttpRequest<?> buildQuery( String query, String namespace ) {
        JsonObject data = new JsonObject();
        data.addProperty( "query", query );
        data.addProperty( "database", namespace );
        return Unirest.post( "{protocol}://{host}:{port}/cypher" )
                .header( "Content-Type", "application/json" )
                .body( data );
    }


    @Override
    public void executeCommit() throws ExecutorException {
        // NoOp
    }


    @Override
    public void executeRollback() throws ExecutorException {
        log.error( "Unsupported operation: Rollback" );
    }


    @Override
    public void closeConnection() throws ExecutorException {
        // NoOp
    }


    @Override
    public void executeInsertList( List<BatchableInsert> batchList, AbstractConfig config ) throws ExecutorException {
        List<String> rows = new ArrayList<>();
        for ( BatchableInsert query : batchList ) {
            query.debug();
            if ( query instanceof MultipartInsert ) {
                continue;
            }

            rows.add( Objects.requireNonNull( query.getCypherRowExpression() ) );
        }
        if ( !rows.isEmpty() ) {
            executeQuery( RawQuery.builder().cypher( Query.buildCypherManyInsert( rows ) ).build() );
        }
    }


    public static class PolyphenyDbCypherExecutorFactory extends ExecutorFactory {

        private final String host;


        public PolyphenyDbCypherExecutorFactory( String host ) {
            this.host = host;
        }


        @Override
        public PolyphenyDbCypherExecutor createExecutorInstance( CsvWriter csvWriter ) {
            return createExecutorInstance( csvWriter, GRAPH_NAMESPACE );
        }


        @Override
        public PolyphenyDbCypherExecutor createExecutorInstance( CsvWriter csvWriter, String namespace ) {
            return new PolyphenyDbCypherExecutor( host, csvWriter, namespace );
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }

    }

}
