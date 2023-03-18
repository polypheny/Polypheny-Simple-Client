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

import static org.polypheny.simpleclient.scenario.coms.simulation.Graph.DOC_POSTFIX;
import static org.polypheny.simpleclient.scenario.coms.simulation.Graph.GRAPH_POSTFIX;

import java.util.List;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbCypherExecutor.PolyphenyDbCypherExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbMongoQlExecutor.PolyphenyDbMongoQlExecutorFactory;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.scenario.AbstractConfig;


@Slf4j
public class PolyphenyDbMultiExecutorFactory extends ExecutorFactory {


    private final String host;

    @Getter
    private final PolyphenyDbJdbcExecutorFactory jdbcExecutorFactory;
    @Getter
    private final PolyphenyDbMongoQlExecutorFactory mongoQlExecutorFactory;
    @Getter
    private final PolyphenyDbCypherExecutorFactory cypherExecutorFactory;
    private final String namespace;


    public PolyphenyDbMultiExecutorFactory( String host, String namespace ) {
        this.host = host;
        this.namespace = namespace;
        jdbcExecutorFactory = new PolyphenyDbJdbcExecutorFactory( host, true );
        mongoQlExecutorFactory = new PolyphenyDbMongoQlExecutorFactory( host );
        cypherExecutorFactory = new PolyphenyDbCypherExecutorFactory( host );
    }


    @Override
    public Executor createExecutorInstance( CsvWriter csvWriter ) {

        return new MultiExecutor( jdbcExecutorFactory, mongoQlExecutorFactory, cypherExecutorFactory, csvWriter, namespace );
    }


    @Override
    public int getMaxNumberOfThreads() {
        return 0;
    }


    @Value
    public static class MultiExecutor implements Executor {

        public PolyphenyDbJdbcExecutor jdbc;
        public PolyphenyDbMongoQlExecutor mongo;
        public PolyphenyDbCypherExecutor cypher;


        public MultiExecutor( PolyphenyDbJdbcExecutorFactory jdbcExecutorFactory, PolyphenyDbMongoQlExecutorFactory mongoQlExecutorFactory, PolyphenyDbCypherExecutorFactory cypherExecutorFactory, CsvWriter csvWriter, String namespace ) {
            this.jdbc = jdbcExecutorFactory.createExecutorInstance( csvWriter );
            this.mongo = mongoQlExecutorFactory.createExecutorInstance( csvWriter, namespace + DOC_POSTFIX );
            this.cypher = cypherExecutorFactory.createExecutorInstance( csvWriter, namespace + GRAPH_POSTFIX );
        }


        @Override
        public void reset() throws ExecutorException {
            jdbc.reset();
            mongo.reset();
            cypher.reset();
        }


        @Override
        public long executeQuery( Query query ) throws ExecutorException {
            if ( query.getMongoQl() != null ) {
                return mongo.executeQuery( query );
            } else if ( query.getCypher() != null ) {
                return cypher.executeQuery( query );
            } else {
                return jdbc.executeQuery( query );
            }
        }


        @Override
        public long executeQueryAndGetNumber( Query query ) throws ExecutorException {
            if ( query.getMongoQl() != null ) {
                return mongo.executeQueryAndGetNumber( query );
            } else if ( query.getCypher() != null ) {
                return cypher.executeQueryAndGetNumber( query );
            } else {
                return jdbc.executeQueryAndGetNumber( query );
            }
        }


        @Override
        public void executeCommit() throws ExecutorException {
            jdbc.executeCommit();
            mongo.executeCommit();
            cypher.executeCommit();
        }


        @Override
        public void executeRollback() throws ExecutorException {
            jdbc.executeRollback();
            mongo.executeRollback();
            cypher.executeRollback();
        }


        @Override
        public void closeConnection() throws ExecutorException {
            jdbc.closeConnection();
            mongo.closeConnection();
            cypher.closeConnection();
        }


        @Override
        public void executeInsertList( List<BatchableInsert> batchList, AbstractConfig config ) throws ExecutorException {
            throw new UnsupportedOperationException();
        }


        @Override
        public void flushCsvWriter() {
            jdbc.flushCsvWriter();
            mongo.flushCsvWriter();
            cypher.flushCsvWriter();
        }

    }

}
