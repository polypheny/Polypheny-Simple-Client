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

import static org.polypheny.simpleclient.scenario.coms.simulation.entites.Graph.DOC_POSTFIX;
import static org.polypheny.simpleclient.scenario.coms.simulation.entites.Graph.GRAPH_POSTFIX;

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
    private String namespace = "coms";


    public PolyphenyDbMultiExecutorFactory( String host ) {
        this.host = host;
        jdbcExecutorFactory = new PolyphenyDbJdbcExecutorFactory( host, true );
        mongoQlExecutorFactory = new PolyphenyDbMongoQlExecutorFactory( host );
        cypherExecutorFactory = new PolyphenyDbCypherExecutorFactory( host );
    }


    @Override
    public Executor createExecutorInstance( CsvWriter csvWriter ) {
        return createExecutorInstance( csvWriter, namespace );
    }


    @Override
    public Executor createExecutorInstance( CsvWriter csvWriter, String namespace ) {
        return new MultiExecutor( namespace, jdbcExecutorFactory, mongoQlExecutorFactory, cypherExecutorFactory, csvWriter );
    }


    @Override
    public int getMaxNumberOfThreads() {
        return 0;
    }


    @Value
    public static class MultiExecutor implements PolyphenyDbExecutor {

        public PolyphenyDbJdbcExecutor jdbc;
        public PolyphenyDbMongoQlExecutor mongo;
        public PolyphenyDbCypherExecutor cypher;


        public MultiExecutor( String namespace, PolyphenyDbJdbcExecutorFactory jdbcExecutorFactory, PolyphenyDbMongoQlExecutorFactory mongoQlExecutorFactory, PolyphenyDbCypherExecutorFactory cypherExecutorFactory, CsvWriter csvWriter ) {
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


        public String deployAdapter( String storeType ) throws ExecutorException {
            switch ( storeType.toLowerCase() ) {
                case "mongodb":
                    return jdbc.deployMongoDb();
                case "neo4j":
                    return jdbc.deployNeo4j();
                case "postgres":
                    return jdbc.deployPostgres( true );
                case "cottontail":
                    return jdbc.deployCottontail();
                case "hsqldb":
                    return jdbc.deployHsqldb();
                case "monetdb":
                    return jdbc.deployMonetDb( true );
                case "file":
                    return jdbc.deployFileStore();
                default:
                    throw new RuntimeException( "Unknown store selected for deployment." );
            }
        }


        @Override
        public void dropStore( String name ) throws ExecutorException {
            jdbc.dropStore( name );
        }


        @Override
        public void deployStore( String name, String clazz, String config ) throws ExecutorException {
            jdbc.deployStore( name, clazz, config );
        }


        @Override
        public void deployAdapter( String name, String adapterIdentifier, String type, String config ) throws ExecutorException {
            jdbc.deployAdapter( name, adapterIdentifier, type, config );
        }


        @Override
        public void setConfig( String key, String value ) {
            jdbc.setConfig( key, value );
        }

    }

}
