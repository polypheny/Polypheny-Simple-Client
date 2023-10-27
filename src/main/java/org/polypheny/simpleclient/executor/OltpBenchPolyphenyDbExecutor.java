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

import java.io.File;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.control.client.PolyphenyControlConnector;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.scenario.AbstractConfig;
import org.polypheny.simpleclient.scenario.oltpbench.AbstractOltpBenchConfig;


@Slf4j
public class OltpBenchPolyphenyDbExecutor extends OltpBenchExecutor implements PolyphenyDbExecutor {

    private final PolyphenyDbJdbcExecutorFactory jdbcExecutorFactory;
    private final String host;
    private boolean useNewDeploySyntax = false;


    public OltpBenchPolyphenyDbExecutor( String host ) {
        super();
        this.host = host;
        jdbcExecutorFactory = new PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory( host, false );
    }


    @Override
    public long executeQuery( Query query ) throws ExecutorException {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createExecutorInstance( null );
            long result = executor.executeQuery( query );
            executor.executeCommit();
            return result;
        } catch ( ExecutorException e ) {
            throw new ExecutorException( "Error while executing query via JDBC", e );
        } finally {
            PolyphenyDbJdbcExecutor.commitAndCloseJdbcExecutor( executor );
        }
    }


    @Override
    protected String getConfigXml( AbstractOltpBenchConfig config ) {
        return config.toXml(
                "POLYPHENY",
                "org.polypheny.jdbc.Driver",
                "jdbc:polypheny://" + host + "/",
                "pa",
                "",
                config.scenario + "-polypheny-ddl.sql",
                "polypheny-dialects.xml",
                "TRANSACTION_SERIALIZABLE"
        );
    }


    @Override
    public void dropStore( String name ) throws ExecutorException {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createExecutorInstance( null );
            executor.dropStore( name );
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            throw new ExecutorException( "Error while executing query via JDBC", e );
        } finally {
            PolyphenyDbJdbcExecutor.commitAndCloseJdbcExecutor( executor );
        }
    }


    @Override
    public void deployStore( String name, String clazz, String config ) throws ExecutorException {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createExecutorInstance(null);
            executor.deployStore(name, clazz, config);
            executor.executeCommit();
        } catch (ExecutorException e) {
            throw new ExecutorException("Error while executing query via JDBC", e);
        } finally {
            PolyphenyDbJdbcExecutor.commitAndCloseJdbcExecutor(executor);
        }
    }


    @Override
    public void deployAdapter(String name, String adapterIdentifier, String type, String config) throws ExecutorException {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createExecutorInstance(null);
            executor.deployAdapter(name, adapterIdentifier, type, config);
            executor.executeCommit();
        } catch (ExecutorException e) {
            throw new ExecutorException("Error while executing query via JDBC", e);
        } finally {
            PolyphenyDbJdbcExecutor.commitAndCloseJdbcExecutor(executor);
        }
    }


    @Override
    public void setConfig(String key, String value) {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createExecutorInstance(null);
            executor.setConfig(key, value);
            executor.executeCommit();
        } catch (ExecutorException e) {
            log.error("Exception while setting config \"" + key + "\"!", e);
        } finally {
            try {
                PolyphenyDbJdbcExecutor.commitAndCloseJdbcExecutor(executor);
            } catch (ExecutorException e) {
                log.error("Exception while closing JDBC executor", e);
            }
        }
    }

    @Override
    public void setNewDeploySyntax(boolean useNewDeploySyntax) {
        this.useNewDeploySyntax = useNewDeploySyntax;
    }

    @Override
    public boolean useNewDeploySyntax() {
        return useNewDeploySyntax;
    }


    public static class OltpBenchPolyphenyInstance extends PolyphenyDbInstance {

        public OltpBenchPolyphenyInstance(PolyphenyControlConnector polyphenyControlConnector, ExecutorFactory executorFactory, File outputDirectory, AbstractConfig config) {
            super(polyphenyControlConnector, executorFactory, outputDirectory, config);
        }

    }


    public static class OltpBenchPolyphenyDbExecutorFactory extends OltpBenchExecutorFactory {

        private final String host;


        public OltpBenchPolyphenyDbExecutorFactory( String host ) {
            this.host = host;
        }


        @Override
        public OltpBenchPolyphenyDbExecutor createExecutorInstance() {
            return new OltpBenchPolyphenyDbExecutor( host );
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }

    }

}
