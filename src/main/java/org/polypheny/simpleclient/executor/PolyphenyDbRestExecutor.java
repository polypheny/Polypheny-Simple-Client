package org.polypheny.simpleclient.executor;

import org.polypheny.simpleclient.query.Query;


public class PolyphenyDbRestExecutor implements PolyphenyDbExecutor {

    public PolyphenyDbRestExecutor( String host ) {
        super();
    }


    @Override
    public void reset() throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    @Override
    public long executeQuery( Query query ) throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    @Override
    public long executeQueryAndGetNumber( Query query ) throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    @Override
    public void executeCommit() throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    @Override
    public void executeRollback() throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    @Override
    public void closeConnection() throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    @Override
    public void dropStore( String name ) throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    @Override
    public void deployStore( String name, String clazz, String config ) throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    @Override
    public void setConfig( String key, String value ) throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    public static class PolyphenyDBRestExecutorFactory extends ExecutorFactory {

        private final String host;


        public PolyphenyDBRestExecutorFactory( String host ) {
            this.host = host;
        }


        @Override
        public Executor createInstance() {
            return new PolyphenyDbRestExecutor( host );
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }
    }
}
