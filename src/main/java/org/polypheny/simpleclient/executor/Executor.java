package org.polypheny.simpleclient.executor;

import org.polypheny.simpleclient.query.Query;

public interface Executor {

    void reset() throws ExecutorException;


    long executeQuery( Query query ) throws ExecutorException;

    long executeQueryAndGetNumber( Query query ) throws ExecutorException;

    void executeCommit() throws ExecutorException;

    void executeRollback() throws ExecutorException;

    void closeConnection() throws ExecutorException;


    abstract class ExecutorFactory {

        public abstract Executor createInstance();

        // Allows to limit number of concurrent executor threads, 0 means no limit
        public abstract int getMaxNumberOfThreads();
    }

}
