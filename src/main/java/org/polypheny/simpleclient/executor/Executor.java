package org.polypheny.simpleclient.executor;

import java.util.List;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.Query;

public interface Executor {

    void reset() throws ExecutorException;


    long executeQuery( Query query ) throws ExecutorException;

    long executeQueryAndGetNumber( Query query ) throws ExecutorException;

    void executeCommit() throws ExecutorException;

    void executeRollback() throws ExecutorException;

    void closeConnection() throws ExecutorException;

    void executeInsertList( List<Query> batchList ) throws ExecutorException;

    void flushCsvWriter();


    abstract class ExecutorFactory {

        public Executor createInstance() {
            return createInstance( null );
        }


        public abstract Executor createInstance( CsvWriter csvWriter );

        // Allows to limit number of concurrent executor threads, 0 means no limit
        public abstract int getMaxNumberOfThreads();
    }

}
