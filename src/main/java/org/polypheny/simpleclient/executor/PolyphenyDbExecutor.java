package org.polypheny.simpleclient.executor;

public interface PolyphenyDbExecutor extends Executor {

    void dropStore( String name ) throws ExecutorException;


    void deployStore( String name, String clazz, String config ) throws ExecutorException;


    default void deployHsqldb() throws ExecutorException {
        deployStore(
                "hsqldb",
                "org.polypheny.db.adapter.jdbc.stores.HsqldbStore",
                "{maxConnections:\"25\",path:.,trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory}" );
    }


    default void deployMonetDb() throws ExecutorException {
        deployStore(
                "monetdb",
                "org.polypheny.db.adapter.jdbc.stores.MonetdbStore",
                "{\"database\":\"test\",\"host\":\"localhost\",\"maxConnections\":\"25\",\"password\":\"monetdb\",\"username\":\"monetdb\",\"port\":\"50000\"}" );
    }


    default void deployPostgres() throws ExecutorException {
        deployStore(
                "postgres",
                "org.polypheny.db.adapter.jdbc.stores.PostgresqlStore",
                "{\"database\":\"test\",\"host\":\"localhost\",\"maxConnections\":\"25\",\"password\":\"postgres\",\"username\":\"postgres\",\"port\":\"5432\"}" );
    }


    default void deployCassandra() throws ExecutorException {
        deployStore(
                "cassandra",
                "org.polypheny.db.adapter.cassandra.CassandraStore",
                "{\"type\":\"Embedded\",\"host\":\"localhost\",\"port\":\"9042\",\"keyspace\":\"cassandra\",\"username\":\"cassandra\",\"password\":\"cass\"}" );
    }


    void setConfig( String key, String value );
}
