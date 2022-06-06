package org.polypheny.simpleclient.executor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.cli.ChronosCommand;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.oltpbench.AbstractOltpBenchConfig;


@Slf4j
public class OltpBenchPostgresExecutor extends OltpBenchExecutor implements Executor {

    private final Connection connection;
    private final Statement executeStatement;
    private final String host;


    public OltpBenchPostgresExecutor( String host ) {
        super();
        this.host = host;
        try {
            Class.forName( "org.postgresql.Driver" );
        } catch ( ClassNotFoundException e ) {
            throw new RuntimeException( "Driver not found." );
        }

        try {
            connection = DriverManager.getConnection( "jdbc:postgresql://" + host + ":5432/test", "postgres", "postgres" );
            connection.setAutoCommit( false );
            //connection.setTransactionIsolation( Connection.TRANSACTION_SERIALIZABLE );
            executeStatement = connection.createStatement();
            executeStatement.setFetchSize( 100 );
        } catch ( SQLException e ) {
            throw new RuntimeException( "Connection failed.", e );
        }
    }


    @Override
    public void reset() throws ExecutorException {
        executeQuery( new RawQuery( "DROP SCHEMA public CASCADE;", null, false ) );
        executeQuery( new RawQuery( "CREATE SCHEMA public;", null, false ) );
        executeQuery( new RawQuery( "GRANT ALL ON SCHEMA public TO postgres;", null, false ) );
        executeQuery( new RawQuery( "GRANT ALL ON SCHEMA public TO public;", null, false ) );
    }


    @Override
    public long executeQuery( Query query ) throws ExecutorException {
        try {
            Statement statement = connection.createStatement();
            int res = statement.executeUpdate( query.getSql() );
            connection.commit();
            statement.close();
            return res;
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    protected String getConfigXml( AbstractOltpBenchConfig config ) {
        return config.toXml(
                "postgres",
                "org.postgresql.Driver",
                "jdbc:postgresql://" + host + ":5432/test",
                "postgres",
                "postgres",
                config.scenario + "-postgres-ddl.sql",
                "postgres-dialects.xml",
                "TRANSACTION_SERIALIZABLE"
        );
    }


    public static class OltpBenchPostgresExecutorFactory extends OltpBenchExecutorFactory {

        private final String host;


        public OltpBenchPostgresExecutorFactory( String host ) {
            this.host = host;
            reset();
        }


        public void tearDown() {
            reset();
        }


        public static void reset() {
            OltpBenchPostgresExecutor postgresExecutor = new OltpBenchPostgresExecutor( ChronosCommand.hostname );
            try {
                postgresExecutor.reset();
                postgresExecutor.executeCommit();
            } catch ( ExecutorException e ) {
                throw new RuntimeException( "Exception while dropping tables on postgres", e );
            } finally {
                try {
                    postgresExecutor.closeConnection();
                } catch ( ExecutorException e ) {
                    log.error( "Exception while closing connection", e );
                }
            }
        }


        @Override
        public OltpBenchExecutor createExecutorInstance() {
            return new OltpBenchPostgresExecutor( host );
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }

    }

}
