package org.polypheny.simpleclient.main;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor;
import org.polypheny.simpleclient.scenario.parquet.ParquetConfig;
import org.polypheny.simpleclient.scenario.parquet.ParquetRunner;
import org.polypheny.simpleclient.scenario.parquet.ParquetSetup;


@Slf4j
public class ParquetExternalScenario {

    public static void setup( String host, int port ) {
        Properties properties = getProperties();
        ParquetConfig config = new ParquetConfig( properties );
        PolyphenyDbJdbcExecutor executor = new PolyphenyDbJdbcExecutorFactory( host + ":" + port, false )
                .createExecutorInstance( null );
        try {
            new ParquetSetup( config, executor ).run();
        } catch ( ExecutorException | RuntimeException e ) {
            try {
                executor.executeRollback();
            } catch ( ExecutorException rollbackError ) {
                e.addSuppressed( rollbackError );
            }
            throw new IllegalStateException( "Could not set up Parquet sources", e );
        } finally {
            try {
                executor.closeConnection();
            } catch ( ExecutorException e ) {
                log.warn( "Could not close Parquet setup connection", e );
            }
        }
    }


    public static void workload( String host, int port, String benchmark ) {
        Properties properties = getProperties();

        String queryFile = properties.getProperty( benchmark + ".queryFile" );

        if ( queryFile == null || queryFile.isBlank() ) {
            throw new IllegalArgumentException( "Unknown benchmark: " + benchmark );
        }

        for ( String name : List.of(
                "engine",
                "imageName",
                "accessMode",
                "namespace",
                "dataDirectory",
                "nestedCustomerFile",
                "queryFile" ) ) {

            String value = properties.getProperty( benchmark + "." + name );

            if ( value != null ) {
                properties.setProperty( name, value.trim() );
            }
        }

        File outputDirectory = new File( "results/parquet", benchmark );

        ParquetRunner runner = new ParquetRunner(
                new ParquetConfig( properties ), host, port
        );

        runner.run( outputDirectory );

        Properties results = new Properties();
        runner.analyze( results, outputDirectory );
    }


    private static Properties getProperties() {
        String resource = "/org/polypheny/simpleclient/scenario/external/parquetexternal.properties";

        Properties properties = new Properties();

        try ( InputStream input = ParquetExternalScenario.class.getResourceAsStream( resource ) ) {

            if ( input == null ) {
                throw new IllegalStateException(
                        "Properties file not found: " + resource
                );
            }

            properties.load( input );
            return properties;
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not read properties file: " + resource, e );
        }
    }
}
