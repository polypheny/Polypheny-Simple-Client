package org.polypheny.simpleclient.main;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.polypheny.simpleclient.scenario.ch2.Ch2Config;
import org.polypheny.simpleclient.scenario.ch2.Ch2Runner;


public class Ch2ExternalScenario {

    public static void data() {
        Ch2Config config = new Ch2Config( getProperties() );
        Ch2Runner runner = createRunner( config );

        runner.loadData( new File( "results/ch2" ) );
    }


    public static void workload() {
        Ch2Config config = new Ch2Config( getProperties() );
        Ch2Runner runner = createRunner( config );
        File outputDirectory = new File( "results/ch2" );

        runner.run( outputDirectory );

        Properties results = new Properties();
        runner.analyze( results, outputDirectory );

        System.out.println( "CH2 run log: " + results.getProperty( "ch2.runLog" ) );
    }


    private static Ch2Runner createRunner( Ch2Config config ) {
        return new Ch2Runner(
                config.imageName,
                config.network,
                config.warehouses,
                config.transactionClients,
                config.analyticalClients,
                config.durationSeconds
        );
    }


    private static Properties getProperties() {
        String resource = "/org/polypheny/simpleclient/scenario/external/ch2external.properties";
        Properties properties = new Properties();

        try ( InputStream input = Ch2ExternalScenario.class.getResourceAsStream( resource ) ) {
            if ( input == null ) {
                throw new IllegalStateException( "Properties file not found: " + resource );
            }

            properties.load( input );
            return properties;
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not read properties file: " + resource, e );
        }
    }
}
