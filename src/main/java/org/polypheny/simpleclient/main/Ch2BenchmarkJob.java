package org.polypheny.simpleclient.main;

import ch.unibas.dmi.dbis.chronos.agent.ChronosHttpClient;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.polypheny.simpleclient.main.ExternalBenchmarkPhases.Phase;
import org.polypheny.simpleclient.scenario.ch2.Ch2Config;

final class Ch2BenchmarkJob extends ExternalBenchmarkJob {

    private Ch2Config config;

    Ch2BenchmarkJob( Path configFile, ChronosHttpClient statusClient ) {
        super( "ch2", configFile, statusClient );
    }

    @Override
    protected Ch2Config configure( Map<String, String> settings, String namespace ) throws IOException {
        if ( !"mongodb".equals( settings.remove( "dataStore" ) ) ) {
            throw new IllegalArgumentException( "CH2 currently supports dataStore=mongodb" );
        }
        config = new Ch2Config( configFile, settings, docker.network, namespace );
        return config;
    }

    @Override
    protected void startEnvironment() throws IOException, InterruptedException {
        docker.startMongoBackedPolypheny( config );
        docker.awaitPolypheny( config.imageName, List.of( "python", "./setup.py", "--ready" ),
                config.environment, null, config.startupTimeoutSeconds );
    }

    @Override
    protected boolean includeMeasurement( String key, Properties measurements ) {
        String metric = key.substring( key.lastIndexOf( '.' ) + 1 );
        return switch ( metric ) {
            case "runtimeMicros", "transactions", "transactionsPerSecond",
                    "operations", "averageMicros", "p95Micros", "errors", "aborted" -> true;
            case "analyticalQueries", "queriesPerSecond" -> config.analyticalClients > 0;
            default -> false;
        };
    }

    @Override
    protected Properties runPhases( List<Phase> phases, Path outputDirectory ) throws IOException {
        return Ch2ExternalScenario.execute( config, phases, outputDirectory );
    }
}
