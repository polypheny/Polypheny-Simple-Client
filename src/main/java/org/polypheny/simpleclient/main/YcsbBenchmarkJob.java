package org.polypheny.simpleclient.main;

import ch.unibas.dmi.dbis.chronos.agent.ChronosHttpClient;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.polypheny.simpleclient.main.ExternalBenchmarkPhases.Phase;
import org.polypheny.simpleclient.scenario.ycsb.YcsbConfig;
import org.polypheny.simpleclient.scenario.ycsb.YcsbRunner;

final class YcsbBenchmarkJob extends ExternalBenchmarkJob {

    private YcsbConfig config;

    YcsbBenchmarkJob( Path configFile, ChronosHttpClient statusClient ) {
        super( "ycsb", configFile, statusClient );
    }

    @Override
    protected YcsbConfig configure( Map<String, String> settings, String namespace ) throws IOException {
        if ( !"mongodb".equals( settings.remove( "dataStore" ) ) ) {
            throw new IllegalArgumentException( "YCSB currently supports dataStore=mongodb" );
        }
        config = new YcsbConfig( configFile, settings, docker.network, namespace );
        return config;
    }

    @Override
    protected void startEnvironment() throws IOException, InterruptedException {
        docker.startMongoBackedPolypheny( config );
        Path readinessConfig = Files.createTempFile( "ycsb-readiness-", ".properties" );
        try {
            config.writeSetupProperties( readinessConfig );
            docker.awaitPolypheny( config.imageName,
                    List.of( "timeout", "--kill-after=2s", "10", "java", "-cp", YcsbRunner.CLASSPATH,
                            YcsbRunner.BINDING_PACKAGE + "YcsbSetup", "-", "polypheny-ready" ),
                    Map.of(), readinessConfig.toFile(), config.startupTimeoutSeconds );
        } finally {
            Files.deleteIfExists( readinessConfig );
        }
    }

    @Override
    protected boolean includeMeasurement( String key, Properties measurements ) {
        // Keys have the form ycsb.<target>.<operation>.<metric>.
        String[] parts = key.split( "\\.", 4 );
        String operation = parts[2];
        String metric = parts[3];
        if ( operation.equals( "OVERALL" ) ) {
            return metric.equals( "RunTime(ms)" ) || metric.equals( "Throughput(ops/sec)" );
        }
        if ( operation.equals( "CLEANUP" ) ) {
            return false;
        }
        if ( metric.startsWith( "Return=" ) ) {
            return !metric.equals( "Return=OK" ) && Double.parseDouble( measurements.getProperty( key ) ) > 0;
        }

        String operationCount = "ycsb." + parts[1] + "." + operation + ".Operations";
        return switch ( metric ) {
            case "Operations", "AverageLatency(us)", "95thPercentileLatency(us)" ->
                    Long.parseLong( measurements.getProperty( operationCount, "0" ) ) > 0;
            default -> false;
        };
    }

    @Override
    protected Properties runPhases( List<Phase> phases, Path outputDirectory ) throws IOException {
        return YcsbExternalScenario.execute( config, phases, outputDirectory );
    }
}
