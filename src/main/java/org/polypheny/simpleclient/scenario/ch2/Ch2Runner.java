package org.polypheny.simpleclient.scenario.ch2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.polypheny.simpleclient.main.DockerLauncher;
import org.polypheny.simpleclient.scenario.external.BenchmarkRunner;
import org.polypheny.simpleclient.scenario.external.ExternalBenchmarkRunner;

public class Ch2Runner extends ExternalBenchmarkRunner implements BenchmarkRunner {
    private final Ch2Config config;

    public Ch2Runner( Ch2Config config ) {
        super( "ch2-polypheny-", config.imageName, config.network );
        this.config = config;
    }

    @Override
    protected Map<String, String> environment() {
        return config.environment;
    }

    @Override
    protected long timeoutSeconds() {
        return Long.parseLong( config.properties.getProperty( "jobTimeoutSeconds", "3600" ) );
    }

    @Override
    protected Map<File, String> buildVolumes( File outputDirectory ) {
        return config.analyticalClients > 0 && config.analyticalQueriesFile != null
                ? Map.of( config.analyticalQueriesFile.getParent().toFile(), "/queries" ) : Map.of();
    }

    @Override
    protected List<String> buildCommand() {
        return benchmarkCommand( false, config.durationSeconds );
    }

    private List<String> benchmarkCommand( boolean load, int duration ) {
        List<String> command = new ArrayList<>( List.of( "python", "./run_ch2.py", "--warehouses=" + config.warehouses,
                "--tclients=" + config.numberOfThreads, "--aclients=" + config.analyticalClients,
                "--duration=" + duration, "--scalefactor=" + config.scaleFactor,
                load ? "--no-execute" : "--no-load", "polypheny", "--stop-on-error" ) );
        if ( config.properties.containsKey( "dataSeed" ) ) {
            command.add( "--datagenSeed=" + config.properties.getProperty( "dataSeed" ) );
        }
        return command;
    }

    @Override
    public void setup( File outputDirectory ) {
        execute( "setup", List.of( "python", "./setup.py" ), outputDirectory );
    }

    @Override
    public void load( File outputDirectory ) throws IOException {
        execute( "load", benchmarkCommand( true, config.durationSeconds ), outputDirectory );
        analyze( new Properties(), outputDirectory, "load" );
    }

    @Override
    public void warmUp( File outputDirectory ) throws IOException {
        if ( config.warmupTime <= 0 ) {
            throw new IllegalArgumentException( "Set warmupTime > 0 to run warmup" );
        }
        execute( "warmup", benchmarkCommand( false, config.warmupTime ), outputDirectory );
        analyze( new Properties(), outputDirectory, "warmup" );
    }

    private void execute( String phase, List<String> command, File outputDirectory ) {
        DockerLauncher.runJob( "ch2-polypheny-" + phase + "-" + UUID.randomUUID(), config.imageName,
                environment(), config.network, command, new File( outputDirectory, phase + ".log" ),
                buildVolumes( outputDirectory ), null, timeoutSeconds() );
    }

    @Override
    protected void prepareRun( File outputDirectory ) {
        // Each phase writes a fresh log; measurements are read only from that log.
    }

    @Override
    public void analyze( Properties results, File outputDirectory ) throws IOException {
        analyze( results, outputDirectory, "run" );
    }

    private void analyze( Properties results, File directory, String phase ) throws IOException {
        Properties metrics = Ch2Measurements.read( directory.toPath().resolve( phase + ".log" ), config, phase );
        Ch2Measurements.write( directory.toPath(), phase, metrics );
        results.putAll( metrics );
    }
}
