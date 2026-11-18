package org.polypheny.simpleclient.main;

import ch.unibas.dmi.dbis.chronos.agent.ChronosHttpClient.JobStatus;
import ch.unibas.dmi.dbis.chronos.agent.ChronosHttpClient;
import ch.unibas.dmi.dbis.chronos.agent.ChronosJob;
import ch.unibas.dmi.dbis.chronos.agent.ExecutionException;
import com.google.gson.JsonParser;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.polypheny.simpleclient.main.ExternalBenchmarkPhases.Phase;
import org.polypheny.simpleclient.scenario.external.ExternalConfig;

abstract class ExternalBenchmarkJob {

    private final String benchmark;
    protected ExternalDockerEnvironment docker;
    private Path archive;
    private ExternalConfig config;
    private Properties measurements;
    private volatile boolean jobFailed;
    private volatile boolean abortRequested;
    private volatile Thread executionThread;
    private final ChronosHttpClient statusClient;
    protected final Path configFile;

    ExternalBenchmarkJob( String benchmark, Path configFile, ChronosHttpClient statusClient ) {
        this.benchmark = benchmark;
        this.configFile = configFile.toAbsolutePath().normalize();
        this.statusClient = statusClient;
    }

    void prepare( File outputDirectory ) {
        try {
            startEnvironment();
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException( "Benchmark startup interrupted", e );
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not start benchmark environment", e );
        }
        runPhase( Phase.SETUP, outputDirectory );
        runPhase( Phase.LOAD, outputDirectory );
    }

    void warmUp( File outputDirectory ) {
        if ( config.warmupTime > 0 ) {
            runPhase( Phase.WARMUP, outputDirectory );
        }
    }

    void execute( File outputDirectory ) {
        measurements = runPhase( Phase.RUN, outputDirectory );
    }

    private Properties runPhase( Phase phase, File outputDirectory ) {
        try {
            return runPhases( List.of( phase ), outputDirectory.toPath() );
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not run phase " + phase.directory, e );
        }
    }

    void runLocal( Path output ) throws IOException {
        Path directory = output.toAbsolutePath().normalize();
        Files.createDirectories( directory );
        try ( var files = Files.list( directory ) ) {
            if ( files.findAny().isPresent() ) {
                throw new IllegalArgumentException( "Managed runs need an empty output directory: " + directory );
            }
        }

        Thread runThread = Thread.currentThread();
        CountDownLatch cleanupDone = new CountDownLatch( 1 );
        Thread shutdownHook = new Thread( () -> {
            runThread.interrupt();
            try {
                cleanupDone.await( 90, TimeUnit.SECONDS );
            } catch ( InterruptedException e ) {
                Thread.currentThread().interrupt();
            }
        }, "local-benchmark-shutdown" );
        Runtime.getRuntime().addShutdownHook( shutdownHook );
        try ( ExternalDockerEnvironment environment = new ExternalDockerEnvironment( benchmark, directory ) ) {
            docker = environment;
            config = configure( new HashMap<>( Map.of( "dataStore", "mongodb" ) ), namespace() );
            startEnvironment();
            runPhases( ExternalBenchmarkPhases.forTask( benchmark, "all", config.warmupTime > 0 ), directory );
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            throw new IOException( "Local benchmark interrupted", e );
        } finally {
            cleanupDone.countDown();
            try {
                Runtime.getRuntime().removeShutdownHook( shutdownHook );
            } catch ( IllegalStateException ignored ) {
                // JVM shutdown is already in progress.
            }
        }
    }

    private String namespace() {
        return benchmark + "_" + docker.id.replace( "-", "_" );
    }

    void analyze( Properties results ) {
        for ( String key : measurements.stringPropertyNames() ) {
            if ( includeMeasurement( key, measurements ) ) {
                results.put( key, Double.parseDouble( measurements.getProperty( key ) ) );
            }
        }
    }

    public boolean hasFailed() {
        return jobFailed;
    }

    @FunctionalInterface
    interface Phases {
        Properties run() throws ExecutionException;
    }

    Properties run( ChronosJob job, File output, Map<String, String> settings, Phases phases ) throws ExecutionException {
        jobFailed = false;
        abortRequested = false;
        executionThread = Thread.currentThread();
        docker = new ExternalDockerEnvironment( benchmark, output.toPath() );
        archive = Path.of( "results", "chronos-" + benchmark, "job-" + job.id + "-" + docker.id ).toAbsolutePath();
        try {
            Properties results;
            try ( ExternalDockerEnvironment resources = docker ) {
                if ( (job.phases & (ChronosJob.EXCLUDE_PREPARE_PHASE | ChronosJob.EXCLUDE_EXECUTE_PHASE
                        | ChronosJob.EXCLUDE_ANALYZE_PHASE)) != 0 ) {
                    throw new IllegalArgumentException( "Managed external benchmarks require prepare, execute and analyze phases" );
                }
                validateBenchmarkSettings( settings );
                config = configure( settings, namespace() );
                results = phases.run();
                archiveTechnicalDurations( results, output.toPath() );
            }
            archiveResults( output );
            return results;
        } catch ( Exception failure ) {
            jobFailed = true;
            abortRequested |= Thread.currentThread().isInterrupted();
            saveFailedRun( output, failure );
            throw new ExecutionException( "External benchmark failed; local results: " + archive, failure );
        } finally {
            docker = null;
            config = null;
            measurements = null;
            executionThread = null;
        }
    }

    private void saveFailedRun( File output, Exception failure ) {
        try ( var writer = new PrintWriter( Files.newBufferedWriter( output.toPath().resolve( "failure.txt" ) ) ) ) {
            failure.printStackTrace( writer );
        } catch ( IOException writeFailure ) {
            failure.addSuppressed( writeFailure );
        }
        // Chronos may delete its temporary output even when the job fails.
        try {
            archiveResults( output );
        } catch ( IOException archiveFailure ) {
            failure.addSuppressed( archiveFailure );
        }
    }

    private void archiveResults( File output ) throws IOException {
        FileUtils.copyDirectory( output, archive.toFile() );
        System.out.println( "Local benchmark results: " + archive );
    }

    private void archiveTechnicalDurations( Properties results, Path output ) throws IOException {
        Properties durations = new Properties();
        for ( String key : List.of( "internal.durations.analyze", "internal.durations.clean" ) ) {
            String duration = results.getProperty( key );
            if ( duration != null ) {
                durations.setProperty( key, duration );
            }
        }
        if ( !durations.isEmpty() ) {
            try ( var writer = Files.newBufferedWriter( output.resolve( "phase-durations.properties" ) ) ) {
                durations.store( writer, "Analysis and cleanup durations (ms)" );
            }
            durations.keySet().forEach( results::remove );
        }
    }

    void copyResults( File output ) throws IOException {
        // Called again after the framework has added result files and the original CDL.
        FileUtils.copyDirectory( output, archive.toFile() );
    }

    void failed( ChronosJob job ) {
        jobFailed = true;
        if ( abortRequested ) {
            // The library catches wrapped phase interruptions as failures. Preserve cancellation status.
            boolean interrupted = Thread.interrupted();
            try {
                statusClient.setStatus( job, JobStatus.ABORTED );
            } finally {
                if ( interrupted ) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    void aborted() {
        jobFailed = true;
        abortRequested = true;
        Thread thread = executionThread;
        if ( thread != null ) {
            thread.interrupt();
        }
    }

    static Map<String, String> readSettings( ChronosJob job ) throws ExecutionException {
        if ( !job.cdl.stripLeading().startsWith( "{" ) ) {
            return new HashMap<>( job.getParsedCdl() );
        }

        Map<String, String> settings = new HashMap<>();
        for ( var entry : JsonParser.parseString( job.cdl ).getAsJsonObject().entrySet() ) {
            if ( !entry.getValue().isJsonPrimitive() ) {
                throw new IllegalArgumentException( "Expected scalar job parameter: " + entry.getKey() );
            }
            settings.put( entry.getKey(), entry.getValue().getAsString() );
        }
        return settings;
    }

    private void validateBenchmarkSettings( Map<String, String> settings ) {
        String scenario = benchmark + "-external";
        if ( !scenario.equals( settings.remove( "scenario" ) ) || !(scenario + "-polypheny").equals( settings.remove( "store" ) ) ) {
            throw new IllegalArgumentException( "Expected scenario=" + scenario + " and store=" + scenario + "-polypheny" );
        }
        settings.remove( "run" );
    }

    protected abstract boolean includeMeasurement( String key, Properties measurements );

    protected abstract ExternalConfig configure( Map<String, String> settings, String namespace ) throws IOException;

    protected abstract void startEnvironment() throws IOException, InterruptedException;

    protected abstract Properties runPhases( List<Phase> phases, Path outputDirectory ) throws IOException;
}
