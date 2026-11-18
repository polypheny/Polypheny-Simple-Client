package org.polypheny.simpleclient.main;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import org.polypheny.simpleclient.scenario.external.BenchmarkRunner;

final class ExternalBenchmarkPhases {
    enum Phase {
        SETUP( "setup" ),
        LOAD( "load" ),
        WARMUP( "warmup" ),
        RUN( "run" );

        final String directory;

        Phase( String directory ) {
            this.directory = directory;
        }
    }

    static List<Phase> forTask( String benchmark, String task, boolean warmup ) {
        return switch ( task ) {
            case "setup", "schema" -> List.of( Phase.SETUP );
            case "data" -> List.of( Phase.LOAD );
            case "warmup" -> {
                if ( !warmup ) {
                    throw new IllegalArgumentException( "Warmup is disabled or unsupported" );
                }
                yield List.of( Phase.WARMUP );
            }
            case "workload" -> warmup ? List.of( Phase.WARMUP, Phase.RUN ) : List.of( Phase.RUN );
            case "all" -> warmup
                    ? List.of( Phase.SETUP, Phase.LOAD, Phase.WARMUP, Phase.RUN )
                    : List.of( Phase.SETUP, Phase.LOAD, Phase.RUN );
            default -> throw new IllegalArgumentException( "Unknown " + benchmark + " task: " + task );
        };
    }

    static Properties execute( String benchmark, List<Phase> phases, Path outputRoot, List<String> targets,
            Properties properties, Function<String, BenchmarkRunner> runnerFactory, String targetMetricPrefix ) throws IOException {
        Path root = outputRoot.toAbsolutePath().normalize();
        Files.createDirectories( root );
        Properties results = new Properties();
        for ( Phase phase : phases ) {
            for ( String target : targets ) {
                Path directory = root.resolve( target ).resolve( phase.directory );
                if ( Files.exists( directory ) ) {
                    throw new IllegalArgumentException( "Output already exists: " + directory );
                }
            }
        }
        for ( Phase phase : phases ) {
            for ( String target : targets ) {
                Path directory = root.resolve( target ).resolve( phase.directory );
                Files.createDirectories( directory.getParent() );
                Files.createDirectory( directory );
                saveConfigSnapshot( properties, directory, benchmark );
                BenchmarkRunner runner = runnerFactory.apply( target );
                var output = directory.toFile();
                System.out.println( "==> " + target + " / " + phase.directory );
                switch ( phase ) {
                    case SETUP -> runner.setup( output );
                    case LOAD -> runner.load( output );
                    case WARMUP -> runner.warmUp( output );
                    case RUN -> {
                        runner.run( output );
                        Properties measurements = new Properties();
                        runner.analyze( measurements, output );
                        for ( String key : measurements.stringPropertyNames() ) {
                            String resultKey = targetMetricPrefix.isEmpty() ? key : targetMetricPrefix + target + "." + key;
                            results.setProperty( resultKey, measurements.getProperty( key ) );
                        }
                    }
                }
                Files.writeString( directory.resolve( "PASS" ), target + " " + phase.directory + "\n" );
                System.out.println( "PASS: " + target + " / " + phase.directory );
            }
        }
        System.out.println( "Results: " + root );
        return results;
    }

    private static void saveConfigSnapshot( Properties properties, Path directory, String benchmark ) throws IOException {
        Properties snapshot = new Properties();
        for ( String key : properties.stringPropertyNames() ) {
            String value = properties.getProperty( key );
            if ( key.contains( "passwd" ) || key.contains( "password" ) || key.endsWith( ".url" ) ) {
                value = "[redacted]";
            }
            snapshot.setProperty( key, value );
        }
        try ( var writer = Files.newBufferedWriter( directory.resolve( "config.properties" ) ) ) {
            snapshot.store( writer, benchmark + " configuration (connection secrets redacted)" );
        }
    }
}
