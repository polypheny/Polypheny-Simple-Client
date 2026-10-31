package org.polypheny.simpleclient.main;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.polypheny.simpleclient.scenario.ycsb.YcsbConfig;
import org.polypheny.simpleclient.scenario.ycsb.YcsbExternal;
import org.polypheny.simpleclient.scenario.ycsb.YcsbRunner;

public class YcsbExternalScenario {
    public static void execute( Path configuration, String task, Path outputRoot ) throws IOException {
        List<String> phases = switch ( task ) {
            case "setup", "schema" -> List.of( "setup" );
            case "data" -> List.of( "load" );
            case "workload" -> List.of( "run" );
            case "all" -> List.of( "setup", "load", "run" );
            default -> throw new IllegalArgumentException( "Unknown YCSB task: " + task );
        };
        YcsbConfig config = new YcsbConfig( configuration );
        Path root = outputRoot.toAbsolutePath().normalize();
        Files.createDirectories( root );
        for ( String phase : phases ) {
            for ( String target : config.targets ) {
                Path directory = root.resolve( target ).resolve( phase );
                if ( Files.exists( directory ) ) {
                    throw new IllegalArgumentException( "Output already exists: " + directory );
                }
            }
        }
        for ( String phase : phases ) {
            for ( String target : config.targets ) {
                Path directory = root.resolve( target ).resolve( phase );
                Files.createDirectories( directory.getParent() );
                Files.createDirectory( directory );
                saveConfigSnapshot( config.properties, directory );
                YcsbExternal scenario = new YcsbExternal( config, new YcsbRunner( config, target ), directory.getParent().toFile() );
                System.out.println( "==> " + target + " / " + phase );
                switch ( phase ) {
                    case "setup" -> scenario.createSchema( null, true );
                    case "load" -> scenario.generateData( null, null );
                    case "run" -> {
                        scenario.execute( null, null, directory.toFile(), config.numberOfThreads );
                        scenario.analyze( new Properties(), directory.toFile() );
                    }
                    default -> throw new IllegalStateException( "Unknown phase: " + phase );
                }
                Files.writeString( directory.resolve( "PASS" ), target + " " + phase + "\n" );
                System.out.println( "PASS: " + target + " / " + phase );
            }
        }
        System.out.println( "Results: " + root );
    }

    private static void saveConfigSnapshot( Properties properties, Path directory ) throws IOException {
        Properties snapshot = new Properties();
        for ( String key : properties.stringPropertyNames() ) {
            String value = properties.getProperty( key );
            if ( key.contains( "passwd" ) || key.endsWith( ".url" ) ) {
                value = "[redacted]";
            }
            snapshot.setProperty( key, value );
        }
        try ( var writer = Files.newBufferedWriter( directory.resolve( "config.properties" ) ) ) {
            snapshot.store( writer, "YCSB configuration (connection secrets redacted)" );
        }
    }
}
