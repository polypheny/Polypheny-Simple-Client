package org.polypheny.simpleclient.scenario.external;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.polypheny.simpleclient.scenario.AbstractConfig;

public abstract class ExternalConfig extends AbstractConfig {
    public final Properties properties;
    public final String imageName;
    public final String network;
    public final String polyphenyImage;
    public final String mongoImage;
    public final Path buildContext;
    public final int warmupTime;
    public final int startupTimeoutSeconds;
    public final int buildTimeoutSeconds;

    protected ExternalConfig( String benchmark, Properties properties, String threadCountKey ) {
        super( benchmark, "polypheny-mongoql", scenarioProperties( properties, threadCountKey ) );
        this.properties = properties;
        imageName = required( "imageName" );
        network = required( "network" );
        polyphenyImage = properties.getProperty( "chronos.polyphenyImage", "polypheny-" + benchmark + "-db:chronos" );
        mongoImage = properties.getProperty( "chronos.mongoImage", "mongo:7.0" );
        buildContext = Path.of( properties.getProperty( "chronos.buildContext", "." ) );
        warmupTime = Integer.parseInt( properties.getProperty( "warmupTime", "0" ).trim() );
        if ( warmupTime < 0 ) {
            throw new IllegalArgumentException( "warmupTime must be non-negative (seconds)" );
        }
        startupTimeoutSeconds = Integer.parseInt( properties.getProperty( "chronos.startupTimeoutSeconds", "180" ) );
        buildTimeoutSeconds = Integer.parseInt( properties.getProperty( "chronos.buildTimeoutSeconds", "1800" ) );
        if ( startupTimeoutSeconds < 1 || buildTimeoutSeconds < 1 ) {
            throw new IllegalArgumentException( "Docker startup and build timeouts must be positive" );
        }
        if ( Long.parseLong( properties.getProperty( "jobTimeoutSeconds", "3600" ) ) < 1 ) {
            throw new IllegalArgumentException( "jobTimeoutSeconds must be positive" );
        }
    }

    public String required( String key ) {
        String value = properties.getProperty( key );
        if ( value == null || value.isBlank() ) {
            throw new IllegalArgumentException( "Missing property: " + key );
        }
        return value.trim();
    }

    protected static Properties read( Path file ) throws IOException {
        Properties properties = new Properties();
        try ( var input = Files.newInputStream( file ) ) {
            properties.load( input );
        }
        Path context = file.toAbsolutePath().getParent().resolve(
                properties.getProperty( "chronos.buildContext", "../.." ) ).normalize();
        properties.setProperty( "chronos.buildContext", context.toString() );
        return properties;
    }

    protected static boolean isCommonSetting( String key ) {
        return Set.of( "imageName", "jobTimeoutSeconds", "polyphenyImage", "mongoImage", "startupTimeoutSeconds", "warmupTime" ).contains( key );
    }

    protected static Properties readManaged( Path file, Map<String, String> overrides, String network ) throws IOException {
        Properties properties = read( file );
        for ( var entry : overrides.entrySet() ) {
            String key = entry.getKey();
            if ( Set.of( "polyphenyImage", "mongoImage", "startupTimeoutSeconds" ).contains( key ) ) {
                key = "chronos." + key;
            }
            properties.setProperty( key, entry.getValue() );
        }
        properties.setProperty( "network", network );
        properties.putIfAbsent( "jobTimeoutSeconds", "3600" );
        return properties;
    }

    protected static void configureMongoStore( Properties properties, String storeName ) {
        properties.setProperty( "setup.store", storeName );
        properties.setProperty( "setup.createStore", "true" );
        properties.setProperty( "setup.mongoHost", "mongo" );
        properties.setProperty( "setup.mongoPort", "27017" );
    }

    private static Properties scenarioProperties( Properties properties, String threadCountKey ) {
        Properties scenario = new Properties();
        scenario.setProperty( "numberOfThreads", properties.getProperty( threadCountKey, "1" ).trim() );
        scenario.setProperty( "numberOfWarmUpIterations", "0" );
        return scenario;
    }

    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return false;
    }
}
