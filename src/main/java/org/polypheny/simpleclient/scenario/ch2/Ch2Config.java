package org.polypheny.simpleclient.scenario.ch2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.polypheny.simpleclient.scenario.external.ExternalConfig;

public class Ch2Config extends ExternalConfig {
    public final int warehouses;
    public final int analyticalClients;
    public final int durationSeconds;
    public final double scaleFactor;
    public final Path analyticalQueriesFile;
    public final Map<String, String> environment;

    public Ch2Config( Path path ) throws IOException {
        this( readConfig( path ) );
    }

    public Ch2Config( Properties properties ) {
        super( "ch2", withDefaults( properties ), "transactionClients" );
        warehouses = getInt( "warehouses", "1" );
        analyticalClients = getInt( "analyticalClients", "0" );
        durationSeconds = getInt( "durationSeconds", "10" );
        scaleFactor = Double.parseDouble( properties.getProperty( "scaleFactor", "1" ).trim() );
        int batchSize = getInt( "batchSize", "100" );
        if ( warehouses < 1 || numberOfThreads < 0 || analyticalClients < 0 || numberOfThreads + analyticalClients < 1
                || durationSeconds < 1 || batchSize < 1 || !Double.isFinite( scaleFactor ) || scaleFactor < 1 ) {
            throw new IllegalArgumentException( "Invalid CH2 size, client count, duration, warmup or batch size" );
        }
        String queryFile = properties.getProperty( "analyticalQueriesFile", "" ).trim();
        analyticalQueriesFile = queryFile.isEmpty() ? null : Path.of( queryFile ).toAbsolutePath().normalize();
        if ( analyticalQueriesFile != null && !Files.isRegularFile( analyticalQueriesFile ) ) {
            throw new IllegalArgumentException( "analyticalQueriesFile must be an existing JSON file" );
        }
        String language = properties.getProperty( "analyticalQueryLanguage", analyticalQueriesFile == null ? "sql" : "mongo" ).trim();
        if ( !Set.of( "mongo", "sql" ).contains( language ) ) {
            throw new IllegalArgumentException( "analyticalQueryLanguage must be mongo or sql" );
        }
        if ( analyticalClients > 0 && analyticalQueriesFile == null && !language.equals( "sql" ) ) {
            throw new IllegalArgumentException( "The built-in CH2 queries require analyticalQueryLanguage=sql" );
        }
        environment = new HashMap<>( Map.of(
                "CH2_STORE", properties.getProperty( "setup.store", "" ).trim(),
                "CH2_HOST", properties.getProperty( "host", "polypheny" ),
                "CH2_PORT", properties.getProperty( "port", "20590" ),
                "CH2_NAMESPACE", properties.getProperty( "namespace", "ch2" ),
                "CH2_USER", properties.getProperty( "user", "pa" ),
                "CH2_PASSWORD", properties.getProperty( "password", "" ) ) );
        environment.put( "CH2_CREATE_STORE", properties.getProperty( "setup.createStore", "false" ) );
        environment.put( "CH2_MONGO_HOST", properties.getProperty( "setup.mongoHost", "mongo" ) );
        environment.put( "CH2_MONGO_PORT", properties.getProperty( "setup.mongoPort", "27017" ) );
        environment.put( "CH2_BATCH_SIZE", Integer.toString( batchSize ) );
        environment.put( "CH2_QUERY_LANGUAGE", language );
        environment.put( "CH2_QUERY_NAMESPACE", properties.getProperty( "analyticalNamespace", properties.getProperty( "namespace", "ch2" ) ) );
        environment.put( "CH2_QUERIES", properties.getProperty( "analyticalQueries", "" ) );
        if ( analyticalQueriesFile != null ) {
            environment.put( "CH2_QUERY_FILE", "/queries/" + analyticalQueriesFile.getFileName() );
        }
    }

    private int getInt( String key, String defaultValue ) {
        return Integer.parseInt( properties.getProperty( key, defaultValue ).trim() );
    }

    private static Properties withDefaults( Properties properties ) {
        properties.putIfAbsent( "imageName", "polypheny-ch2:test" );
        properties.putIfAbsent( "network", "benchmark-net" );
        return properties;
    }

    private static Properties readConfig( Path path ) throws IOException {
        Properties properties = read( path );
        resolveQueriesFile( properties, path );
        return properties;
    }

    private static void resolveQueriesFile( Properties properties, Path configFile ) {
        String value = properties.getProperty( "analyticalQueriesFile", "" ).trim();
        if ( !value.isEmpty() ) {
            properties.setProperty( "analyticalQueriesFile", configFile.toAbsolutePath().getParent().resolve( value ).normalize().toString() );
        }
    }

    public Ch2Config( Path base, Map<String, String> overrides, String network, String namespace ) throws IOException {
        this( loadManaged( base, overrides, network, namespace ) );
    }

    private static Properties loadManaged( Path base, Map<String, String> overrides, String network, String namespace ) throws IOException {
        for ( String key : overrides.keySet() ) {
            if ( !isCommonSetting( key ) && !Set.of( "warehouses", "transactionClients", "analyticalClients",
                    "durationSeconds", "scaleFactor", "batchSize", "dataSeed", "analyticalQueriesFile", "analyticalQueries",
                    "analyticalQueryLanguage", "user", "password" ).contains( key ) ) {
                throw new IllegalArgumentException( "Unsupported CH2 job parameter: " + key );
            }
        }
        Properties properties = readManaged( base, overrides, network );
        resolveQueriesFile( properties, base );
        properties.setProperty( "host", "polypheny" );
        properties.setProperty( "port", "20590" );
        properties.setProperty( "namespace", namespace );
        properties.setProperty( "analyticalNamespace", namespace );
        configureMongoStore( properties, "ch2_store" );
        return properties;
    }
}
