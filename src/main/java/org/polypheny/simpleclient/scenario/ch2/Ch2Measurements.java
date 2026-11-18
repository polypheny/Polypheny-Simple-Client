package org.polypheny.simpleclient.scenario.ch2;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Properties;

public final class Ch2Measurements {
    private static final String PREFIX = "ch2.polypheny.";
    private static final List<String> TRANSACTIONS = List.of( "NEW_ORDER", "PAYMENT", "DELIVERY", "ORDER_STATUS", "STOCK_LEVEL" );

    private Ch2Measurements() {
    }

    public static Properties read( Path logFile, Ch2Config config, String phase ) throws IOException {
        String summary;
        try ( var lines = Files.lines( logFile ) ) {
            summary = lines.filter( line -> line.startsWith( "[CH2] " ) ).reduce( (first, last) -> last )
                    .orElseThrow( () -> new IllegalStateException( "Missing CH2 measurements in " + logFile ) );
        }
        JsonObject data = JsonParser.parseString( summary.substring( 6 ) ).getAsJsonObject();
        double runtime = data.get( "runtimeMicros" ).getAsDouble();
        if ( !Double.isFinite( runtime ) || runtime <= 0 ) {
            throw new IllegalStateException( "Invalid CH2 runtime: " + logFile );
        }
        if ( !phase.equals( "load" ) && (data.get( "transactionClients" ).getAsInt() != config.numberOfThreads
                || data.get( "analyticalClients" ).getAsInt() != config.analyticalClients) ) {
            throw new IllegalStateException( "Missing CH2 worker results: " + logFile );
        }
        Properties metrics = new Properties();
        metrics.setProperty( PREFIX + "runtimeMicros", Double.toString( runtime ) );
        long transactions = 0;
        long queries = 0;
        long inserts = 0;
        double transactionTime = 0;
        for ( var entry : data.getAsJsonObject( "operations" ).entrySet() ) {
            String operation = entry.getKey();
            if ( !TRANSACTIONS.contains( operation ) && !operation.matches( "Q(0[1-9]|1[0-9]|2[0-2])" ) && !operation.equals( "INSERT" ) ) {
                throw new IllegalStateException( "Unknown CH2 operation: " + operation );
            }
            JsonObject values = entry.getValue().getAsJsonObject();
            long count = values.get( "operations" ).getAsLong();
            if ( count < 0 || values.get( "errors" ).getAsLong() != 0 ) {
                throw new IllegalStateException( "CH2 operation failed: " + operation + "; see " + logFile );
            }
            for ( var value : values.entrySet() ) {
                double number = value.getValue().getAsDouble();
                if ( !Double.isFinite( number ) || number < 0 ) {
                    throw new IllegalStateException( "Invalid CH2 measurement: " + operation + "." + value.getKey() );
                }
                metrics.setProperty( PREFIX + operation + "." + value.getKey(), value.getValue().getAsString() );
            }
            metrics.setProperty( PREFIX + operation + ".throughput", Double.toString( count * 1_000_000.0 / runtime ) );
            if ( TRANSACTIONS.contains( operation ) ) {
                transactions += count;
                transactionTime += values.get( "totalMicros" ).getAsDouble();
            } else if ( operation.equals( "INSERT" ) ) {
                inserts += count;
            } else {
                queries += count;
            }
        }
        if ( phase.equals( "load" ) ) {
            if ( inserts == 0 || !data.has( "loadCounts" ) ) {
                throw new IllegalStateException( "Missing CH2 load validation: " + logFile );
            }
            data.getAsJsonObject( "loadCounts" ).entrySet().forEach( entry ->
                    metrics.setProperty( PREFIX + "loaded." + entry.getKey(), entry.getValue().getAsString() ) );
        } else {
            if ( (config.numberOfThreads > 0 && transactions == 0) || (config.analyticalClients > 0 && queries == 0) ) {
                throw new IllegalStateException( "CH2 completed no transactions or analytical queries: " + logFile );
            }
            metrics.setProperty( PREFIX + "transactions", Long.toString( transactions ) );
            metrics.setProperty( PREFIX + "transactionTimeMicros", Double.toString( transactionTime ) );
            metrics.setProperty( PREFIX + "transactionsPerSecond", Double.toString( transactions * 1_000_000.0 / runtime ) );
            metrics.setProperty( PREFIX + "analyticalQueries", Long.toString( queries ) );
            metrics.setProperty( PREFIX + "queriesPerSecond", Double.toString( queries * 1_000_000.0 / runtime ) );
        }
        return metrics;
    }

    public static void write( Path directory, String phase, Properties metrics ) throws IOException {
        try ( var writer = Files.newBufferedWriter( directory.resolve( "measurements.properties" ) ) ) {
            metrics.store( writer, "CH2 measurements" );
        }
        Path summary = directory.getParent().getParent().resolve( "summary.csv" );
        StringBuilder csv = new StringBuilder();
        if ( !Files.exists( summary ) ) {
            csv.append( "target,phase,operation,operations,runtime_ms,throughput_ops_sec,average_us,p95_us,p99_us,aborted,errors\n" );
        }
        for ( String key : metrics.stringPropertyNames().stream().sorted().toList() ) {
            if ( !key.endsWith( ".operations" ) ) {
                continue;
            }
            String operation = key.substring( PREFIX.length(), key.length() - ".operations".length() );
            String prefix = PREFIX + operation + ".";
            csv.append( "polypheny," ).append( phase ).append( ',' ).append( operation ).append( ',' )
                    .append( metrics.getProperty( key ) ).append( ',' )
                    .append( Double.parseDouble( metrics.getProperty( PREFIX + "runtimeMicros" ) ) / 1000 );
            for ( String field : List.of( "throughput", "averageMicros", "p95Micros", "p99Micros", "aborted", "errors" ) ) {
                csv.append( ',' ).append( metrics.getProperty( prefix + field, "" ) );
            }
            csv.append( '\n' );
        }
        Files.writeString( summary, csv, StandardOpenOption.CREATE, StandardOpenOption.APPEND );
    }
}
