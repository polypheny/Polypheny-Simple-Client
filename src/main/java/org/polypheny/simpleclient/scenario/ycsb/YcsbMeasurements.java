package org.polypheny.simpleclient.scenario.ycsb;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Properties;

public final class YcsbMeasurements {

    private YcsbMeasurements() {
    }

    public static Properties read( Path path ) throws IOException {
        Properties metrics = new Properties();
        try ( BufferedReader reader = Files.newBufferedReader( path ) ) {
            String line;
            while ( (line = reader.readLine()) != null ) {
                String[] parts = line.split( ",", 3 );
                if ( parts.length != 3 ) {
                    continue;
                }
                String group = parts[0].trim();
                if ( group.startsWith( "[" ) && group.endsWith( "]" ) ) {
                    metrics.setProperty( group.substring( 1, group.length() - 1 ) + "." + parts[1].trim(), parts[2].trim() );
                }
            }
        }
        if ( !metrics.containsKey( "OVERALL.RunTime(ms)" )
                || !metrics.containsKey( "OVERALL.Throughput(ops/sec)" ) ) {
            throw new IllegalStateException( "Missing YCSB overall measurements: " + path );
        }
        long operations = 0;
        for ( String key : metrics.stringPropertyNames() ) {
            String value = metrics.getProperty( key );
            if ( key.endsWith( ".Operations" ) && !key.equals( "CLEANUP.Operations" ) ) {
                operations += Long.parseLong( value );
            }
            if ( (key.contains( ".Return=" ) && !key.endsWith( ".Return=OK" ))
                    || key.equals( "VERIFY.Failed" ) ) {
                if ( Long.parseLong( value ) != 0 ) {
                    throw new IllegalStateException( "YCSB failure: " + key + "=" + value + ". See " + path );
                }
            }
        }
        if ( operations <= 0 ) {
            throw new IllegalStateException( "YCSB executed no measured operations: " + path );
        }
        return metrics;
    }

    public static void validateCount( Properties metrics, Properties config, String phase ) {
        long actual;
        long expected;
        if ( phase.equals( "load" ) ) {
            actual = count( metrics, "INSERT" );
            expected = Long.parseLong( config.getProperty( "ycsb.recordcount" ) );
        } else {
            actual = count( metrics, "READ" ) + count( metrics, "UPDATE" ) + count( metrics, "INSERT" )
                    + count( metrics, "SCAN" ) + count( metrics, "DELETE" ) - count( metrics, "READ-MODIFY-WRITE" );
            expected = Long.parseLong( config.getProperty( "ycsb.operationcount" ) );

            if ( Long.parseLong( config.getProperty( "ycsb.maxexecutiontime", "0" ) ) > 0 && actual > 0 && actual <= expected ) {
                return;
            }
        }
        if ( actual != expected ) {
            throw new IllegalStateException( "Incomplete YCSB " + phase + ": expected " + expected + ", got " + actual );
        }
    }

    private static long count( Properties metrics, String operation ) {
        return Long.parseLong( metrics.getProperty( operation + ".Operations", "0" ) );
    }

    static void appendSummary( Path path, String target, String phase, Properties metrics ) throws IOException {
        StringBuilder csv = new StringBuilder();
        if ( !Files.exists( path ) ) {
            csv.append( "target,phase,operation,operations,runtime_ms,throughput_ops_sec,average_us,p95_us,p99_us\n" );
        }
        for ( String operation : List.of( "INSERT", "READ", "UPDATE", "SCAN", "DELETE", "READ-MODIFY-WRITE" ) ) {
            if ( !metrics.containsKey( operation + ".Operations" ) ) {
                continue;
            }
            csv.append( target ).append( ',' ).append( phase ).append( ',' ).append( operation );
            for ( String key : List.of( operation + ".Operations", "OVERALL.RunTime(ms)", "OVERALL.Throughput(ops/sec)",
                    operation + ".AverageLatency(us)", operation + ".95thPercentileLatency(us)", operation + ".99thPercentileLatency(us)" ) ) {
                csv.append( ',' ).append( metrics.getProperty( key, "" ) );
            }
            csv.append( '\n' );
        }
        Files.writeString( path, csv, StandardOpenOption.CREATE, StandardOpenOption.APPEND );
    }
}
