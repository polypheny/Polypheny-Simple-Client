package org.polypheny.simpleclient.scenario.parquet;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.polypheny.simpleclient.scenario.external.ExternalBenchmarkRunner;

public class ParquetRunner extends ExternalBenchmarkRunner {

    private final ParquetConfig config;
    private final String host;
    private final int port;


    public ParquetRunner( ParquetConfig config, String host, int port ) {
        super( "parquet", config.imageName, config.network );
        this.config = config;
        this.host = host;
        this.port = port;
    }

    @Override
    protected void prepareRun( File outputDirectory ) {
        String resultName = getResultName();

        File csvFile = new File( outputDirectory, resultName + ".csv" );
        try {
            Files.deleteIfExists( csvFile.toPath() );
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not clear previous results: " + csvFile, e );
        }
    }
    @Override
    public void analyze( Properties results, File outputDirectory ) {
        File csvFile = new File( outputDirectory, getResultName() + ".csv" );
        Map<String, List<CSVRecord>> runsByQuery = readMeasuredRuns( csvFile );

        for ( String queryId : runsByQuery.keySet() ) {
            List<CSVRecord> runs = runsByQuery.get( queryId );

            List<Double> times = new ArrayList<>();
            Set<String> errors = new LinkedHashSet<>();
            Set<String> rowCounts = new LinkedHashSet<>();
            Set<String> columnCounts = new LinkedHashSet<>();

            int failed = 0;
            double totalTime = 0;

            // Collect successful measurements and failed runs separately.
            for ( CSVRecord run : runs ) {
                if ( "true".equals( run.get( "success" ) ) ) {
                    double time = Double.parseDouble( run.get( "elapsed_ms" ) );

                    times.add( time );
                    totalTime += time;

                    rowCounts.add( run.get( "rows" ) );
                    columnCounts.add( run.get( "columns" ) );
                } else {
                    failed++;

                    String error = run.get( "error" );
                    if ( !error.isBlank() ) {
                        errors.add( error );
                    }
                }
            }

            int successful = times.size();

            results.setProperty( queryId + ".successfulRuns", String.valueOf( successful ) );
            results.setProperty( queryId + ".failedRuns", String.valueOf( failed ) );
            results.setProperty( queryId + ".errors", String.join( " | ", errors ) );
            results.setProperty( queryId + ".rowCounts", rowCounts.toString() );
            results.setProperty( queryId + ".columnCounts", columnCounts.toString() );

            // No successful measurements means there are no runtime statistics.
            if ( successful == 0 ) {
                continue;
            }

            Collections.sort( times );

            double mean = totalTime / successful;
            double min = times.getFirst();
            double max = times.get( successful - 1 );

            // With an even number of measurements, average the two middle values.
            int middle = successful / 2;
            double median;

            if ( successful % 2 == 0 ) {
                median = (times.get( middle - 1 ) + times.get( middle )) / 2.0;
            } else {
                median = times.get( middle );
            }

            results.setProperty( queryId + ".meanMs", String.valueOf( mean ) );
            results.setProperty( queryId + ".minMs", String.valueOf( min ) );
            results.setProperty( queryId + ".maxMs", String.valueOf( max ) );
            results.setProperty( queryId + ".medianMs", String.valueOf( median ) );

            // Sample standard deviation requires at least two measurements.
            if ( successful >= 2 ) {
                double squaredDifferences = 0;

                for ( double time : times ) {
                    double difference = time - mean;
                    squaredDifferences += difference * difference;
                }

                double variance = squaredDifferences / (successful - 1);
                double standardDeviation = Math.sqrt( variance );

                results.setProperty( queryId + ".stddevMs", String.valueOf( standardDeviation ) );
            }
        }
    }

    @Override
    protected List<String> buildCommand() {
        List<String> command = new ArrayList<>();

        if ( "polypheny".equals( config.engine ) ) {
            addPolyphenyArguments( command );

        } else if ( "duckdb".equals( config.engine ) ) {
            addArgument( command, "data-dir", "/data" );
            addArgument( command, "threads", config.duckdbThreads );
            addArgument( command, "memory-limit", config.duckdbMemoryLimit );

        } else if ( "spark".equals( config.engine ) ) {
            addArgument( command, "data-dir", "/data" );
            addArgument( command, "shuffle-partitions", config.sparkShufflePartitions );

        } else {
            throw new IllegalArgumentException( "Unsupported engine: " + config.engine );
        }

        if ( ("duckdb".equals( config.engine ) || "spark".equals( config.engine ))
                && !config.nestedCustomerFile.isBlank() ) {
            addArgument( command, "nested-customer-file", config.nestedCustomerFile );
        }

        addArgument( command, "queries", config.queryFile );
        addArgument( command, "warmups", config.warmups );
        addArgument( command, "runs", config.runs );
        addArgument( command, "output", "/results/" + getResultName() + ".csv" );

        return command;
    }

    @Override
    protected Map<File, String> buildVolumes( File outputDirectory ) {
        Map<File, String> volumes = new HashMap<>();
        volumes.put( outputDirectory, "/results" );

        if ( "duckdb".equals( config.engine ) || "spark".equals( config.engine ) ) {
            File dataDirectory = new File( config.dataDirectory );

            if ( !dataDirectory.isDirectory() ) {
                throw new IllegalArgumentException( "Data directory does not exist: " + dataDirectory.getAbsolutePath() );
            }

            volumes.put( dataDirectory, "/data,readonly" );
        }

        return volumes;
    }


    private void addPolyphenyArguments( List<String> command ) {
        String accessMode = config.accessMode;

        if ( "sql".equals( accessMode ) ) {
            command.add( "PolyphenyJdbcBenchmark" );

            String jdbcUrl = "jdbc:polypheny://pa:@"
                    + host + ":" + port + "/"
                    + config.namespace;

            addArgument( command, "url", jdbcUrl );
            command.add( "--polypheny-table-names=false" );

        } else if ( "mongoql".equals( accessMode ) ) {
            command.add( "PolyphenyMqlBenchmark" );

            addArgument( command, "host", host );
            addArgument( command, "port", port );
            addArgument( command, "username", "pa" );
            addArgument( command, "password", "" );
            addArgument( command, "namespace", config.namespace );

        } else {
            throw new IllegalArgumentException(
                    "Unsupported access mode: " + accessMode );
        }
    }


    private static void addArgument(
            List<String> command, String name, Object value ) {
        command.add( "--" + name );
        command.add( value.toString() );
    }

    private String getResultName () {
        String resultName = "polypheny".equals( config.engine )
                ? "parquet-" + config.accessMode
                : config.engine;
        return resultName;
    }

    private Map<String, List<CSVRecord>> readMeasuredRuns( File csvFile ) {
        Map<String, List<CSVRecord>> runsByQuery = new LinkedHashMap<>();

        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord( true )
                .get();

        try ( Reader reader = Files.newBufferedReader( csvFile.toPath() );
                CSVParser parser = format.parse( reader ) ) {

            for ( CSVRecord row : parser ) {
                if ( !"measured".equals( row.get( "phase" ) ) ) {
                    continue;
                }

                String queryId = row.get( "query_id" );

                if ( !runsByQuery.containsKey( queryId ) ) {
                    runsByQuery.put( queryId, new ArrayList<>() );
                }

                runsByQuery.get( queryId ).add( row );
            }
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not read results: " + csvFile, e );
        }

        return runsByQuery;
    }

}
