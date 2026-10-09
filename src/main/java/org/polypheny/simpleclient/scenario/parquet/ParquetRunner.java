package org.polypheny.simpleclient.scenario.parquet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.polypheny.simpleclient.main.DockerLauncher;


public class ParquetRunner {

    private final ParquetConfig config;
    private final String host;
    private final int port;


    public ParquetRunner( ParquetConfig config, String host, int port ) {
        this.config = config;
        this.host = host;
        this.port = port;
    }


    public void run( File outputDirectory ) {
        String engine = config.engine;
        if ( config.queryFile.isBlank() ) {
            throw new IllegalArgumentException( "Missing Parquet queryFile" );
        }
        try {
            Files.createDirectories( outputDirectory.toPath() );
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not create results directory: " + outputDirectory, e );
        }

        String resultName = "polypheny".equals( engine )
                ? "parquet-" + config.accessMode
                : engine;

        List<String> command = buildCommand( engine, resultName );

        File logFile = new File(
                outputDirectory, resultName + "-run.log" );

        Map<File, String> volumes = new HashMap<>();
        volumes.put( outputDirectory, "/results" );

        // DuckDB and Spark read Parquet files directly.
        if ( "duckdb".equals( engine ) || "spark".equals( engine ) ) {
            File dataDirectory = new File( config.dataDirectory );

            if ( !dataDirectory.isDirectory() ) {
                throw new IllegalArgumentException(
                        "Data directory does not exist: "
                                + dataDirectory.getAbsolutePath() );
            }

            volumes.put( dataDirectory, "/data,readonly" );
        }

        File csvFile = new File( outputDirectory, resultName + ".csv" );
        try {
            Files.deleteIfExists( csvFile.toPath() );
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not clear previous results: " + csvFile, e );
        }

        DockerLauncher.runJob(
                "parquet-" + UUID.randomUUID(),
                config.imageName,
                Map.of(),
                config.network,
                command,
                logFile,
                volumes
        );

        ParquetResults.check( csvFile, config.runs );

        System.out.println( "Benchmark completed. Results: " + outputDirectory.getAbsolutePath() );
    }


    private List<String> buildCommand( String engine, String resultName ) {
        List<String> command = new ArrayList<>();

        if ( "polypheny".equals( engine ) ) {
            addPolyphenyArguments( command );

        } else if ( "duckdb".equals( engine ) ) {
            addArgument( command, "data-dir", "/data" );
            addArgument( command, "threads", config.duckdbThreads );
            addArgument( command, "memory-limit", config.duckdbMemoryLimit );

        } else if ( "spark".equals( engine ) ) {
            addArgument( command, "data-dir", "/data" );
            addArgument( command, "shuffle-partitions", config.sparkShufflePartitions );

        } else {
            throw new IllegalArgumentException( "Unsupported engine: " + engine );
        }

        addArgument( command, "queries", config.queryFile );
        addArgument( command, "warmups", config.warmups );
        addArgument( command, "runs", config.runs );
        addArgument( command, "output", "/results/" + resultName + ".csv" );

        return command;
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
}
