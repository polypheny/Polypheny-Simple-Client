package org.polypheny.simpleclient.scenario.ycsb;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.polypheny.simpleclient.main.DockerLauncher;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import org.polypheny.simpleclient.scenario.external.ExternalBenchmarkRunner;


public class YcsbRunner extends ExternalBenchmarkRunner {

    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final YcsbConfig config;

    private static final String JDBC_DRIVER = "org.polypheny.jdbc.PolyphenyDriver";


    public YcsbRunner( String jdbcUrl, String jdbcUser, String jdbcPassword, YcsbConfig config ) {
        super( "ycsb", config.imageName, config.network );

        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
        this.config = config;
    }

    @Override
    protected List<String> buildCommand() {
        List<String> command = buildYcsbCommand( "run" );
        addProperty( command, "operationcount", config.operationCount );

        return command;
    }


    @Override
    protected void prepareRun( File outputDirectory ) {
        // No additional preparation required.
    }

    @Override
    public void analyze( Properties results, File outputDirectory ) {
        File logFile = new File( outputDirectory, "run.log" );
        Properties measurements = new Properties();

        try ( BufferedReader reader = Files.newBufferedReader( logFile.toPath() ) ) {
            String line;

            while ( (line = reader.readLine()) != null ) {
                String[] parts = line.split( ",", 3 );

                if ( parts.length != 3 ) {
                    continue;
                }

                String group = parts[0].trim();

                if ( !group.startsWith( "[" ) || !group.endsWith( "]" ) ) {
                    continue;
                }

                group = group.substring( 1, group.length() - 1 );

                String metric = parts[1].trim();
                String value = parts[2].trim();

                measurements.setProperty( group + "." + metric, value );
            }
        } catch ( IOException e ) {
            throw new IllegalStateException(
                    "Could not read YCSB results: " + logFile, e );
        }

        if ( !measurements.containsKey( "OVERALL.RunTime(ms)" ) ) {
            throw new IllegalStateException(
                    "No YCSB results found in: " + logFile );
        }

        results.putAll( measurements );
    }


    List<String> buildYcsbCommand( String operation ) {
        if ( !"load".equals( operation ) && !"run".equals( operation ) ) {
            throw new IllegalArgumentException( "Unsupported YCSB operation: " + operation );
        }

        List<String> command = new ArrayList<>();

        if ( "sql".equals( config.accessMode ) ) {
            command.add( "./bin/ycsb.sh" );
            command.add( operation );
            command.add( "jdbc" );

        } else if ( "mongoql".equals( config.accessMode ) ) {
            command.add( "java" );
            command.add( "-cp" );
            command.add( "/ycsbExternal/conf:/ycsbExternal/lib/*" );
            command.add( "site.ycsb.Client" );
            command.add( "load".equals( operation ) ? "-load" : "-t" );
            command.add( "-db" );
            command.add( "org.polypheny.simpleclient.scenario.ycsb.binding.PolyphenyMongoBinding" );

            addProperty( command, "db.namespace", YcsbExternal.DOCUMENT_NAMESPACE );
        } else {
            throw new IllegalArgumentException( "Unsupported YCSB access mode: " + config.accessMode );
        }

        command.add( "-P" );
        command.add( config.workload );

        addProperty( command, "db.driver", JDBC_DRIVER );
        addProperty( command, "db.url", jdbcUrl );
        addProperty( command, "db.user", jdbcUser );
        addProperty( command, "db.passwd", jdbcPassword );

        addProperty( command, "recordcount", config.recordCount );
        addProperty( command, "fieldcount", config.fieldCount );
        addProperty( command, "fieldlength", config.fieldLength );
        addProperty( command, "threadcount", config.numberOfThreads );

        return command;
    }


    private static void addProperty( List<String> command, String name, Object value ) {
        command.add( "-p" );
        command.add( name + "=" + value );
    }


    void load( File logFile ) {
        List<String> command = buildYcsbCommand( "load" );
        execute( command, logFile );
    }



    private void execute( List<String> command, File logFile ) {
        String containerName = "ycsb-" + UUID.randomUUID();

        DockerLauncher.runJob(
                containerName,
                config.imageName,
                Map.of(),
                config.network,
                command,
                logFile
        );
    }

}
