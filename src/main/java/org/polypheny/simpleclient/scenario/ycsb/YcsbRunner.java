package org.polypheny.simpleclient.scenario.ycsb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.polypheny.simpleclient.main.DockerLauncher;
import org.polypheny.simpleclient.scenario.external.BenchmarkRunner;
import org.polypheny.simpleclient.scenario.external.ExternalBenchmarkRunner;

public class YcsbRunner extends ExternalBenchmarkRunner implements BenchmarkRunner {
    public static final String CLASSPATH = "/ycsbExternal/conf:/ycsbExternal/lib/*:"
            + "/ycsbExternal/mongodb-binding/lib/*:/ycsbExternal/postgrenosql-binding/lib/*:"
            + "/ycsbExternal/jdbc-binding/lib/*";
    public static final String BINDING_PACKAGE = "org.polypheny.simpleclient.scenario.ycsb.binding.";

    private final YcsbConfig config;
    private final String target;

    public YcsbRunner( YcsbConfig config, String target ) {
        super( "ycsb-" + target + "-", config.imageName, config.network );
        this.config = config;
        this.target = target;
    }

    @Override
    protected List<String> buildCommand() {
        return buildCommand( "run" );
    }

    @Override
    protected long timeoutSeconds() {
        return Long.parseLong( config.properties.getProperty( "jobTimeoutSeconds", "3600" ) ) + 30;
    }

    @Override
    protected void prepareRun( File outputDirectory ) {
        // A repeated run must never analyze measurements from a previous container.
        try {
            Files.deleteIfExists( outputDirectory.toPath().resolve( "measurements.txt" ) );
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not clear previous YCSB measurements", e );
        }
    }

    @Override
    public void setup( File outputDirectory ) throws IOException {
        var input = Files.createTempFile( "ycsb-setup-", ".properties" );
        try {
            config.writeSetupProperties( input );
            execute( "setup", outputDirectory, input.toFile() );
        } finally {
            Files.deleteIfExists( input );
        }
    }

    @Override
    public void load( File outputDirectory ) throws IOException {
        prepareRun( outputDirectory );
        execute( "load", outputDirectory, null );
        analyze( new Properties(), outputDirectory, "load" );
    }

    @Override
    public void warmUp( File outputDirectory ) throws IOException {
        if ( config.warmupTime <= 0 ) {
            throw new IllegalArgumentException( "Set warmupTime > 0 to run warmup" );
        }
        prepareRun( outputDirectory );
        execute( "warmup", outputDirectory, null );
        analyze( new Properties(), outputDirectory, "warmup" );
    }

    private Properties phaseProperties( String phase ) {
        Properties properties = new Properties();
        properties.putAll( config.properties );
        if ( phase.equals( "warmup" ) ) {
            properties.setProperty( "ycsb.maxexecutiontime", Integer.toString( config.warmupTime ) );
            properties.setProperty( "ycsb.operationcount", Integer.toString( Integer.MAX_VALUE ) );
        }
        return properties;
    }

    private void execute( String phase, File outputDirectory, File standardInput ) {
        DockerLauncher.runJob( "ycsb-" + target + "-" + UUID.randomUUID(), config.imageName,
                Map.of(), config.network, buildCommand( phase ), new File( outputDirectory, phase + ".log" ),
                Map.of(), standardInput, timeoutSeconds() );
    }

    @Override
    public void analyze( Properties results, File outputDirectory ) throws IOException {
        analyze( results, outputDirectory, "run" );
    }

    private void analyze( Properties results, File outputDirectory, String phase ) throws IOException {
        var measurements = outputDirectory.toPath().resolve( "measurements.txt" );
        if ( !Files.exists( measurements ) ) {
            try ( var log = Files.newBufferedReader( outputDirectory.toPath().resolve( phase + ".log" ) );
                    var output = Files.newBufferedWriter( measurements ) ) {
                String line;
                while ( (line = log.readLine()) != null ) {
                    if ( line.startsWith( "[" ) && line.contains( "], " ) ) {
                        output.write( line );
                        output.newLine();
                    }
                }
            }
        }
        Properties metrics = YcsbMeasurements.read( measurements );
        YcsbMeasurements.validateCount( metrics, phaseProperties( phase ), phase );
        results.putAll( metrics );
        YcsbMeasurements.appendSummary( outputDirectory.toPath().getParent().getParent().resolve( "summary.csv" ),
                target, phase, metrics );
    }

    private List<String> buildCommand( String phase ) {
        long timeout = Long.parseLong( config.properties.getProperty( "jobTimeoutSeconds", "3600" ) );
        long measurementSeconds = Long.parseLong( phaseProperties( phase ).getProperty( "ycsb.maxexecutiontime", "0" ) );
        if ( !phase.equals( "load" ) && measurementSeconds > 0 ) {
            timeout = Math.min( timeout, measurementSeconds + 60 );
        }
        List<String> command = new ArrayList<>( List.of( "timeout", "--kill-after=10s", Long.toString( timeout ), "java", "-cp", CLASSPATH ) );
        if ( phase.equals( "setup" ) ) {
            command.addAll( List.of( BINDING_PACKAGE + "YcsbSetup", "-", target ) );
            return command;
        }
        command.addAll( List.of( "site.ycsb.Client", "-s", phase.equals( "load" ) ? "-load" : "-t",
                "-P", config.workload, "-db", bindingClass( target ) ) );
        addProperties( command, "ycsb." );
        if ( phase.equals( "load" ) ) {
            addProperty( command, "maxexecutiontime", "0" );
        }
        addProperty( command, "dotransactions", Boolean.toString( !phase.equals( "load" ) ) );
        addProperty( command, "exporter", "site.ycsb.measurements.exporter.TextMeasurementsExporter" );
        addProperties( command, "target." + target + "." );
        if ( phase.equals( "warmup" ) ) {
            addProperty( command, "maxexecutiontime", Integer.toString( config.warmupTime ) );
            // YCSB 0.17 parses an int and starts no workers for operationcount=0.
            addProperty( command, "operationcount", Integer.toString( Integer.MAX_VALUE ) );
        }
        addProperty( command, "exportfile", "/dev/stdout" );
        return command;
    }

    private void addProperties( List<String> command, String prefix ) {
        for ( String key : config.properties.stringPropertyNames() ) {
            if ( key.startsWith( prefix ) ) {
                addProperty( command, key.substring( prefix.length() ), config.properties.getProperty( key ) );
            }
        }
    }

    private static void addProperty( List<String> command, String key, String value ) {
        command.addAll( List.of( "-p", key + "=" + value ) );
    }

    private static String bindingClass( String target ) {
        return switch ( target ) {
            case "polypheny" -> BINDING_PACKAGE + "PolyphenyMongoBinding";
            case "mongodb" -> "site.ycsb.db.MongoDbClient";
            case "postgrenosql" -> "site.ycsb.postgrenosql.PostgreNoSQLDBClient";
            case "postgresql" -> "site.ycsb.db.JdbcDBClient";
            default -> throw new IllegalArgumentException( "Unknown target: " + target );
        };
    }
}
