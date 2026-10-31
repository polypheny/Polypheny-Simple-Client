package org.polypheny.simpleclient.scenario.ycsb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.polypheny.simpleclient.main.DockerLauncher;
import org.polypheny.simpleclient.scenario.external.ExternalBenchmarkRunner;

public class YcsbRunner extends ExternalBenchmarkRunner {
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
    protected Map<File, String> buildVolumes( File outputDirectory ) {
        return Map.of( config.file.getParent().toFile(), "/config", outputDirectory, "/results" );
    }

    @Override
    protected void prepareRun( File outputDirectory ) {
    }

    public void setup( File outputDirectory ) {
        execute( "setup", outputDirectory );
    }

    public void load( File outputDirectory ) {
        execute( "load", outputDirectory );
        analyze( new Properties(), outputDirectory, "load" );
    }

    private void execute( String phase, File outputDirectory ) {
        DockerLauncher.runJob( "ycsb-" + target + "-" + UUID.randomUUID(), config.imageName,
                Map.of(), config.network, buildCommand( phase ), new File( outputDirectory, phase + ".log" ),
                buildVolumes( outputDirectory ) );
    }

    @Override
    public void analyze( Properties results, File outputDirectory ) {
        analyze( results, outputDirectory, "run" );
    }

    private void analyze( Properties results, File outputDirectory, String phase ) {
        try {
            Properties metrics = YcsbMeasurements.read( outputDirectory.toPath().resolve( "measurements.txt" ) );
            YcsbMeasurements.validateCount( metrics, config.properties, phase );
            results.putAll( metrics );
            YcsbMeasurements.appendSummary( outputDirectory.toPath().getParent().getParent().resolve( "summary.csv" ),
                    target, phase, metrics );
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not read/write YCSB results: " + outputDirectory, e );
        }
    }

    private List<String> buildCommand( String phase ) {
        long timeout = Long.parseLong( config.properties.getProperty( "jobTimeoutSeconds", "3600" ) );
        long measurementSeconds = Long.parseLong( config.properties.getProperty( "ycsb.maxexecutiontime", "0" ) );
        if ( phase.equals( "run" ) && measurementSeconds > 0 ) {
            timeout = Math.min( timeout, measurementSeconds + 60 );
        }
        List<String> command = new ArrayList<>( List.of( "timeout", "--kill-after=10s", Long.toString( timeout ), "java", "-cp", CLASSPATH ) );
        if ( phase.equals( "setup" ) ) {
            command.addAll( List.of( BINDING_PACKAGE + "YcsbSetup", "/config/" + config.file.getFileName(), target ) );
            return command;
        }
        command.addAll( List.of( "site.ycsb.Client", "-s", phase.equals( "load" ) ? "-load" : "-t",
                "-P", config.workload, "-db", bindingClass( target ) ) );
        addProperties( command, "ycsb." );
        if ( phase.equals( "load" ) ) {
            addProperty( command, "maxexecutiontime", "0" );
        }
        addProperty( command, "dotransactions", Boolean.toString( phase.equals( "run" ) ) );
        addProperty( command, "exporter", "site.ycsb.measurements.exporter.TextMeasurementsExporter" );
        addProperty( command, "exportfile", "/results/measurements.txt" );
        addProperties( command, "target." + target + "." );
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
