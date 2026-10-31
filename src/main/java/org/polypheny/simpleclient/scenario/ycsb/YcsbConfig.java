package org.polypheny.simpleclient.scenario.ycsb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.polypheny.simpleclient.scenario.AbstractConfig;

public class YcsbConfig extends AbstractConfig {
    public final Properties properties;
    public final Path file;
    public final List<String> targets;
    public final String imageName;
    public final String network;

    public final String workload;

    public YcsbConfig( Path path ) throws IOException {
        this( read( path ), path.toRealPath() );
    }

    public YcsbConfig( Properties properties, Path file ) {
        super( "ycsb", "polypheny-mongoql", scenarioProperties( properties ) );
        this.properties = properties;
        this.file = file;
        imageName = required( "imageName" );
        network = required( "network" );
        targets = List.of( required( "targets" ).split( "\\s*,\\s*", -1 ) );
        if ( Set.copyOf( targets ).size() != targets.size() ) {
            throw new IllegalArgumentException( "Duplicate targets" );
        }
        if ( targets.contains( "postgrenosql" ) && numberOfThreads != 1 ) {
            throw new IllegalArgumentException( "YCSB 0.17.0 PostgreNoSQL requires ycsb.threadcount=1: "
                    + "its shared prepared statements are unsafe with multiple threads. "
                    + "For concurrent runs use targets=polypheny,mongodb." );
        }

        workload = required( "workloadFile" );
        if ( Long.parseLong( required( "ycsb.recordcount" ) ) < 1
                || Long.parseLong( required( "ycsb.operationcount" ) ) < 1
                || Integer.parseInt( required( "ycsb.fieldcount" ) ) < 1
                || Integer.parseInt( required( "ycsb.fieldlength" ) ) < 1 ) {
            throw new IllegalArgumentException( "YCSB counts and field sizes must be positive" );
        }
        if ( Integer.parseInt( properties.getProperty( "jobTimeoutSeconds", "3600" ) ) < 1 ) {
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

    private static Properties read( Path file ) throws IOException {
        Properties properties = new Properties();
        try ( var input = Files.newInputStream( file ) ) {
            properties.load( input );
        }
        return properties;
    }

    private static Properties scenarioProperties( Properties properties ) {
        Properties scenario = new Properties();
        scenario.setProperty( "numberOfThreads", properties.getProperty( "ycsb.threadcount", "1" ) );
        scenario.setProperty( "numberOfWarmUpIterations", "0" );
        return scenario;
    }

    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return false;
    }
}
