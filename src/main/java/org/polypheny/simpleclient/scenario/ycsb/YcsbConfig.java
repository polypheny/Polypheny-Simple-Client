package org.polypheny.simpleclient.scenario.ycsb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.polypheny.simpleclient.scenario.external.ExternalConfig;

public class YcsbConfig extends ExternalConfig {
    public final List<String> targets;

    public final String workload;

    public YcsbConfig( Path path ) throws IOException {
        this( read( path ) );
    }

    private YcsbConfig( Properties properties ) {
        super( "ycsb", properties, "ycsb.threadcount" );
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
        if ( numberOfThreads < 1 || Long.parseLong( properties.getProperty( "ycsb.maxexecutiontime", "0" ) ) < 0 ) {
            throw new IllegalArgumentException( "Threads must be positive and maxexecutiontime non-negative" );
        }
        if ( Long.parseLong( required( "ycsb.recordcount" ) ) < 1
                || Long.parseLong( required( "ycsb.operationcount" ) ) < 1
                || Integer.parseInt( required( "ycsb.fieldcount" ) ) < 1
                || Integer.parseInt( required( "ycsb.fieldlength" ) ) < 1 ) {
            throw new IllegalArgumentException( "YCSB counts and field sizes must be positive" );
        }
    }

    public YcsbConfig( Path base, Map<String, String> overrides, String network, String namespace ) throws IOException {
        this( loadManaged( base, overrides, network, namespace ) );
    }

    public void writeSetupProperties( Path destination ) throws IOException {
        try ( var output = Files.newOutputStream( destination ) ) {
            properties.store( output, null );
        }
    }

    private static Properties loadManaged( Path base, Map<String, String> overrides, String network, String namespace ) throws IOException {
        for ( String key : overrides.keySet() ) {
            if ( key.equals( "target.polypheny.db.url" ) || key.equals( "target.polypheny.db.namespace" ) ) {
                throw new IllegalArgumentException( "Connection URL and namespace are managed by Chronos" );
            }
            if ( !isCommonSetting( key ) && !key.startsWith( "ycsb." ) && !key.startsWith( "target.polypheny.db." )
                    && !Set.of( "workloadFile", "targets" ).contains( key ) ) {
                throw new IllegalArgumentException( "Unsupported YCSB job parameter: " + key );
            }
        }
        if ( overrides.containsKey( "targets" ) && !"polypheny".equals( overrides.get( "targets" ) ) ) {
            throw new IllegalArgumentException( "Managed jobs support only targets=polypheny" );
        }
        Properties properties = readManaged( base, overrides, network );
        properties.setProperty( "targets", "polypheny" );
        properties.setProperty( "target.polypheny.db.url", "jdbc:polypheny://polypheny:20590/" );
        properties.setProperty( "target.polypheny.db.namespace", namespace );
        configureMongoStore( properties, "ycsb_store" );
        return properties;
    }
}
