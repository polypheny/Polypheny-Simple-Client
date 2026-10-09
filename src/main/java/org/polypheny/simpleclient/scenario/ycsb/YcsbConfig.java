package org.polypheny.simpleclient.scenario.ycsb;

import java.util.Properties;
import org.polypheny.simpleclient.scenario.AbstractConfig;


public class YcsbConfig extends AbstractConfig {

    public final String workload;
    public final int recordCount;
    public final int operationCount;

    public final int fieldCount;
    public final int fieldLength;

    public final String imageName;
    public final String network;

    public final String accessMode;
    public final String storageBackend;


    public YcsbConfig( Properties properties, int multiplier ) {
        super( "ycsb", toSystem( properties.getProperty( "accessMode", "sql" ).trim() ), properties );
        accessMode = properties.getProperty( "accessMode", "sql" ).trim();
        workload = getStringProperty( properties, "workload" );
        recordCount = getIntProperty( properties, "recordCount" );

        if ( multiplier < 1 ) {
            throw new IllegalArgumentException( "Multiplier must be greater than 0" );
        }
        // The multiplier increases workload operations; the dataset size stays fixed.
        operationCount = Math.multiplyExact( getIntProperty( properties, "operationCount" ), multiplier );

        fieldCount = getIntProperty( properties, "fieldCount" );
        fieldLength = getIntProperty( properties, "fieldLength" );

        imageName = getStringProperty( properties, "imageName" );
        network = getStringProperty( properties, "network" );
        storageBackend = getStringProperty( properties, "storageBackend" ).trim();

        if ( numberOfWarmUpIterations != 0 ) {
            throw new IllegalArgumentException( "External YCSB warmup is not implemented; use numberOfWarmUpIterations=0" );
        }
        validate();
    }


    // just to make it a bit more readable
    private static String toSystem( String accessMode ) {
        if ( "sql".equals( accessMode ) ) {
            return "polypheny-jdbc";
        }
        if ( "mongoql".equals( accessMode ) ) {
            return "polypheny-mongoql";
        }
        throw new IllegalArgumentException( "Unsupported YCSB access mode: " + accessMode );
    }


    private void validate() {
        if ( recordCount < 1 || operationCount < 1 || fieldCount < 1 || fieldLength < 1 || numberOfThreads < 1 ) {
            throw new IllegalArgumentException( "Record count, operation count, field count, field length and number of threads must be greater than 0" );
        }
        if ( workload == null || workload.isBlank() || imageName == null || imageName.isBlank() || network == null || network.isBlank() ) {
            throw new IllegalArgumentException( "Workload, image name and network must not be blank" );
        }

        if ( !"postgres".equals( storageBackend ) && !"mongodb".equals( storageBackend ) ) {
            throw new IllegalArgumentException( "Unsupported storage backend: " + storageBackend );
        }
        if ( "mongodb".equals( storageBackend ) && !"mongoql".equals( accessMode ) ) {
            throw new IllegalArgumentException( "MongoDB requires accessMode=mongoql" );
        }
        if ( "mongoql".equals( accessMode ) && !"workloads/workloadc".equals( workload ) ) {
            throw new IllegalArgumentException( "The MongoQL binding currently supports only workloads/workloadc (insert and read)" );
        }
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return false;
    }

}
