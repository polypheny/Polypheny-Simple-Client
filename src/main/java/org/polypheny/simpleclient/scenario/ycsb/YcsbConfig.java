package org.polypheny.simpleclient.scenario.ycsb;

import java.util.Map;
import java.util.Properties;
import org.polypheny.simpleclient.scenario.AbstractConfig;


public class YcsbConfig extends AbstractConfig {

    public String workload;
    public int recordCount;
    public int operationCount;

    public int fieldCount;
    public int fieldLength;

    public String imageName;
    public String network;

    public boolean partitionTable;


    public YcsbConfig( Properties properties, int multiplier ) {
        super( "ycsb", properties.getProperty( "store", "polypheny-jdbc" ), properties );
        workload = getStringProperty( properties, "workload" );
        recordCount = getIntProperty( properties, "recordCount" );

        operationCount = Math.multiplyExact( getIntProperty( properties, "operationCount" ), multiplier );

        fieldCount = getIntProperty( properties, "fieldCount" );
        fieldLength = getIntProperty( properties, "fieldLength" );

        imageName = getStringProperty( properties, "imageName" );
        network = getStringProperty( properties, "network" );
        partitionTable = getBooleanProperty( properties, "partitionTable" );

        validate();
    }


    public YcsbConfig( Map<String, String> cdl ) {
        super( "ycsb", cdl.get( "store" ), cdl );

        workload = cdl.get( "workload" );
        recordCount = Integer.parseInt( cdl.get( "recordCount" ) );
        operationCount = Integer.parseInt( cdl.get( "operationCount" ) );

        fieldCount = Integer.parseInt( cdl.get( "fieldCount" ) );
        fieldLength = Integer.parseInt( cdl.get( "fieldLength" ) );

        imageName = cdl.get( "imageName" );
        network = cdl.get( "network" );
        partitionTable = Boolean.parseBoolean( cdl.get( "partitionTable" ) );

        validate();
    }


    private void validate() {
        if ( !"polypheny-jdbc".equals( system ) && !"polypheny-mongoql".equals( system ) ) {
            throw new IllegalArgumentException( "Unsupported YCSB system: " + system );
        }

        if ( "polypheny-mongoql".equals( system ) && partitionTable ) {
            throw new IllegalArgumentException( "partitionTable is only supported for relational YCSB" );
        }

        if ( recordCount < 1 || operationCount < 1 || fieldCount < 1 || fieldLength < 1 || numberOfThreads < 1 ) {
            throw new IllegalArgumentException( "Record count, operation count, field count, field length and number of threads must be greater than 0" );
        }
        if ( workload == null || workload.isBlank() || imageName == null || imageName.isBlank() || network == null || network.isBlank() ) {
            throw new IllegalArgumentException( "Workload, image name and network must not be blank" );
        }
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return false;
    }

}
