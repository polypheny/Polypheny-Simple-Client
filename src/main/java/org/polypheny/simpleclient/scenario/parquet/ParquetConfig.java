package org.polypheny.simpleclient.scenario.parquet;

import java.util.List;
import java.util.Properties;
import org.polypheny.simpleclient.scenario.AbstractConfig;


/** Settings shared by the Parquet setup and benchmark runner. */
public class ParquetConfig extends AbstractConfig {

    public final Properties properties;
    public final String engine;
    public final String imageName;
    public final String network;
    public final String accessMode;
    public final String namespace;
    public final String queryFile;
    public final String dataDirectory;
    public final int warmups;
    public final int runs;
    public final int duckdbThreads;
    public final String duckdbMemoryLimit;
    public final int sparkShufflePartitions;
    public final String nestedCustomerFile;


    public ParquetConfig( Properties properties ) {
        super( "parquet", "polypheny-jdbc", properties );
        this.properties = properties;
        engine = getStringProperty( properties, "engine" );
        imageName = getStringProperty( properties, "imageName" );
        network = getStringProperty( properties, "network" );
        accessMode = properties.getProperty( "accessMode", "sql" ).trim();
        namespace = properties.getProperty( "namespace", "public" ).trim();
        queryFile = properties.getProperty( "queryFile", "" ).trim();
        dataDirectory = properties.getProperty( "dataDirectory", "" ).trim();
        warmups = getIntProperty( properties, "warmups" );
        runs = getIntProperty( properties, "runs" );
        duckdbThreads = Integer.parseInt( properties.getProperty( "duckdbThreads", "2" ).trim() );
        duckdbMemoryLimit = properties.getProperty( "duckdbMemoryLimit", "4GB" ).trim();
        sparkShufflePartitions = Integer.parseInt( properties.getProperty( "sparkShufflePartitions", "2" ).trim() );
        nestedCustomerFile = properties.getProperty( "nestedCustomerFile", "" ).trim();

        if ( !List.of( "polypheny", "duckdb", "spark" ).contains( engine ) ) {
            throw new IllegalArgumentException( "Unsupported Parquet engine: " + engine );
        }
        if ( "polypheny".equals( engine ) && !List.of( "sql", "mongoql" ).contains( accessMode ) ) {
            throw new IllegalArgumentException( "Unsupported Parquet access mode: " + accessMode );
        }
        if ( warmups < 0 || runs < 1 || duckdbThreads < 1 || sparkShufflePartitions < 1 ) {
            throw new IllegalArgumentException( "Warmups must be >= 0; runs, threads and shuffle partitions must be > 0" );
        }
        if ( imageName.isBlank() || network.isBlank() || namespace.isBlank() || duckdbMemoryLimit.isBlank() ) {
            throw new IllegalArgumentException( "Image name, network, namespace and memory limit must not be blank" );
        }
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return false;
    }
}
