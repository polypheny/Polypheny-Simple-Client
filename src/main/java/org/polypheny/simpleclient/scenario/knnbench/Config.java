package org.polypheny.simpleclient.scenario.knnbench;


import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.scenario.IConfig;


@Slf4j
public class Config implements IConfig {
    public final String system;

    public final String pdbBranch;
    public final String puiBranch;
    public final boolean memoryCatalog;
    public final boolean resetCatalog;

    public final String dataStoreFeature;
    public final String dataStoreMetadata;
    public final String router;
    public final String planAndImplementationCaching;

    public final int numberOfThreads;

    public final long randomSeedInsert;
    public final long randomSeedQuery;

    public final int dimensionFeatureVectors;
    public final int batchSizeInserts;
    public final int batchSizeQueries;


    public final int numberOfEntries;
    public final int numberOfPureKnnQueries;
    public final int numberOfCombinedQueries;

    public final int limitKnnQueries;
    public final String distanceNorm;



    public final int progressReportBase;


    public Config( Properties properties, int multiplier ) {
        system = "polypheny";

        pdbBranch = null;
        puiBranch = null;
        resetCatalog = false;
        memoryCatalog = false;

        dataStoreFeature = "cottontail";
        dataStoreMetadata = "cottontail";
        router = "icarus";
        planAndImplementationCaching = "Both";

        progressReportBase = getIntProperty( properties, "progressReportBase" );
        numberOfThreads = getIntProperty( properties, "numberOfThreads" );

        randomSeedInsert = getLongProperty( properties, "randomSeedInsert" );
        randomSeedQuery = getLongProperty( properties, "randomSeedQuery" );

        dimensionFeatureVectors = getIntProperty( properties, "dimensionFeatureVectors" );
        batchSizeInserts = getIntProperty( properties, "batchSizeInserts" );
        batchSizeQueries = getIntProperty( properties, "batchSizeQueries" );

        numberOfEntries = getIntProperty( properties, "numberOfEntries" ) * multiplier;
        numberOfPureKnnQueries = getIntProperty( properties, "numberOfPureKnnQueries" ) * multiplier;
        numberOfCombinedQueries = getIntProperty( properties, "numberOfCombinedQueries" ) * multiplier;
        limitKnnQueries = getIntProperty( properties, "limitKnnQueries" );
        distanceNorm = getStringProperty( properties, "distanceNorm" );
    }


    private String getStringProperty( Properties properties, String name ) {
        String str = getProperty( properties, name );
        if ( str == null ) {
            log.error( "Property '{}' not found in config", name );
            throw new RuntimeException( "Property '" + name + "' not found in config" );
        }
        return str;
    }


    private int getIntProperty( Properties properties, String name ) {
        String str = getProperty( properties, name );
        if ( str == null ) {
            log.error( "Property '{}' not found in config", name );
            throw new RuntimeException( "Property '" + name + "' not found in config" );
        }
        return Integer.parseInt( str );
    }


    private long getLongProperty( Properties properties, String name ) {
        String str = getProperty( properties, name );
        if ( str == null ) {
            log.error( "Property '{}' not found in config", name );
            throw new RuntimeException( "Property '" + name + "' not found in config" );
        }
        return Long.parseLong( str );
    }


    private boolean getBooleanProperty( Properties properties, String name ) {
        String str = getStringProperty( properties, name );
        switch ( str ) {
            case "true":
                return true;
            case "false":
                return false;
            default:
                log.error( "Value for config property '{}' is unknown. Supported values are 'true' and 'false'. Current value is: {}", name, str );
                throw new RuntimeException( "Value for config property '" + name + "' is unknown. " + "Supported values are 'true' and 'false'. Current value is: " + str );
        }
    }


    private String getProperty( Properties properties, String name ) {
        return properties.getProperty( name );
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return true;
    }
}
