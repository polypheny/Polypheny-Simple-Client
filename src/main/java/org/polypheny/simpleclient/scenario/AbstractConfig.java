package org.polypheny.simpleclient.scenario;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractConfig {

    public final String scenario;
    public final String system;

    public String pdbBranch;
    public String puiBranch;
    public boolean buildUi;
    public boolean memoryCatalog;
    public boolean resetCatalog;

    public List<String> dataStores = new ArrayList<>();
    public String planAndImplementationCaching;

    public int numberOfThreads;
    public int numberOfWarmUpIterations;

    public int progressReportBase;

    public boolean deployStoresUsingDocker;

    public String router;  // For old routing, to be removed
    public String[] routers;
    public String newTablePlacementStrategy;
    public String planSelectionStrategy;
    public int preCostRatio;
    public int postCostRatio;
    public boolean routingCache;
    public boolean postCostAggregation;

    public String workloadMonitoringProcessingInterval;
    public int workloadMonitoringElementsPerInterval;


    public AbstractConfig( String scenario, String system ) {
        this.scenario = scenario;
        this.system = system;
    }


    public abstract boolean usePreparedBatchForDataInsertion();


    private String getProperty( Properties properties, String name ) {
        return properties.getProperty( name );
    }


    protected String getStringProperty( Properties properties, String name ) {
        String str = getProperty( properties, name );
        if ( str == null ) {
            log.error( "Property '{}' not found in config", name );
            throw new RuntimeException( "Property '" + name + "' not found in config" );
        }
        return str;
    }


    protected int getIntProperty( Properties properties, String name ) {
        String str = getProperty( properties, name );
        if ( str == null ) {
            log.error( "Property '{}' not found in config", name );
            throw new RuntimeException( "Property '" + name + "' not found in config" );
        }
        return Integer.parseInt( str );
    }


    protected long getLongProperty( Properties properties, String name ) {
        String str = getProperty( properties, name );
        if ( str == null ) {
            log.error( "Property '{}' not found in config", name );
            throw new RuntimeException( "Property '" + name + "' not found in config" );
        }
        return Long.parseLong( str );
    }


    protected boolean getBooleanProperty( Properties properties, String name ) {
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


    protected String cdlGetOrDefault( Map<String, String> cdl, String key, String defaultValue ) {
        if ( cdl.containsKey( key ) ) {
            return cdl.get( key );
        } else {
            log.warn( "The job definition does not contain a value for '" + key + "' using default value '" + defaultValue + "' instead!" );
            return defaultValue;
        }
    }

}
