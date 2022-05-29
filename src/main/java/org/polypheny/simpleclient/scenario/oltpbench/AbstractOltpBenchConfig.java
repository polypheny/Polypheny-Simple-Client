package org.polypheny.simpleclient.scenario.oltpbench;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import org.polypheny.simpleclient.scenario.AbstractConfig;

public abstract class AbstractOltpBenchConfig extends AbstractConfig {

    //public int batchSize;
    public int scaleFactor;

    public AbstractOltpBenchConfig( Properties properties, int multiplier, String scenario, String system ) {
        super( scenario, system );

        pdbBranch = null;
        puiBranch = null;
        buildUi = false;
        resetCatalog = false;
        memoryCatalog = false;
        deployStoresUsingDocker = false;

        dataStores.add( "hsqldb" );
        planAndImplementationCaching = "Both";

        router = "icarus"; // For old routing, to be removed

        routers = new String[]{ "Simple", "Icarus", "FullPlacement" };
        newTablePlacementStrategy = "Single";
        planSelectionStrategy = "Best";
        preCostRatio = 50;
        postCostRatio = 50;
        routingCache = true;
        postCostAggregation = "onWarmup";

        progressReportBase = getIntProperty( properties, "progressReportBase" );
        numberOfThreads = getIntProperty( properties, "numberOfThreads" );
        numberOfWarmUpIterations = getIntProperty( properties, "numberOfWarmUpIterations" );


        // OLTPbench settings
        //batchSize = getIntProperty( properties, "batchSize"); // 128
        scaleFactor = multiplier;
    }


    public AbstractOltpBenchConfig( Map<String, String> cdl, String scenario, String system  ) {
        super( scenario, system );

        pdbBranch = cdl.get( "pdbBranch" );
        puiBranch = "master";
        buildUi = false;

        deployStoresUsingDocker = Boolean.parseBoolean( cdl.get( "deployStoresUsingDocker" ) );

        resetCatalog = true;
        memoryCatalog = Boolean.parseBoolean( cdl.get( "memoryCatalog" ) );

        numberOfThreads = Integer.parseInt( cdl.get( "numberOfThreads" ) );
        numberOfWarmUpIterations = 0;

        dataStores.addAll( Arrays.asList( cdl.get( "dataStore" ).split( "_" ) ) );
        planAndImplementationCaching = "Both";

        routers = cdl.get( "routers" ).split( "_" );
        newTablePlacementStrategy = cdl.get( "newTablePlacementStrategy" );
        planSelectionStrategy = cdl.get( "planSelectionStrategy" );

        preCostRatio = Integer.parseInt( cdlGetOrDefault( cdl, "preCostRatio", "50%" ).replace( "%", "" ).trim() );
        postCostRatio = Integer.parseInt( cdlGetOrDefault( cdl, "postCostRatio", "50%" ).replace( "%", "" ).trim() );
        postCostAggregation = cdlGetOrDefault( cdl, "postCostAggregation", "onWarmup" );
        routingCache = true;

        progressReportBase = 100;


        // OLTPbench settings
        batchSize = Integer.parseInt( cdl.get( "batchSize" ) );
        scaleFactor = Integer.parseInt( cdl.get( "scaleFactor" ) );
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return false;
    }


    public abstract String toXml(
            String dbType,
            String driver,
            String dbUrl,
            String username,
            String password,
            String ddlFileName,
            String dialectFileName,
            String isolation
    );

}
