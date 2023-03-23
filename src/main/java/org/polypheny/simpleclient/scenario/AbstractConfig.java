/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2022 The Polypheny Project
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.polypheny.simpleclient.scenario;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public abstract class AbstractConfig {

    public final String scenario;
    public final String system;

    public final String pdbBranch;
    public final String puiBranch;
    public final boolean buildUi;
    public final boolean memoryCatalog;
    public final boolean resetCatalog;
    public final boolean restartAfterLoadingData;

    public final List<String> dataStores = new ArrayList<>();
    public final String planAndImplementationCaching;

    public final int numberOfThreads;

    public final boolean deployStoresUsingDocker;

    public final String router;  // For old routing, to be removed
    public final String[] routers;
    public final String newTablePlacementStrategy;
    public final String planSelectionStrategy;
    public final int preCostRatio;
    public final int postCostRatio;
    public final boolean routingCache;
    public final String postCostAggregation;

    public final int numberOfWarmUpIterations;

    public final boolean workloadMonitoringExecutingWorkload;
    public final boolean workloadMonitoringLoadingData;
    public final boolean workloadMonitoringWarmup;

    public final int progressReportBase = 100;


    protected AbstractConfig( String scenario, String system, Properties properties ) {
        this.scenario = scenario;
        this.system = system;

        pdbBranch = null;
        puiBranch = null;
        buildUi = false;
        resetCatalog = false;
        memoryCatalog = false;
        deployStoresUsingDocker = false;
        restartAfterLoadingData = false;

        dataStores.add( "hsqldb" );
        planAndImplementationCaching = "Both";

        router = "icarus"; // For old routing, to be removed

        routers = new String[]{ "Simple", "Icarus", "FullPlacement" };
        newTablePlacementStrategy = "All";
        planSelectionStrategy = "Best";
        preCostRatio = 50;
        postCostRatio = 50;
        routingCache = true;
        postCostAggregation = "onWarmup";

        numberOfThreads = getIntProperty( properties, "numberOfThreads" );
        numberOfWarmUpIterations = getIntProperty( properties, "numberOfWarmUpIterations" );

        workloadMonitoringExecutingWorkload = false;
        workloadMonitoringLoadingData = true;
        workloadMonitoringWarmup = true;
    }


    protected AbstractConfig( String scenario, String system, Map<String, String> cdl ) {
        this.system = system;
        this.scenario = scenario;

        pdbBranch = cdl.get( "pdbBranch" );
        puiBranch = "master";
        buildUi = false;
        resetCatalog = true;

        restartAfterLoadingData = Boolean.parseBoolean( cdlGetOrDefault( cdl, "restartAfterLoadingData", "false" ) );
        if ( restartAfterLoadingData && dataStores.contains( "hsqldb" ) ) {
            throw new RuntimeException( "Not allowed to restart Polypheny after loading data if using a non-persistent data store!" );
        }

        deployStoresUsingDocker = Boolean.parseBoolean( cdlGetOrDefault( cdl, "deployStoresUsingDocker", "false" ) );

        memoryCatalog = Boolean.parseBoolean( cdl.get( "memoryCatalog" ) );

        numberOfThreads = Integer.parseInt( cdl.get( "numberOfThreads" ) );
        numberOfWarmUpIterations = Integer.parseInt( cdlGetOrDefault( cdl, "numberOfWarmUpIterations", "4" ) );

        dataStores.addAll( Arrays.asList( cdl.get( "dataStore" ).split( "_" ) ) );
        planAndImplementationCaching = cdlGetOrDefault( cdl, "planAndImplementationCaching", "Both" );

        router = cdl.get( "router" ); // For old routing, to be removed

        routers = cdlGetOrDefault( cdl, "routers", "Simple_Icarus_FullPlacement" ).split( "_" );
        newTablePlacementStrategy = cdlGetOrDefault( cdl, "newTablePlacementStrategy", "Single" );
        planSelectionStrategy = cdlGetOrDefault( cdl, "planSelectionStrategy", "Best" );

        preCostRatio = Integer.parseInt( cdlGetOrDefault( cdl, "preCostRatio", "50%" ).replace( "%", "" ).trim() );
        postCostRatio = Integer.parseInt( cdlGetOrDefault( cdl, "postCostRatio", "50%" ).replace( "%", "" ).trim() );
        routingCache = Boolean.parseBoolean( cdlGetOrDefault( cdl, "routingCache", "true" ) );
        postCostAggregation = cdlGetOrDefault( cdl, "postCostAggregation", "onWarmup" );

        workloadMonitoringExecutingWorkload = Boolean.parseBoolean( cdlGetOrDefault( cdl, "workloadMonitoring", "false" ) );
        workloadMonitoringLoadingData = Boolean.parseBoolean( cdlGetOrDefault( cdl, "workloadMonitoringLoadingData", "false" ) );
        workloadMonitoringWarmup = Boolean.parseBoolean( cdlGetOrDefault( cdl, "workloadMonitoringWarmup", "true" ) );
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


    protected double getDoubleProperty( Properties properties, String name ) {
        String str = getProperty( properties, name );
        if ( str == null ) {
            log.error( "Property '{}' not found in config", name );
            throw new RuntimeException( "Property '" + name + "' not found in config" );
        }
        return Double.parseDouble( str );
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
