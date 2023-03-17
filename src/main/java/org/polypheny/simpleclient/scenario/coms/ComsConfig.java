/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/11/23, 11:18 AM The Polypheny Project
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
 */

package org.polypheny.simpleclient.scenario.coms;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.simpleclient.scenario.AbstractConfig;

@EqualsAndHashCode(callSuper = true)
@Value
public class ComsConfig extends AbstractConfig {

    public long seed;
    public int networkScale;
    public int pcDynConfigsMax;
    public int switchConfigs;
    public int mobileDynConfigsMax;

    public int connectionConfigs;
    public int apDynConfigs;
    public int nestingDepth;
    public int graphCreateBatch;
    public int docCreateBatch;
    public int relCreateBatch;
    public List<Integer> threadDistribution;
    public int numberOfAddDevicesQueries;
    public int numberOfChangeDeviceAttributeQueries;
    public int numberOfRemoveDevicesQueries;
    public int numberOfAddLogsQueries;
    public int numberOfDeleteLogsQueries;
    public int numberOfScanLogsQueries;
    public int numberOfAddNewDeviceQueries;
    public int numberOfAddNewSwitchQueries;
    public int numberOfDropDevicesQueries;


    public ComsConfig( String system, Properties properties ) {
        super( "coms", system, properties );

        threadDistribution = Arrays.stream( getStringProperty( properties, "threadDistribution" ).split( "_" ) ).map( Integer::parseInt ).collect( Collectors.toList() );

        seed = getLongProperty( properties, "seed" );

        networkScale = getIntProperty( properties, "networkScale" );
        pcDynConfigsMax = getIntProperty( properties, "pcDynConfigsMax" );
        switchConfigs = getIntProperty( properties, "switchConfigs" );
        mobileDynConfigsMax = getIntProperty( properties, "mobileDynConfigsMax" );

        connectionConfigs = getIntProperty( properties, "connectionConfigs" );

        apDynConfigs = getIntProperty( properties, "connectionConfigs" );

        nestingDepth = getIntProperty( properties, "nestingDepth" );

        graphCreateBatch = getIntProperty( properties, "graphCreateBatch" );

        docCreateBatch = getIntProperty( properties, "docCreateBatch" );
        relCreateBatch = getIntProperty( properties, "relCreateBatch" );

        //// QUERIES

        numberOfAddDevicesQueries = getIntProperty( properties, "numberOfAddDevicesQueries" );
        numberOfChangeDeviceAttributeQueries = getIntProperty( properties, "numberOfChangeDeviceAttributeQueries" );
        numberOfRemoveDevicesQueries = getIntProperty( properties, "numberOfRemoveDevicesQueries" );
        numberOfAddLogsQueries = getIntProperty( properties, "numberOfAddLogsQueries" );
        numberOfDeleteLogsQueries = getIntProperty( properties, "numberOfDeleteLogsQueries" );
        numberOfScanLogsQueries = getIntProperty( properties, "numberOfScanLogsQueries" );
        numberOfAddNewDeviceQueries = getIntProperty( properties, "numberOfAddNewDeviceQueries" );
        numberOfAddNewSwitchQueries = getIntProperty( properties, "numberOfAddNewSwitchQueries" );
        numberOfDropDevicesQueries = getIntProperty( properties, "numberOfDropDevicesQueries" );

    }


    public ComsConfig( Map<String, String> parsedConfig ) {
        super( "coms", parsedConfig.get( "store" ), parsedConfig );

        seed = Long.parseLong( parsedConfig.get( "seed" ) );
        threadDistribution = Arrays.stream( parsedConfig.get( "threadDistribution" ).split( "_" ) ).map( Integer::parseInt ).collect( Collectors.toList() );

        networkScale = Integer.parseInt( parsedConfig.get( "networkScale" ) );
        pcDynConfigsMax = Integer.parseInt( parsedConfig.get( "pcDynConfigsMax" ) );
        switchConfigs = Integer.parseInt( parsedConfig.get( "switchConfigs" ) );
        mobileDynConfigsMax = Integer.parseInt( parsedConfig.get( "mobileDynConfigsMax" ) );

        connectionConfigs = Integer.parseInt( parsedConfig.get( "connectionConfigs" ) );

        apDynConfigs = Integer.parseInt( parsedConfig.get( "apDynConfigs" ) );

        nestingDepth = Integer.parseInt( parsedConfig.get( "nestingDepth" ) );

        graphCreateBatch = Integer.parseInt( parsedConfig.get( "graphCreateBatch" ) );

        docCreateBatch = Integer.parseInt( parsedConfig.get( "docCreateBatch" ) );

        relCreateBatch = Integer.parseInt( parsedConfig.get( "relCreateBatch" ) );

        //// QUERIES

        numberOfAddDevicesQueries = Integer.parseInt( parsedConfig.get( "numberOfAddDevicesQueries" ) );
        numberOfChangeDeviceAttributeQueries = Integer.parseInt( parsedConfig.get( "numberOfChangeDeviceAttributeQueries" ) );
        numberOfRemoveDevicesQueries = Integer.parseInt( parsedConfig.get( "numberOfRemoveDevicesQueries" ) );
        numberOfAddLogsQueries = Integer.parseInt( parsedConfig.get( "numberOfAddLogsQueries" ) );
        numberOfDeleteLogsQueries = Integer.parseInt( parsedConfig.get( "numberOfDeleteLogsQueries" ) );
        numberOfScanLogsQueries = Integer.parseInt( parsedConfig.get( "numberOfScanLogsQueries" ) );
        numberOfAddNewDeviceQueries = Integer.parseInt( parsedConfig.get( "numberOfAddNewDeviceQueries" ) );
        numberOfAddNewSwitchQueries = Integer.parseInt( parsedConfig.get( "numberOfAddNewSwitchQueries" ) );
        numberOfDropDevicesQueries = Integer.parseInt( parsedConfig.get( "numberOfDropDevicesQueries" ) );
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return false;
    }

}
