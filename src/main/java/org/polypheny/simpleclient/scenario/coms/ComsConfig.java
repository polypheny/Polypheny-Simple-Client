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
import org.polypheny.simpleclient.cli.Mode;
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

    public Mode mode;
    public boolean allowNegative;
    public String graphStore;
    public String docStore;
    public String relStore;
    public int cycles;
    public double removeLogs;
    public double addLogs;
    public double changeDevice;
    public double newDevice;
    public double removeDevice;
    public double removeUsers;
    public int readQueries;
    public int loginsPerUser;
    public long duration;
    public double newLogins;
    public boolean createIndexes;
    public double olapRate;

    public double inverseOlapRate;


    public ComsConfig( String system, Properties properties, int multiplier ) {
        super( "coms", system, properties );

        cycles = multiplier;
        mode = Mode.valueOf( system.toUpperCase() );
        threadDistribution = Arrays.stream( getStringProperty( properties, "threadDistribution" ).split( "_" ) ).map( Integer::parseInt ).collect( Collectors.toList() );

        seed = getLongProperty( properties, "seed" );

        createIndexes = getBooleanProperty( properties, "createIndexes" );

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

        allowNegative = getBooleanProperty( properties, "allowNegative" );

        graphStore = getStringProperty( properties, "graphStore" );
        docStore = getStringProperty( properties, "docStore" );
        relStore = getStringProperty( properties, "relStore" );

        removeLogs = getDoubleProperty( properties, "removeLogs" );
        addLogs = getDoubleProperty( properties, "addLogs" );

        changeDevice = getDoubleProperty( properties, "changeDevice" );
        newDevice = getDoubleProperty( properties, "newDevice" );
        removeDevice = getDoubleProperty( properties, "removeDevice" );

        removeUsers = getDoubleProperty( properties, "removeUsers" );

        readQueries = getIntProperty( properties, "readQueries" );

        newLogins = getDoubleProperty( properties, "newLogins" );

        loginsPerUser = getIntProperty( properties, "loginsPerUser" );

        duration = getIntProperty( properties, "duration" );

        olapRate = getDoubleProperty( properties, "olapRate" );

        inverseOlapRate = 1 - olapRate;
    }


    public ComsConfig( Map<String, String> parsedConfig ) {
        super( "coms", parsedConfig.get( "store" ), parsedConfig );

        cycles = Integer.parseInt( parsedConfig.get( "cycles" ) );
        mode = Mode.valueOf( this.system.toUpperCase() );
        seed = Long.parseLong( parsedConfig.get( "seed" ) );

        threadDistribution = Arrays.stream( parsedConfig.get( "threadDistribution" ).split( "_" ) ).map( Integer::parseInt ).collect( Collectors.toList() );

        createIndexes = Boolean.getBoolean( parsedConfig.get( "createIndexes" ) );

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

        allowNegative = Boolean.getBoolean( parsedConfig.get( "allowNegative" ) );

        graphStore = parsedConfig.get( "graphStore" );
        docStore = parsedConfig.get( "docStore" );
        relStore = parsedConfig.get( "relStore" );

        duration = Integer.parseInt( parsedConfig.get( "duration" ) );

        removeLogs = parsePercent( parsedConfig.get( "removeLogs" ) );
        addLogs = parsePercent( parsedConfig.get( "addLogs" ) );
        changeDevice = parsePercent( parsedConfig.get( "changeDevice" ) );
        newDevice = parsePercent( parsedConfig.get( "newDevice" ) );
        removeDevice = parsePercent( parsedConfig.get( "removeDevice" ) );

        removeUsers = Double.parseDouble( parsedConfig.get( "removeUsers" ) );
        readQueries = Integer.parseInt( parsedConfig.get( "readQueries" ) );

        newLogins = parsePercent( parsedConfig.get( "newLogins" ) );

        loginsPerUser = Integer.parseInt( parsedConfig.get( "loginsPerUser" ) );

        olapRate = parsePercent( "olapRate" );

        inverseOlapRate = 1 - olapRate;
    }


    public double parsePercent( String percent ) {
        return Integer.parseInt( percent.replace( "%", "" ).trim() ) / 100d;
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return false;
    }

}
