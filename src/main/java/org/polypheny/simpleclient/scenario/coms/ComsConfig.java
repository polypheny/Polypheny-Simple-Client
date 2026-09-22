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
    public double addUsers;
    public int readQueries;
    public int loginsPerUser;

    public int users;
    public int servers;
    public int clients;
    public long duration;
    public double newLogins;
    public boolean createIndexes;
    public double olapRate;

    public double inverseOlapRate;


    public ComsConfig( Map<String, String> parsedConfig, int multiplier ) {
        super( "coms", parsedConfig.get( "store" ), parsedConfig );

        cycles = multiplier;
        mode = Mode.valueOf( this.system.toUpperCase() );
        seed = Long.parseLong( parsedConfig.get( "seed" ) );

        threadDistribution = Arrays.stream( parsedConfig.get( "threadDistribution" ).split( "_" ) ).map( Integer::parseInt ).toList();

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

        removeLogs = Double.parseDouble( parsedConfig.get( "removeLogs" ) );
        addLogs = Double.parseDouble( parsedConfig.get( "addLogs" ) );
        changeDevice = Double.parseDouble( parsedConfig.get( "changeDevice" ) );
        newDevice = Double.parseDouble( parsedConfig.get( "newDevice" ) );
        removeDevice = Double.parseDouble( parsedConfig.get( "removeDevice" ) );

        removeUsers = Double.parseDouble( parsedConfig.get( "removeUsers" ) );
        addUsers = Double.parseDouble( parsedConfig.get( "addUsers" ) );

        readQueries = Integer.parseInt( parsedConfig.get( "readQueries" ) );

        newLogins = Double.parseDouble( parsedConfig.get( "newLogins" ) );

        loginsPerUser = Integer.parseInt( parsedConfig.get( "loginsPerUser" ) );
        users = Integer.parseInt( parsedConfig.get( "users" ) );
        servers = Integer.parseInt( parsedConfig.get( "servers" ) );
        clients = Integer.parseInt( parsedConfig.get( "clients" ) );

        olapRate = Double.parseDouble( parsedConfig.get( "olapRate" ) );

        inverseOlapRate = 1 - olapRate;
    }


    public ComsConfig( Map<String, String> parsedConfig ) {
        this( parsedConfig, Integer.parseInt( parsedConfig.get( "cycles" ) ) );
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return false;
    }

}
