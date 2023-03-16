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

import java.util.Map;
import java.util.Properties;
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


    public ComsConfig( String system, Properties properties ) {
        super( "coms", system, properties );

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

    }


    public ComsConfig( Map<String, String> parsedConfig ) {
        super( "coms", parsedConfig.get( "store" ), parsedConfig );

        seed = Long.parseLong( parsedConfig.get( "seed" ) );

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
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return false;
    }

}