/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-13.06.22, 13:51 The Polypheny Project
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

package org.polypheny.simpleclient.scenario.docbench;


import java.util.Map;
import java.util.Properties;
import org.polypheny.simpleclient.scenario.AbstractConfig;

public class DocBenchConfig extends AbstractConfig {

    public long seed;

    public int batchSize;

    // Data
    public int numberOfDocuments;
    public int maxNumberOfAttributes;
    public int minNumberOfAttributes;
    public int sizeOfAttributesPool;
    public int sizeOfValuesPool;
    public int valuesStringMaxLength;
    public int valuesStringMinLength;

    // Workload
    public int numberOfFindQueries;
    public int numberOfUpdateQueries;
    public int numberOfPutQueries;


    public DocBenchConfig( Properties properties, int multiplier ) {
        super( "docbench", "polypheny-mongoql", properties );

        seed = getLongProperty( properties, "seed" );
        batchSize = getIntProperty( properties, "batchSize" );

        numberOfFindQueries = getIntProperty( properties, "numberOfFindQueries" );
        numberOfUpdateQueries = getIntProperty( properties, "numberOfUpdateQueries" );
        numberOfPutQueries = getIntProperty( properties, "numberOfPutQueries" );

        numberOfDocuments = getIntProperty( properties, "numberOfDocuments" );
        minNumberOfAttributes = getIntProperty( properties, "minNumberOfAttributes" );
        maxNumberOfAttributes = getIntProperty( properties, "maxNumberOfAttributes" );
        sizeOfAttributesPool = getIntProperty( properties, "sizeOfAttributesPool" );
        sizeOfValuesPool = getIntProperty( properties, "sizeOfValuesPool" );
        valuesStringMinLength = getIntProperty( properties, "valuesStringMinLength" );
        valuesStringMaxLength = getIntProperty( properties, "valuesStringMaxLength" );
    }


    public DocBenchConfig( Map<String, String> cdl ) {
        super( "docbench", cdl.get( "store" ), cdl );

        seed = Integer.parseInt( cdl.get( "seed" ) );
        batchSize = Integer.parseInt( cdl.get( "batchSize" ) );

        numberOfFindQueries = Integer.parseInt( cdl.get( "numberOfFindQueries" ) );
        numberOfUpdateQueries = Integer.parseInt( cdl.get( "numberOfUpdateQueries" ) );
        numberOfPutQueries = Integer.parseInt( cdl.get( "numberOfPutQueries" ) );

        numberOfDocuments = Integer.parseInt( cdl.get( "numberOfDocuments" ) );
        minNumberOfAttributes = Integer.parseInt( cdl.get( "minNumberOfAttributes" ) );
        maxNumberOfAttributes = Integer.parseInt( cdl.get( "maxNumberOfAttributes" ) );
        sizeOfAttributesPool = Integer.parseInt( cdl.get( "sizeOfAttributesPool" ) );
        sizeOfValuesPool = Integer.parseInt( cdl.get( "sizeOfValuesPool" ) );
        valuesStringMinLength = Integer.parseInt( cdl.get( "valuesStringMinLength" ) );
        valuesStringMaxLength = Integer.parseInt( cdl.get( "valuesStringMaxLength" ) );
    }


    // For MultiBench
    protected DocBenchConfig( String scenario, String system, Map<String, String> cdl ) {
        super( scenario, system, cdl );
    }


    // For MultiBench
    protected DocBenchConfig( String scenario, String system, Properties properties ) {
        super( scenario, system, properties );
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return false;
    }

}
