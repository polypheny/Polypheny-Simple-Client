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
 */

package org.polypheny.simpleclient.scenario.oltpbench;

import java.util.Map;
import java.util.Properties;
import org.polypheny.simpleclient.scenario.AbstractConfig;
import org.w3c.dom.Document;
import org.w3c.dom.Element;


public abstract class AbstractOltpBenchConfig extends AbstractConfig {

    //public int batchSize;
    public int scaleFactor;
    public int warmupTime;


    public AbstractOltpBenchConfig( Properties properties, int multiplier, String scenario, String system ) {
        super( scenario, system, properties );

        //batchSize = getIntProperty( properties, "batchSize"); // 128
        scaleFactor = multiplier;
        warmupTime = 0;
        //loaderThreads
    }


    public AbstractOltpBenchConfig( Map<String, String> cdl, String scenario, String system ) {
        super( scenario, system, updateCdl( cdl ) );

        warmupTime = Integer.parseInt( cdl.get( "warmupTime" ) );
        scaleFactor = Integer.parseInt( cdl.get( "scaleFactor" ) );
    }


    private static Map<String, String> updateCdl( Map<String, String> cdl ) {
        if ( !cdl.containsKey( "numberOfWarmUpIterations" ) ) {
            cdl.put( "numberOfWarmUpIterations", "1" );
        }
        return cdl;
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


    protected void createAndAppendElement( Document doc, Element parent, String tag, String content ) {
        Element element = doc.createElement( tag );
        element.setTextContent( content );
        parent.appendChild( element );
    }


    protected void createAndAppendTransactionTypeElement( Document doc, Element parent, String name ) {
        Element element = doc.createElement( "transactiontype" );
        parent.appendChild( element );
        Element nameElement = doc.createElement( "name" );
        nameElement.setTextContent( name );
        element.appendChild( nameElement );
    }

}
