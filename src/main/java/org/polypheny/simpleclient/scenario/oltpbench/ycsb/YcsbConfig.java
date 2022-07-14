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

package org.polypheny.simpleclient.scenario.oltpbench.ycsb;

import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import lombok.SneakyThrows;
import org.polypheny.simpleclient.scenario.oltpbench.AbstractOltpBenchConfig;
import org.w3c.dom.Document;
import org.w3c.dom.Element;


public class YcsbConfig extends AbstractOltpBenchConfig {

    public long time;
    public String rate;
    public int batchSize;

    public int readRecordWeight;
    public int insertRecordWeight;
    public int scanRecordWeight;
    public int updateRecordWeight;
    public int deleteRecordWeight;
    public int readModifyWriteRecordWeight;

    public boolean partitionTable;


    public YcsbConfig( Properties properties, int multiplier ) {
        super( properties, multiplier, "ycsb", "oltpbench-polypheny" );

        time = getLongProperty( properties, "time" );
        rate = getStringProperty( properties, "rate" );
        batchSize = 128;

        readRecordWeight = getIntProperty( properties, "readRecordWeight" );
        insertRecordWeight = getIntProperty( properties, "insertRecordWeight" );
        scanRecordWeight = getIntProperty( properties, "scanRecordWeight" );
        updateRecordWeight = getIntProperty( properties, "updateRecordWeight" );
        deleteRecordWeight = getIntProperty( properties, "deleteRecordWeight" );
        readModifyWriteRecordWeight = getIntProperty( properties, "readModifyWriteRecordWeight" );

        partitionTable = getBooleanProperty( properties, "partitionTable" );
    }


    public YcsbConfig( Map<String, String> cdl ) {
        super( cdl, "ycsb", cdl.get( "store" ) );

        time = Long.parseLong( cdl.get( "time" ) );
        rate = cdl.get( "rate" );
        if ( dataStores.size() > 1 && dataStores.contains( "neo4j" ) ) {
            batchSize = 1;
        } else {
            batchSize = 128;
        }

        readRecordWeight = Integer.parseInt( cdl.get( "readRecordWeight" ) );
        insertRecordWeight = Integer.parseInt( cdl.get( "insertRecordWeight" ) );
        scanRecordWeight = Integer.parseInt( cdl.get( "scanRecordWeight" ) );
        updateRecordWeight = Integer.parseInt( cdl.get( "updateRecordWeight" ) );
        deleteRecordWeight = Integer.parseInt( cdl.get( "deleteRecordWeight" ) );
        readModifyWriteRecordWeight = Integer.parseInt( cdl.get( "readModifyWriteRecordWeight" ) );

        partitionTable = Boolean.parseBoolean( cdl.get( "partitionTable" ) );
    }


    @SneakyThrows
    @Override
    public String toXml( String dbType, String driver, String dbUrl, String username, String password, String ddlFileName, String dialectFileName, String isolation ) {
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

        // root elements
        Document doc = docBuilder.newDocument();
        doc.setXmlStandalone( true );
        Element rootElement = doc.createElement( "parameters" );
        doc.appendChild( rootElement );

        // Executor specific elements
        createAndAppendElement( doc, rootElement, "dbtype", dbType );
        createAndAppendElement( doc, rootElement, "driver", driver );
        createAndAppendElement( doc, rootElement, "DBUrl", dbUrl );
        createAndAppendElement( doc, rootElement, "username", username );
        createAndAppendElement( doc, rootElement, "password", password );
        createAndAppendElement( doc, rootElement, "ddlFileName", ddlFileName );
        //createAndAppendElement( doc, rootElement, "dialectFileName", ddlFileName );
        createAndAppendElement( doc, rootElement, "isolation", isolation );
        createAndAppendElement( doc, rootElement, "batchsize", batchSize + "" );

        // General OLTPbench elements
        createAndAppendElement( doc, rootElement, "scalefactor", scaleFactor + "" );
        createAndAppendElement( doc, rootElement, "terminals", numberOfThreads + "" );

        // -- Scenario specific elements --

        // Works
        Element worksElement = doc.createElement( "works" );
        rootElement.appendChild( worksElement );
        Element workElement = doc.createElement( "work" );
        worksElement.appendChild( workElement );
        createAndAppendElement( doc, workElement, "serial", "false" );
        createAndAppendElement( doc, workElement, "time", time + "" );
        createAndAppendElement( doc, workElement, "rate", rate );
        createAndAppendElement( doc, workElement, "warmup", warmupTime + "" );
        createAndAppendElement( doc, workElement, "weights",
                readRecordWeight + "," +
                        insertRecordWeight + "," +
                        scanRecordWeight + "," +
                        updateRecordWeight + "," +
                        deleteRecordWeight + "," +
                        readModifyWriteRecordWeight
        );

        // Transaction types
        Element transactionTypesElement = doc.createElement( "transactiontypes" );
        rootElement.appendChild( transactionTypesElement );
        createAndAppendTransactionTypeElement( doc, transactionTypesElement, "ReadRecord" );
        createAndAppendTransactionTypeElement( doc, transactionTypesElement, "InsertRecord" );
        createAndAppendTransactionTypeElement( doc, transactionTypesElement, "ScanRecord" );
        createAndAppendTransactionTypeElement( doc, transactionTypesElement, "UpdateRecord" );
        createAndAppendTransactionTypeElement( doc, transactionTypesElement, "DeleteRecord" );
        createAndAppendTransactionTypeElement( doc, transactionTypesElement, "ReadModifyWriteRecord" );

        StringWriter sw = new StringWriter();
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty( OutputKeys.OMIT_XML_DECLARATION, "no" );
        transformer.setOutputProperty( OutputKeys.METHOD, "xml" );
        transformer.setOutputProperty( OutputKeys.INDENT, "yes" );
        transformer.setOutputProperty( OutputKeys.ENCODING, "UTF-8" );

        transformer.transform( new DOMSource( doc ), new StreamResult( sw ) );
        return sw.toString();
    }


}
