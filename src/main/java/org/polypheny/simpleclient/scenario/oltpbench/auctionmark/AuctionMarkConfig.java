package org.polypheny.simpleclient.scenario.oltpbench.auctionmark;

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

public class AuctionMarkConfig extends AbstractOltpBenchConfig {


    public long time;
    public String rate;

    public int getItemWeight;
    public int getUserInfoWeight;
    public int newBidWeight;
    public int newCommentWeight;
    public int newCommentResponseWeight;
    public int newFeedbackWeight;
    public int newItemWeight;
    public int newPurchaseWeight;
    public int updateItemWeight;


    public AuctionMarkConfig( Properties properties, int multiplier ) {
        super( properties, multiplier, "auctionmark", "oltpbench-polypheny" );

        time = getLongProperty( properties, "time" );
        rate = getStringProperty( properties, "rate" );

        getItemWeight = getIntProperty( properties, "getItemWeight" );
        getUserInfoWeight = getIntProperty( properties, "getUserInfoWeight" );
        newBidWeight = getIntProperty( properties, "newBidWeight" );
        newCommentWeight = getIntProperty( properties, "newCommentWeight" );
        newCommentResponseWeight = getIntProperty( properties, "newCommentResponseWeight" );
        newFeedbackWeight = getIntProperty( properties, "newFeedbackWeight" );
        newItemWeight = getIntProperty( properties, "newItemWeight" );
        newPurchaseWeight = getIntProperty( properties, "newPurchaseWeight" );
        updateItemWeight = getIntProperty( properties, "updateItemWeight" );
    }


    public AuctionMarkConfig( Map<String, String> cdl  ) {
        super( cdl, "auctionmark", cdl.get( "store" ) );

        time = Long.parseLong( cdl.get( "time" ) );
        rate = cdl.get( "rate" );

        getItemWeight = Integer.parseInt( cdl.get( "getItemWeight" ) );
        getUserInfoWeight = Integer.parseInt( cdl.get( "getUserInfoWeight" ) );
        newBidWeight = Integer.parseInt( cdl.get( "newBidWeight" ) );
        newCommentWeight = Integer.parseInt( cdl.get( "newCommentWeight" ) );
        newCommentResponseWeight = Integer.parseInt( cdl.get( "newCommentResponseWeight" ) );
        newFeedbackWeight = Integer.parseInt( cdl.get( "newFeedbackWeight" ) );
        newItemWeight = Integer.parseInt( cdl.get( "newItemWeight" ) );
        newPurchaseWeight = Integer.parseInt( cdl.get( "newPurchaseWeight" ) );
        updateItemWeight = Integer.parseInt( cdl.get( "updateItemWeight" ) );
    }


    @SneakyThrows
    @Override
    public String toXml( String dbType, String driver, String dbUrl, String username, String password, String ddlFileName, String dialectFileName, String isolation ) {
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

        // root elements
        Document doc = docBuilder.newDocument();
        doc.setXmlStandalone(true);
        Element rootElement = doc.createElement( "parameters" );
        doc.appendChild( rootElement );

        // Executor specific elements
        createAndAppendElement( doc, rootElement, "dbtype", dbType );
        createAndAppendElement( doc, rootElement, "driver", driver );
        createAndAppendElement( doc, rootElement, "DBUrl", dbUrl );
        createAndAppendElement( doc, rootElement, "username", username );
        createAndAppendElement( doc, rootElement, "password", password );
        createAndAppendElement( doc, rootElement, "ddlFileName", ddlFileName );
        createAndAppendElement( doc, rootElement, "dialectFileName", ddlFileName );
        createAndAppendElement( doc, rootElement, "isolation", isolation );

        // General OLTPbench elements
        createAndAppendElement( doc, rootElement, "scalefactor", scaleFactor + "" );
        createAndAppendElement( doc, rootElement, "terminals", numberOfThreads + "" );

        // -- Scenario specific elements --

        // Works
        Element worksElement = doc.createElement( "works" );
        rootElement.appendChild( worksElement );
        Element workElement = doc.createElement( "work" );
        worksElement.appendChild( workElement );
        createAndAppendElement( doc, workElement, "time", time + "" );
        createAndAppendElement( doc, workElement, "rate", rate );
        createAndAppendElement( doc, workElement, "warmup", warmupTime + "" );
        createAndAppendElement( doc, workElement, "weights",
                getItemWeight + "," +
                        getUserInfoWeight + "," +
                        newBidWeight + "," +
                        newCommentWeight + "," +
                        newCommentResponseWeight + "," +
                        newFeedbackWeight + "," +
                        newItemWeight + "," +
                        newPurchaseWeight + "," +
                        updateItemWeight
        );

        // Transaction types
        Element transactionTypesElement = doc.createElement( "transactiontypes" );
        rootElement.appendChild( transactionTypesElement );
        createAndAppendTransactionTypeElement(doc, transactionTypesElement, "GetItem" );
        createAndAppendTransactionTypeElement(doc, transactionTypesElement, "GetUserInfo" );
        createAndAppendTransactionTypeElement(doc, transactionTypesElement, "NewBid" );
        createAndAppendTransactionTypeElement(doc, transactionTypesElement, "NewComment" );
        createAndAppendTransactionTypeElement(doc, transactionTypesElement, "NewCommentResponse" );
        createAndAppendTransactionTypeElement(doc, transactionTypesElement, "NewFeedback" );
        createAndAppendTransactionTypeElement(doc, transactionTypesElement, "NewItem" );
        createAndAppendTransactionTypeElement(doc, transactionTypesElement, "NewPurchase" );
        createAndAppendTransactionTypeElement(doc, transactionTypesElement, "UpdateItem" );

        StringWriter sw = new StringWriter();
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty( OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.setOutputProperty(OutputKeys.METHOD, "xml");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");

        transformer.transform(new DOMSource(doc), new StreamResult(sw));
        return sw.toString();
    }

}
