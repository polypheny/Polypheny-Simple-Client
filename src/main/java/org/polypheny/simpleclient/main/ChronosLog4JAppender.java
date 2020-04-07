package org.polypheny.simpleclient.main;

import ch.unibas.dmi.dbis.chronos.agent.ChronosHttpClient.ChronosLogHandler;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

@Plugin(
        name = "ChronosAppender",
        category = Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE)
public class ChronosLog4JAppender extends AbstractAppender {

    private static volatile ChronosLogHandler chronosLogHandler;


    protected ChronosLog4JAppender( String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions, Property[] properties ) {
        super( name, filter, layout, ignoreExceptions, properties );
    }


    @PluginFactory
    public static ChronosLog4JAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("ignoreExceptions") boolean ignoreExceptions,
            @PluginElement("Layout") Layout layout,
            @PluginElement("Filters") Filter filter ) {

        if ( name == null ) {
            LOGGER.error( "No name provided for ChronosLog4JAppender" );
            return null;
        }
        if ( layout == null ) {
            layout = PatternLayout.createDefaultLayout();
        }
        return new ChronosLog4JAppender( name, filter, layout, ignoreExceptions, null );
    }


    public static void setChronosLogHandler( ChronosLogHandler chronosLogHandler ) {
        ChronosLog4JAppender.chronosLogHandler = chronosLogHandler;
    }


    @Override
    public void append( LogEvent event ) {
        if ( chronosLogHandler != null ) {
            String prefix = "";
            if ( event.getLoggerName().equals( "CONTROL_MESSAGES_LOGGER" ) ) {
                prefix = "CONTROL > ";
            } else if ( event.getLoggerName().startsWith( "org.polypheny.jdbc.Driver" ) ) {
                prefix = "DRIVER > ";
            } else if ( event.getLoggerName().startsWith( "ch.unibas.dmi.dbis.chronos.agent" ) ) {
                prefix = "CHRONOS > ";
            }
            chronosLogHandler.publish( prefix + event.getLevel().name() + " : " + event.getMessage().getFormattedMessage() + "\n " );
            if ( event.getThrown() != null ) {
                StringWriter sw = new StringWriter();
                event.getThrown().printStackTrace( new PrintWriter( sw ) );
                chronosLogHandler.publish( sw.toString() );
            }
            if ( event.isEndOfBatch() ) {
                try {
                    chronosLogHandler.flush();
                } catch ( InterruptedException e ) {
                    LOGGER.warn( "Error while flushing chronos log handler", e );
                }
            }
        }
    }


}
