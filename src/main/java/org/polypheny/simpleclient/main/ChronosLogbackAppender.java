/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 The Polypheny Project
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

package org.polypheny.simpleclient.main;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.AppenderBase;
import ch.unibas.dmi.dbis.chronos.agent.ChronosHttpClient.ChronosLogHandler;


public class ChronosLogbackAppender extends AppenderBase<ILoggingEvent> {

    private static final String ANSI_RESET = "\u001B[0m";

    private static volatile ChronosLogHandler chronosLogHandler;


    public static void setChronosLogHandler( ChronosLogHandler chronosLogHandler ) {
        ChronosLogbackAppender.chronosLogHandler = chronosLogHandler;
    }


    @Override
    protected void append( ILoggingEvent event ) {
        ChronosLogHandler logHandler = chronosLogHandler;
        if ( logHandler == null ) {
            return;
        }

        String prefix = "";
        String loggerName = event.getLoggerName();
        if ( "CONTROL_MESSAGES_LOGGER".equals( loggerName ) ) {
            prefix = "CONTROL > ";
        } else if ( loggerName != null && loggerName.startsWith( "org.polypheny.jdbc.Driver" ) ) {
            prefix = "DRIVER > ";
        } else if ( loggerName != null && loggerName.startsWith( "ch.unibas.dmi.dbis.chronos.agent" ) ) {
            prefix = "CHRONOS > ";
        }

        logHandler.publish( ANSI_RESET + prefix + event.getLevel().toString() + " : " + event.getFormattedMessage() + "\n " );
        if ( event.getThrowableProxy() != null ) {
            logHandler.publish( ThrowableProxyUtil.asString( event.getThrowableProxy() ) );
        }

        // ChronosLogHandler queues writes asynchronously; flush waits for them to complete.
        // Logback does not expose Log4j's end-of-batch flag, so flush this event here.
        try {
            logHandler.flush();
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            addError( "Interrupted while flushing Chronos log handler", e );
        }
    }

}
