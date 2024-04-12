/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-12/04/2024, 09:30 The Polypheny Project
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import org.polypheny.simpleclient.executor.PolyphenyVersionSwitch;

public class CustomClassLoader extends ClassLoader {

    private static final String OLD_DRIVER_JAR_PATH = "/libs/polyphenyJdbcDrivers/old-driver.zip";
    private static final String NEW_DRIVER_JAR_PATH = "/libs/polyphenyJdbcDrivers/new-driver.zip";

    private URLClassLoader driverClassLoader;


    public CustomClassLoader( ClassLoader parent ) {
        super( parent );
    }


    @Override
    protected Class<?> findClass( String name ) throws ClassNotFoundException {
        if ( name.startsWith( "org.polypheny.jdbc." ) ) {
            if ( driverClassLoader == null ) {
                try {
                    driverClassLoader = createClassLoader();
                } catch ( IOException e ) {
                    throw new RuntimeException( e );
                }
            }
            return driverClassLoader.loadClass( name );
        }
        return super.findClass( name );
    }


    private URLClassLoader createClassLoader() throws IOException {
        String path = PolyphenyVersionSwitch.getInstance().usePrismJdbcDriver ? NEW_DRIVER_JAR_PATH : OLD_DRIVER_JAR_PATH;
        try ( InputStream is = CustomClassLoader.class.getResourceAsStream( path ) ) {
            if ( is != null ) {
                // Temporary file to copy the JAR out of the resource
                File tempFile = File.createTempFile( "driver", ".jar" );
                tempFile.deleteOnExit();
                try ( FileOutputStream out = new FileOutputStream( tempFile ) ) {
                    // Copy the data from the resource to the temporary file
                    byte[] buffer = new byte[1024];
                    int bytesRead;
                    while ( (bytesRead = is.read( buffer )) != -1 ) {
                        out.write( buffer, 0, bytesRead );
                    }
                }
                // URLClassLoader with the temporary file
                return new URLClassLoader( new URL[]{ tempFile.toURI().toURL() } );
            } else {
                throw new FileNotFoundException( "Could not find driver jar file " + path );
            }
        }
    }

}
