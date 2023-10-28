/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-28.10.23, 15:49 The Polypheny Project
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

package org.polypheny.simpleclient.executor;

import lombok.Getter;
import org.polypheny.simpleclient.scenario.AbstractConfig;

public class PolyphenyVersionSwitch {

    @Getter
    private static PolyphenyVersionSwitch instance;

    public final int uiPort;
    public final boolean hasIcarusRoutingSettings;
    public final boolean useNewDeploySyntax;
    public final boolean useNewAdapterDeployParameters;


    public static void initialize( AbstractConfig config ) {
        instance = new PolyphenyVersionSwitch( config );
    }


    private PolyphenyVersionSwitch( AbstractConfig config ) {
        uiPort = config.pdbBranch.equals( "refactor" ) ? 7659 : 8080;
        hasIcarusRoutingSettings = config.pdbBranch.equalsIgnoreCase( "old-routing" );
        useNewDeploySyntax = !hasIcarusRoutingSettings;
        useNewAdapterDeployParameters = true;
    }

}
