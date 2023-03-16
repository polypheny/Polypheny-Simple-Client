/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/13/23, 10:52 AM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.coms.simulation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator.Network;

@Value
@NonFinal
public abstract class GraphElement {

    public long id = Network.idBuilder.getAndIncrement();

    public Map<String, String> fixedProperties;

    public Map<String, String> dynProperties;
    public Map<String, PropertyType> types;


    public List<String> getLabels() {
        return Collections.singletonList( getClass().getSimpleName() );
    }


    public GraphElement( Map<String, PropertyType> types, Map<String, String> fixedProperties, Map<String, String> dynProperties ) {
        this.fixedProperties = fixedProperties;
        this.dynProperties = dynProperties;
        this.types = types;
    }


    public String asMongo() {
        List<String> query = new ArrayList<>();
        for ( Entry<String, String> entry : dynProperties.entrySet() ) {
            query.add( entry.getKey() + ":" + entry.getValue() );
        }
        return String.join( ",", query );
    }


    public String asDynSurreal() {
        List<String> query = new ArrayList<>();
        for ( Entry<String, String> entry : dynProperties.entrySet() ) {
            query.add( entry.getKey() + ":" + entry.getValue() );
        }
        return String.join( ",", query );
    }


    public String asSql() {
        List<String> query = new ArrayList<>();
        for ( Entry<String, String> entry : fixedProperties.entrySet() ) {
            query.add( entry.getValue() );
        }
        return String.join( ",", query );
    }

}
