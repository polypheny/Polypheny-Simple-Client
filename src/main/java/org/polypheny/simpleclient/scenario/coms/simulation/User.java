/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/28/23, 11:14 AM The Polypheny Project
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator.Device;
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator.Network;
import org.polypheny.simpleclient.scenario.coms.simulation.PropertyType.Type;

public class User {

    private final Random random;

    public static Map<String, PropertyType> types = new HashMap<String, PropertyType>() {{
        put( "id", new PropertyType( 5, Type.NUMBER ) );
        put( "firstname", new PropertyType( 12, Type.CHAR ) );
        put( "lastname", new PropertyType( 12, Type.CHAR ) );
        put( "birthday", new PropertyType( 12, Type.NUMBER ) );
        put( "salary", new PropertyType( 255, Type.NUMBER ) );
    }};

    final long id;
    private Map<String, String> properties;


    public User( Random random ) {
        super();
        this.random = random;
        this.id = random.nextInt();
        this.properties = Network.generateFixedTypedProperties( random, types );
    }


    public static List<String> getSql( List<User> users ) {
        List<String> queries = new ArrayList<>();
        for ( User user : users ) {
            List<String> query = new ArrayList<>();
            for ( Entry<String, String> entry : user.properties.entrySet() ) {
                query.add( entry.getValue() );
            }
            queries.add( String.join( ",", query ) );
        }

        return queries;
    }

    public List<Query> getRemoveQuery() {
        String sql = "DELETE FROM ";
        String surreal = sql;

        String where = " WHERE ";

    }


    public Query getGraphQuery() {
        return null;
    }



}
