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

import static org.polypheny.simpleclient.scenario.coms.simulation.entites.Graph.REL_POSTFIX;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Value;
import org.jetbrains.annotations.NotNull;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator.Network;
import org.polypheny.simpleclient.scenario.coms.simulation.PropertyType.Type;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.Graph;

public class User {

    public static final String namespace = "coms";


    public static Map<String, PropertyType> userTypes = new HashMap<String, PropertyType>() {{
        put( "id", new PropertyType( 5, Type.NUMBER ) );
        put( "firstname", new PropertyType( 12, Type.CHAR ) );
        put( "lastname", new PropertyType( 12, Type.CHAR ) );
        put( "birthday", new PropertyType( 12, Type.NUMBER ) );
        put( "salary", new PropertyType( 255, Type.NUMBER ) );
    }};


    public static Map<String, PropertyType> loginTypes = new HashMap<String, PropertyType>() {{
        put( "userid", new PropertyType( 5, Type.NUMBER ) );
        put( "username", new PropertyType( 255, Type.CHAR ) );
        put( "timestamp", new PropertyType( 255, Type.CHAR ) );
        put( "duration", new PropertyType( 5, Type.FLOAT ) );
        put( "successful", new PropertyType( 5, Type.BOOLEAN ) );
    }};

    public static String userPK = "id";

    public static List<String> loginPK = Collections.singletonList( "timestamp" );

    public final long id;
    private Map<String, String> properties;


    public User( Random random ) {
        this.id = random.nextInt();
        this.properties = Network.generateFixedTypedProperties( random, userTypes );
    }


    public static List<String> getSql( List<User> users, Function<User, String> asSqlValues ) {
        return users.stream().map( asSqlValues ).collect( Collectors.toList() );
    }


    public String userToSql() {
        List<String> query = new ArrayList<>();
        for ( Entry<String, String> entry : properties.entrySet() ) {
            query.add( userTypes.get( entry.getKey() ).getType() == Type.CHAR ? "\"" + entry.getValue() + "\"" : entry.getValue() );
        }
        return String.join( ",", query );
    }


    public List<Query> getRemoveQuery() {
        String sql = "DELETE FROM ";
        String surreal = sql;

        sql += namespace + ".user" + Graph.REL_POSTFIX;
        surreal += "user" + Graph.REL_POSTFIX;

        String where = " WHERE id = " + id;
        sql += where;
        surreal += where;

        return Collections.singletonList( RawQuery.builder().sql( sql ).surrealQl( surreal ).build() );
    }


    public static Query getRelQuery( List<User> users ) {
        StringBuilder sql = new StringBuilder( "INSERT INTO " );
        StringBuilder surreal = new StringBuilder( "INSERT INTO " );
        sql.append( users.get( 0 ).getTable( true ) );
        surreal.append( users.get( 0 ).getTable( false ) );

        sql.append( " (" ).append( String.join( ",", userTypes.keySet() ) ).append( ")" );
        sql.append( " VALUES " );

        String values = users.stream().map( s -> "(" + s.userToSql() + ")" ).collect( Collectors.joining( "," ) );
        sql.append( values );

        surreal.append( " (" ).append( String.join( ",", userTypes.keySet() ) ).append( ")" );
        surreal.append( " VALUES " );
        surreal.append( values );

        return RawQuery.builder()
                .sql( sql.toString() )
                .surrealQl( surreal.toString() ).build();
    }


    @NotNull
    protected String getTable( boolean withPrefix ) {
        return (withPrefix ? namespace + REL_POSTFIX + "." : "") + "user" + REL_POSTFIX;
    }


    public List<Query> changeUser( Random random ) {
        int changedPerProperties = random.nextInt( userTypes.size() ) + 1;

        StringBuilder sql = new StringBuilder( " SET " );

        Map<String, String> updates = new HashMap<>();
        for ( int j = 0; j < changedPerProperties; j++ ) {
            String key = new ArrayList<>( userTypes.keySet() ).get( j );
            PropertyType property = userTypes.get( key );
            updates.put( key, property.getType().asString( random, 0, property.getLength(), Type.OBJECT ) );
        }

        sql.append( updates.entrySet().stream().map( u -> u.getKey() + " = " + u.getValue() ).collect( Collectors.joining( ", " ) ) );
        sql.append( " WHERE " );
        sql.append( updates.keySet().stream().map( u -> u + " = " + properties.get( u ) ).collect( Collectors.joining( " AND " ) ) );

        // update simulation
        properties.putAll( updates );

        return Collections.singletonList( RawQuery.builder()
                .sql( "UPDATE " + getTable( true ) + sql )
                .surrealQl( "UPDATE " + getTable( false ) + sql ).build() );
    }


    private List<Query> buildQueries( List<Map<String, String>> adds ) {
        StringBuilder sql = new StringBuilder();

        sql.append( " (" ).append( String.join( ",", userTypes.keySet() ) ).append( ")" );
        sql.append( " VALUES " );
        int i = 0;
        for ( Map<String, String> element : adds ) {
            if ( i != 0 ) {
                sql.append( ", " );
            }
            sql.append( "(" ).append( String.join( ", ", element.values() ) ).append( ")" );
            i++;
        }
        String label = getTable( false );
        String sqlLabel = GraphElement.namespace + REL_POSTFIX + "." + label;

        return Collections.singletonList( RawQuery.builder()
                .sql( "INSERT INTO " + sqlLabel + sql )
                .surrealQl( "INSERT INTO " + label + sql ).build() );
    }


    public List<Query> getReadAllFixed() {
        return Collections.singletonList( RawQuery.builder()
                .surrealQl( "SELECT * FROM " + getTable( false ) + " WHERE _id =" + id )
                .sql( "SELECT * FROM " + getTable( true ) + " WHERE id = " + id ).build() );
    }


    public List<Query> getReadSpecificPropFixed( Random random ) {
        String keyVal = getRandomFixed( random );
        String projects = getRandomFixedProjects( random );
        return Collections.singletonList( RawQuery.builder()
                .surrealQl( "SELECT " + projects + " FROM " + getTable( false ) + " WHERE " + keyVal )
                .sql( "SELECT " + projects + " FROM " + getTable( true ) + " WHERE " + keyVal ).build() );
    }


    public List<Query> getComplex1( Random random ) {
        String keyVal = getRandomFixed( random );
        String projects = getRandomFixedProjects( random );
        return Collections.singletonList( RawQuery.builder()
                .surrealQl( "SELECT " + projects + " FROM " + getTable( false ) + " WHERE " + keyVal )
                .sql( "SELECT " + projects + " FROM " + getTable( true ) + " WHERE " + keyVal ).build() );
    }


    private String getRandomFixedProjects( Random random ) {
        if ( random.nextBoolean() ) {
            return "*";
        }

        List<String> keys = new ArrayList<>( userTypes.keySet() );
        int amount = random.nextInt( keys.size() ) + 1;

        List<String> projects = new ArrayList<>();

        for ( int i = 0; i < amount; i++ ) {
            projects.add( keys.remove( random.nextInt( keys.size() ) ) );
        }
        return String.join( ", ", projects );
    }


    private String getRandomFixed( Random random ) {
        List<String> keys = new ArrayList<>( properties.keySet() );
        int randomKey = random.nextInt( keys.size() );
        String value = properties.get( keys.get( randomKey ) );

        return keys.get( randomKey ) + " = " + value;
    }


    public List<Query> getInsertQuery( Random random ) {
        int newProps = random.nextInt( 5 ) + 1;

        List<Map<String, String>> adds = new ArrayList<>();

        for ( int i = 0; i < newProps; i++ ) {
            adds.add( Network.generateFixedTypedProperties( random, userTypes ) );
        }
        return buildQueries( adds );
    }


    @Value
    public static class Login {

        long userId;
        long deviceId;

        String timestamp;

        long duration;

        boolean successful;


        public static Login createRandomLogin( long userid, long deviceId, Network network ) {
            boolean successful = network.getRandom().nextFloat() < 0.8;
            return new Login( userid, deviceId, network.createRandomTimestamp(), network.getRandom().nextInt( 24 ), successful );
        }


        public Map<String, String> asStrings() {
            Map<String, String> values = new HashMap<>();
            for ( Entry<String, PropertyType> entry : User.loginTypes.entrySet() ) {
                switch ( entry.getKey() ) {
                    case "userId":
                        values.put( entry.getKey(), String.valueOf( getUserId() ) );
                        break;
                    case "deviceId":
                        values.put( entry.getKey(), String.valueOf( getDeviceId() ) );
                        break;
                    case "timestamp":
                        values.put( entry.getKey(), getTimestamp() );
                        break;
                    case "duration":
                        values.put( entry.getKey(), String.valueOf( getDuration() ) );
                        break;
                    case "successful":
                        values.put( entry.getKey(), String.valueOf( successful ) );
                        break;
                    default:
                        throw new RuntimeException();
                }
            }
            return values;
        }

    }

}
