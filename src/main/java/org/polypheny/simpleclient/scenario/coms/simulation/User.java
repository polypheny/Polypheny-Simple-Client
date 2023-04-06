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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Value;
import org.jetbrains.annotations.NotNull;
import org.polypheny.simpleclient.cli.Mode;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.coms.QueryTypes;
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator.Network;
import org.polypheny.simpleclient.scenario.coms.simulation.PropertyType.Type;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.Graph;

public class User {

    public static final String namespace = "coms";
    public static final AtomicLong idBuilder = new AtomicLong();


    public static Map<String, PropertyType> userTypes = new HashMap<>() {{
        put( "id", new PropertyType( 5, Type.NUMBER ) );
        put( "firstname", new PropertyType( 12, Type.CHAR ) );
        put( "lastname", new PropertyType( 12, Type.CHAR ) );
        put( "birthday", new PropertyType( 12, Type.NUMBER ) );
        put( "salary", new PropertyType( 255, Type.NUMBER ) );
    }};


    public static Map<String, PropertyType> loginTypes = new HashMap<>() {{
        put( "userid", new PropertyType( 5, Type.NUMBER ) );
        put( "accesstime", new PropertyType( 5, Type.TIMESTAMP ) );
        put( "deviceid", new PropertyType( 5, Type.NUMBER ) );
        put( "duration", new PropertyType( 5, Type.FLOAT ) );
        put( "successful", new PropertyType( 5, Type.BOOLEAN ) );
    }};

    public static String userPK = "id";

    public static List<String> loginPK = Collections.singletonList( "accesstime" );

    public final long id;
    private Map<String, String> properties;


    public User( Random random ) {
        this.id = idBuilder.getAndIncrement();
        this.properties = Network.generateFixedTypedProperties( random, userTypes );
        this.properties.put( "id", String.valueOf( id ) );
    }


    public static List<String> getSql( List<User> users, Function<User, String> asSqlValues ) {
        return users.stream().map( asSqlValues ).collect( Collectors.toList() );
    }


    public String userToSql() {
        List<String> query = new ArrayList<>();
        for ( String key : userTypes.keySet() ) {
            query.add( properties.get( key ) );
        }
        return String.join( ",", query );
    }


    public List<Query> getRemoveQuery() {
        String sql = "DELETE FROM ";
        String surreal = sql;

        sql += namespace + REL_POSTFIX + ".user" + Graph.REL_POSTFIX;
        surreal += "user" + Graph.REL_POSTFIX;

        String where = " WHERE id = " + id;
        sql += where;
        surreal += where;

        return Collections.singletonList( RawQuery.builder()
                .sql( sql )
                .surrealQl( surreal )
                .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.RELATIONAL ) )
                .build() );
    }


    @NotNull
    protected String getUserTable( boolean withPrefix ) {
        return (withPrefix ? namespace + REL_POSTFIX + "." : "") + "user" + REL_POSTFIX;
    }


    @NotNull
    protected String getLoginTable( boolean withPrefix ) {
        return (withPrefix ? namespace + REL_POSTFIX + "." : "") + "login" + REL_POSTFIX;
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
                .sql( "UPDATE " + getUserTable( true ) + sql )
                .surrealQl( "UPDATE " + getUserTable( false ) + sql )
                .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.CHANGE_USER, QueryTypes.RELATIONAL ) )
                .build() );
    }


    private List<Query> buildQueries( Map<String, String> elements ) {
        StringBuilder sql = new StringBuilder();

        sql.append( " (" ).append( String.join( ",", userTypes.keySet() ) ).append( ")" );
        sql.append( " VALUES " );
        sql.append( "(" ).append( String.join( ", ", elements.values() ) ).append( ")" );
        String label = getUserTable( false );
        String sqlLabel = GraphElement.namespace + REL_POSTFIX + "." + label;

        return Collections.singletonList( RawQuery.builder()
                .sql( "INSERT INTO " + sqlLabel + sql )
                .surrealQl( "INSERT INTO " + label + sql )
                .types( Arrays.asList( QueryTypes.MODIFY, QueryTypes.RELATIONAL ) )
                .build() );
    }


    public List<Query> getReadAllFixed() {
        return Collections.singletonList( RawQuery.builder()
                .surrealQl( "SELECT * FROM " + getUserTable( false ) + " WHERE _id =" + id )
                .sql( "SELECT * FROM " + getUserTable( true ) + " WHERE id = " + id )
                .types( Arrays.asList( QueryTypes.QUERY, QueryTypes.RELATIONAL ) )
                .build() );
    }


    public List<Query> getReadSpecificPropFixed( Random random ) {
        String keyVal = getRandomFixed( random );
        String projects = getRandomFixedProjects( random );
        return Collections.singletonList( RawQuery.builder()
                .surrealQl( "SELECT " + projects + " FROM " + getUserTable( false ) + " WHERE " + keyVal )
                .sql( "SELECT " + projects + " FROM " + getUserTable( true ) + " WHERE " + keyVal )
                .types( Arrays.asList( QueryTypes.QUERY, QueryTypes.RELATIONAL ) )
                .build() );
    }


    /**
     * Successful logins by user and by month
     */
    public List<Query> getComplex1() {
        String sql = "SELECT \n"
                + "    userid, \n"
                + "    YEAR(accesstime) AS Years, \n"
                + "    MONTH(accesstime) AS Months, \n"
                + "    COUNT(*) AS Successful_Logins\n"
                + "FROM \n"
                + "    %s\n"
                + "WHERE \n"
                + "    successful = true\n"
                + "GROUP BY \n"
                + "    userid, \n"
                + "    YEAR(accesstime), \n"
                + "    MONTH(accesstime)";

        String surreal = "SELECT \n"
                + "    userid, \n"
                + "    time::year(accesstime) AS years, \n"
                + "    time::month(accesstime) AS months, \n"
                + "    count() AS Successful_Logins\n"
                + "FROM \n"
                + "    %s\n"
                + "WHERE \n"
                + "    successful = true\n"
                + "GROUP BY \n"
                + "    userid, \n"
                + "    years, \n"
                + "    months";

        return Collections.singletonList( RawQuery.builder()
                .surrealQl( String.format( surreal, getLoginTable( false ) ) )
                .sql( String.format( sql, getLoginTable( true ) ) )
                .types( Arrays.asList( QueryTypes.COMPLEX_LOGIN_1, QueryTypes.RELATIONAL ) )
                .build() );
    }


    public List<Query> getComplex2() {
        String sql = "SELECT \n"
                + "    userid, \n"
                + "    HOUR(accesstime) AS hours, \n"
                + "    AVG(duration) AS avgDuration\n"
                + "FROM \n"
                + "    %s\n"
                + "WHERE \n"
                + "    successful = true\n"
                + "GROUP BY \n"
                + "    userid, \n"
                + "    HOUR(accesstime)";

        String surreal = "SELECT \n"
                + "    userid, \n"
                + "    time::hour(accesstime) AS hours, \n"
                + "    math::mean(duration) AS avgDuration\n"
                + "FROM \n"
                + "    %s\n"
                + "WHERE \n"
                + "    successful = true\n"
                + "GROUP BY \n"
                + "    userid, \n"
                + "    hours";

        return Collections.singletonList( RawQuery.builder()
                .surrealQl( String.format( surreal, getLoginTable( false ) ) )
                .sql( String.format( sql, getLoginTable( true ) ) )
                .types( Arrays.asList( QueryTypes.COMPLEX_LOGIN_2, QueryTypes.RELATIONAL ) )
                .build() );
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
        return buildQueries( properties );
    }


    @Value
    public static class Login {

        long userId;
        long deviceId;

        LocalDateTime timestamp;

        long mins;

        boolean successful;


        public static Login createRandomLogin( long userid, long deviceId, Network network ) {
            boolean successful = network.getRandom().nextFloat() < 0.8;
            return new Login( userid, deviceId, network.createRandomTimestamp(), successful ? network.getRandom().nextInt( 24 ) : 0, successful );
        }


        public Map<String, String> asStrings( Mode mode ) {
            Map<String, String> values = new HashMap<>();
            for ( Entry<String, PropertyType> entry : User.loginTypes.entrySet() ) {
                switch ( entry.getKey() ) {
                    case "userid":
                        values.put( entry.getKey(), String.valueOf( getUserId() ) );
                        break;
                    case "deviceid":
                        values.put( entry.getKey(), String.valueOf( getDeviceId() ) );
                        break;
                    case "accesstime":
                        String timestamp = "";
                        if ( mode == Mode.POLYPHENY ) {
                            timestamp = " TIMESTAMP '" + this.timestamp.format( DateTimeFormatter.ofPattern( "yyyy-MM-dd hh:mm:ss" ) ) + "'";
                        } else if ( mode == Mode.SURREALDB ) {
                            timestamp = "'" + this.timestamp.format( DateTimeFormatter.ofPattern( "yyyy-MM-dd hh:mm:ss" ) ).replace( " ", "T" ) + "Z" + "'";
                        } else {
                            throw new RuntimeException();
                        }

                        values.put( entry.getKey(), timestamp );
                        break;
                    case "duration":
                        values.put( entry.getKey(), String.valueOf( getMins() ) );
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
