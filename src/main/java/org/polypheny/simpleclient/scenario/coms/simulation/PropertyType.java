/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/15/23, 1:24 PM The Polypheny Project
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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.Value;


@Value
public class PropertyType {

    int length;

    Type type;


    public String asSql() {
        return type.asSql( length );
    }


    public String asSurreal() {
        return type.asSurreal();
    }


    public enum Type {
        CHAR( "VARCHAR(", ")" ),
        FLOAT( "DOUBLE" ),
        NUMBER( "INT" ),
        ARRAY( "ARRAY" ),
        OBJECT( "" ),
        BOOLEAN( "BOOLEAN" ),
        TIMESTAMP( "TIMESTAMP" );


        private final String start;
        private final String end;


        Type( String start ) {
            this( start, null );
        }


        Type( String start, String end ) {
            this.start = start;
            this.end = end;
        }


        public JsonElement asJson( Random random, int nestingDepth, Type... excepts ) {
            switch ( this ) {
                case CHAR:
                    return new JsonPrimitive( "value" + random.nextInt() );
                case BOOLEAN:
                    return new JsonPrimitive( random.nextBoolean() );
                case FLOAT:
                    float fval = random.nextFloat() * 1000;
                    return new JsonPrimitive( NetworkGenerator.Network.config.allowNegative ? fval : Math.abs( fval ) );
                case NUMBER:
                    int ival = random.nextInt() * 5;
                    return new JsonPrimitive( NetworkGenerator.Network.config.allowNegative ? ival : Math.abs( ival ) );
                case ARRAY:
                    JsonArray array = new JsonArray();
                    int fill = random.nextInt( 10 );

                    Type[] types = new Type[excepts.length + 2];
                    System.arraycopy( excepts, 0, types, 0, excepts.length );
                    types[excepts.length] = OBJECT;
                    types[excepts.length + 1] = ARRAY;

                    for ( int i = 0; i < fill; i++ ) {
                        Type child = getRandom( random, types );
                        array.add( child.asJson( random, nestingDepth ) );
                    }
                    return array;
                case OBJECT:
                    JsonObject object = new JsonObject();
                    int f = random.nextInt( 10 );
                    Type[] objects = new Type[excepts.length + 1];
                    System.arraycopy( excepts, 0, objects, 0, excepts.length );
                    objects[excepts.length] = OBJECT;

                    for ( int i = 0; i < f; i++ ) {
                        Type child = nestingDepth <= 0 ? getRandom( random, objects ) : getRandom( random, excepts );
                        object.add( "key" + i, child.asJson( random, nestingDepth - 1, objects ) );
                    }
                    return object;
            }

            throw new RuntimeException();
        }


        public static Type getRandom( Random random, Type... excepts ) {
            List<Type> typesExcepts = Arrays.stream( Type.values() ).filter( t -> !Arrays.asList( excepts ).contains( t ) ).collect( Collectors.toList() );
            return typesExcepts.get( random.nextInt( typesExcepts.size() ) );
        }


        public String asString( Random random, int maxDepth, int size, Type... excepts ) {
            if ( maxDepth <= 0 ) {
                excepts = new Type[]{ OBJECT, ARRAY };
            }
            switch ( this ) {
                case CHAR:
                    return "'value" + random.nextInt( size ) + "'";
                case FLOAT:
                    float fval = random.nextFloat() * 5;
                    return String.valueOf( NetworkGenerator.Network.config.allowNegative ? fval : Math.abs( fval ) );
                case BOOLEAN:
                    return String.valueOf( random.nextBoolean() );
                case NUMBER:
                    int ival = random.nextInt() * 1000;
                    return String.valueOf( NetworkGenerator.Network.config.allowNegative ? ival : Math.abs( ival ) );
                case ARRAY:
                    List<String> array = new ArrayList<>();
                    int fill = random.nextInt( 10 );
                    Type type = getRandom( random, excepts ); // sadly only same type supported by neo4j...
                    for ( int i = 0; i < fill; i++ ) {
                        array.add( type.asString( random, maxDepth - 1, size, excepts ) );
                    }
                    return "[" + String.join( ", ", array ) + "]";
                case OBJECT:
                    throw new UnsupportedOperationException();
            }

            throw new RuntimeException();
        }


        public String asSql( int length ) {
            if ( end == null ) {
                return start;
            }
            return start + length + end;
        }


        public String asSurreal() {
            switch ( this ) {
                case CHAR:
                    return "string";
                case FLOAT:
                    return "float";
                case BOOLEAN:
                    return "bool";
                case NUMBER:
                    return "int";
                case TIMESTAMP:
                    return "datetime";
                case ARRAY:
                case OBJECT:
                    throw new RuntimeException();
            }
            throw new RuntimeException();
        }
    }

}
