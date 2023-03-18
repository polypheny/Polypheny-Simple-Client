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
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator.Network;

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
        CHAR( "VARCHAR(", ")" ), FLOAT( "FLOAT(", ")" ), NUMBER( "INT" ), ARRAY( "ARRAY" ), OBJECT( "" );


        private final String start;
        private final String end;


        Type( String start ) {
            this( start, null );
        }


        Type( String start, String end ) {
            this.start = start;
            this.end = end;
        }


        public JsonElement asJson( Random random, int nestingDepth ) {
            switch ( this ) {
                case CHAR:
                    return new JsonPrimitive( "value" + random.nextInt() );
                case FLOAT:
                    return new JsonPrimitive( Network.config.allowNegative ? random.nextFloat() : random.nextFloat( 0, 1000 ) );
                case NUMBER:
                    return new JsonPrimitive( Network.config.allowNegative ? random.nextInt() : random.nextInt( 0, 5 ) );
                case ARRAY:
                    JsonArray array = new JsonArray();
                    int fill = random.nextInt( 10 );
                    for ( int i = 0; i < fill; i++ ) {
                        Type child = getRandom( random, OBJECT, ARRAY );
                        array.add( child.asJson( random, nestingDepth ) );
                    }
                    return array;
                case OBJECT:
                    JsonObject object = new JsonObject();
                    int f = random.nextInt( 10 );
                    for ( int i = 0; i < f; i++ ) {
                        Type child = nestingDepth <= 0 ? getRandom( random, OBJECT ) : getRandom( random );
                        object.add( "key" + i, child.asJson( random, nestingDepth - 1 ) );
                    }
                    return object;
            }

            throw new RuntimeException();
        }


        public static Type getRandom( Random random, Type... excepts ) {
            List<Type> typesExcepts = Arrays.stream( Type.values() ).filter( t -> !Arrays.asList( excepts ).contains( t ) ).collect( Collectors.toList() );
            return typesExcepts.get( random.nextInt( typesExcepts.size() ) );
        }


        public String asString( Random random, int maxDepth, Type... excepts ) {
            if ( maxDepth <= 0 ) {
                excepts = new Type[]{ OBJECT, ARRAY };
            }
            switch ( this ) {
                case CHAR:
                    return "'value" + random.nextInt() + "'";
                case FLOAT:
                    return String.valueOf( Network.config.allowNegative ? random.nextFloat( 5 ) : random.nextFloat( 0, 5 ) );
                case NUMBER:
                    return String.valueOf( Network.config.allowNegative ? random.nextInt( 1000 ) : random.nextInt( 0, 1000 ) );
                case ARRAY:
                    List<String> array = new ArrayList<>();
                    int fill = random.nextInt( 10 );
                    Type type = getRandom( random, excepts ); // sadly only same type supported by neo4j...
                    for ( int i = 0; i < fill; i++ ) {
                        array.add( type.asString( random, maxDepth - 1, excepts ) );
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
                case NUMBER:
                    return "int";
                case ARRAY:
                case OBJECT:
                    throw new RuntimeException();
            }
            throw new RuntimeException();
        }
    }


}
