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


    public enum Type {
        CHAR, FLOAT, NUMBER, ARRAY, OBJECT;


        public JsonElement asJson( Random random, int nestingDepth ) {
            switch ( this ) {
                case CHAR:
                    return new JsonPrimitive( "value" + random.nextInt() );
                case FLOAT:
                    return new JsonPrimitive( random.nextFloat() );
                case NUMBER:
                    return new JsonPrimitive( random.nextInt() );
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


        public String asString( Random random, Type... excepts ) {
            switch ( this ) {
                case CHAR:
                    return "\"value" + random.nextInt() + "\"";
                case FLOAT:
                    return String.valueOf( random.nextFloat() );
                case NUMBER:
                    return String.valueOf( random.nextInt() );
                case ARRAY:
                    List<String> array = new ArrayList<>();
                    int fill = random.nextInt( 10 );
                    for ( int i = 0; i < fill; i++ ) {
                        Type type = getRandom( random, excepts );
                        array.add( type.asString( random, excepts ) );
                    }
                    return "[" + String.join( ", ", array ) + "]";
                case OBJECT:
                    throw new UnsupportedOperationException();
            }

            throw new RuntimeException();
        }
    }


}
