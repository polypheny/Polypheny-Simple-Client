/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/11/23, 11:54 AM The Polypheny Project
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

import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;

@Value
public class Graph {

    Map<Long, Node> nodes;
    Map<Long, Edge> edges;


    public List<String> getGraphQueries() {
    }


    public List<String> getDocQueries() {
    }


    public List<String> getRelQueries() {
        return null;
    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    @NonFinal
    public static class Node extends GraphElement {

        public Node(
                Map<String, PropertyType> types,
                Map<String, String> fixedProperties,
                Map<String, String> dynProperties,
                Map<String, String> nestedQueries ) {
            super( types, fixedProperties, dynProperties );
        }

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    @NonFinal
    public static class Edge extends GraphElement {

        public Edge( Map<String, PropertyType> types, Map<String, String> fixedProperties, Map<String, String> dynProperties, long from, long to, boolean directed ) {
            super( types, fixedProperties, dynProperties );
            this.from = from;
            this.to = to;
            this.directed = directed;
        }


        long from;
        long to;
        boolean directed;

    }

}
