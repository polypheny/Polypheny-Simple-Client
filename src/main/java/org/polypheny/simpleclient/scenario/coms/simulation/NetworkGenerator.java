/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/11/23, 11:48 AM The Polypheny Project
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

import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.simpleclient.scenario.coms.ComsConfig;
import org.polypheny.simpleclient.scenario.coms.simulation.Graph.Edge;
import org.polypheny.simpleclient.scenario.coms.simulation.Graph.Node;
import org.polypheny.simpleclient.scenario.coms.simulation.PropertyType.Type;

public class NetworkGenerator {


    public final ComsConfig config;
    private final Random random;
    public final Network network;


    /**
     * Network architecture relies on a low amount of main architecture
     * with a low connectivity
     * Depending on size of users a amount of Switches and APs is deploy.
     *
     * @param config
     */
    public NetworkGenerator( ComsConfig config ) {
        this.config = config;
        this.random = new Random( config.seed );

        // generateSchema network (simulation)
        // -> generateSchema structure of network -> nodes
        // -> generateSchema parameters per nodes
        // -> generateSchema configs depending on type of node
        // -> generateSchema logs?

        this.network = new Network( random, config );

    }


    @Value
    public static class Network {

        public static AtomicLong idBuilder = new AtomicLong();
        public static long SERVERS = 5;

        public static long CLIENTS = 100;

        public static double OS_DISTRIBUTION = 0.7;

        public static double MOBILE_DISTRIBUTION = 0.4;

        public static long MOBILES = 400;

        public static long APS = 100;

        public static long SWITCHES = 5;
        Random random;
        List<Server> servers = new ArrayList<>();
        List<Switch> switches = new ArrayList<>();

        List<AP> aps = new ArrayList<>();
        List<IoT> ioTs = new ArrayList<>();

        List<Mobile> mobiles = new ArrayList<>();

        List<PC> pcs = new ArrayList<>();

        List<Mac> macs = new ArrayList<>();

        List<Lan> lans = new ArrayList<>();

        List<WLan> wlans = new ArrayList<>();
        ComsConfig config;
        int scale;


        public Network( Random random, ComsConfig config ) {
            this.random = random;
            this.scale = config.networkScale;
            this.config = config;

            generateObject( SERVERS, () -> servers.add( new Server( this, random ) ) );
            generateObject( SWITCHES, () -> switches.add( new Switch( this, random ) ) );
            generateObject( APS, () -> aps.add( new AP( this, random ) ) );

            generateObject( CLIENTS * OS_DISTRIBUTION, () -> pcs.add( new PC( this, random ) ) );
            generateObject( CLIENTS * (1 - OS_DISTRIBUTION), () -> macs.add( new Mac( this, random ) ) );

            generateObject( MOBILES * MOBILE_DISTRIBUTION, () -> mobiles.add( new Mobile( this, random ) ) );
            generateObject( MOBILES * (1 - MOBILE_DISTRIBUTION), () -> ioTs.add( new IoT( this, random ) ) );

            generateConnection( this.servers, this.servers, this.servers.size() / 4, Connection.LAN );

            generateConnection( this.servers, this.switches, this.switches.size(), Connection.LAN );

            generateConnection( this.switches, this.pcs, this.switches.size(), Connection.LAN );
            generateConnection( this.switches, this.macs, this.switches.size(), Connection.LAN );

            generateConnection( this.servers, this.aps, this.aps.size(), Connection.LAN );

            generateConnection( this.aps, this.mobiles, this.mobiles.size(), Connection.WLAN );
            generateConnection( this.aps, this.ioTs, this.ioTs.size(), Connection.WLAN );
        }


        private <L extends GraphElement, R extends GraphElement> void generateConnection( List<L> fromElements, List<R> toElements, int max, Connection connection ) {
            generateConnection( fromElements, toElements, 0, max, connection );
        }


        private <L extends GraphElement, R extends GraphElement> void generateConnection( List<L> fromElements, List<R> toElements, int min, int max, Connection connection ) {

            for ( int i = 0; i < min + random.nextInt( max - min ); i++ ) {
                L from = fromElements.get( random.nextInt( fromElements.size() ) );
                R to = toElements.get( random.nextInt( toElements.size() ) );

                if ( connection == Connection.WLAN ) {
                    this.wlans.add( new WLan( this, from.getId(), to.getId(), false, random ) );
                } else if ( connection == Connection.LAN ) {
                    this.lans.add( new Lan( this, from.getId(), to.getId(), false, random ) );
                }
            }

        }


        private void generateObject( double amount, Runnable runnable ) {
            for ( long i = 0; i < amount * this.scale; i++ ) {
                runnable.run();
            }
        }


        public Graph toGraph() {
            Map<Long, Node> nodes = new HashMap<>();
            collectNodes( servers, nodes );
            collectNodes( pcs, nodes );
            collectNodes( macs, nodes );
            collectNodes( switches, nodes );
            collectNodes( mobiles, nodes );
            collectNodes( aps, nodes );

            Map<Long, Edge> edges = new HashMap<>();
            collectEdges( lans, edges );
            collectEdges( wlans, edges );

            return new Graph( nodes, edges );
        }


        private void collectNodes( List<? extends Node> source, Map<Long, Node> target ) {
            for ( Node element : source ) {
                target.put( element.getId(), element );
            }
        }


        private void collectEdges( List<? extends Edge> source, Map<Long, Edge> target ) {
            for ( Edge element : source ) {
                target.put( element.getId(), element );
            }
        }


        public static Map<String, String> generateProperties( Random random, int amount ) {
            Map<String, String> properties = new HashMap<>();

            for ( int i = 0; i < amount; i++ ) {
                Type type = Type.getRandom( random, Type.OBJECT );
                properties.put( "key" + i, type.asString( random, 3, Type.OBJECT ) );
            }
            return properties;
        }


        public static Map<String, String> generateFixedTypedProperties( Random random, Map<String, PropertyType> types ) {
            Map<String, String> properties = new HashMap<>();

            for ( Entry<String, PropertyType> entry : types.entrySet() ) {
                StringBuilder value = new StringBuilder( String.valueOf( random.nextInt( 10 ) ) );
                switch ( entry.getValue().getType() ) {
                    case CHAR:
                        value = new StringBuilder( "\"value" + value + "\"" );
                        break;
                    case NUMBER:
                        break;
                    case FLOAT:
                        value.append( random.nextInt( 10 ) );
                        break;
                    case ARRAY:
                        value = new StringBuilder( "[" + value );
                        for ( int j = 0; j < entry.getValue().getLength(); j++ ) {
                            value.append( ", " );
                            random.nextInt( 10 );
                        }
                        value.append( "]" );
                        break;
                }

                properties.put( entry.getKey(), value.toString() );


            }
            return properties;
        }


        public static JsonObject generateNestedProperties( Random random, int nestingDepth ) {
            JsonObject object = new JsonObject();

            for ( int i = 0; i < random.nextInt( 10 ); i++ ) {
                String key = "key" + random.nextInt( 10 );
                Type type = Type.getRandom( random );
                object.add( key, type.asJson( random, nestingDepth ) );
            }

            return object;
        }

    }

    //// Backbone


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Server extends Node {

        Random random;

        public static Map<String, PropertyType> types = new HashMap<String, PropertyType>() {{
            put( "id", new PropertyType( 3, Type.NUMBER ) );
            put( "manufactureId", new PropertyType( 12, Type.NUMBER ) );
            put( "manufactureName", new PropertyType( 12, Type.CHAR ) );
            put( "entry", new PropertyType( 12, Type.NUMBER ) );
        }};


        public Server( Network network, Random random ) {
            super(
                    types,
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, network.config.switchConfigs ),
                    Network.generateNestedProperties( random, network.config.nestingDepth ) );
            this.random = random;
        }

    }

    //// Distributors


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Switch extends Node {

        Random random;

        public static Map<String, PropertyType> types = new HashMap<String, PropertyType>() {{
            put( "id", new PropertyType( 3, Type.NUMBER ) );
            put( "manufactureId", new PropertyType( 12, Type.NUMBER ) );
            put( "manufactureName", new PropertyType( 12, Type.CHAR ) );
            put( "entry", new PropertyType( 12, Type.NUMBER ) );
        }};


        public Switch( Network network, Random random ) {
            super(
                    types,
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, network.config.switchConfigs ),
                    Network.generateNestedProperties( random, network.config.nestingDepth ) );
            this.random = random;
        }

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class AP extends Node {

        Random random;

        public static Map<String, PropertyType> types = new HashMap<String, PropertyType>() {{
            put( "id", new PropertyType( 3, Type.NUMBER ) );
            put( "manufactureId", new PropertyType( 12, Type.NUMBER ) );
            put( "manufactureName", new PropertyType( 12, Type.CHAR ) );
            put( "entry", new PropertyType( 12, Type.NUMBER ) );
        }};


        public AP( Network network, Random random ) {
            super( types,
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, network.config.apDynConfigs ),
                    Network.generateNestedProperties( random, network.config.nestingDepth ) );
            this.random = random;
        }

    }

    //// Devices


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class IoT extends Node {

        Random random;

        public static Map<String, PropertyType> types = new HashMap<String, PropertyType>() {{
            put( "id", new PropertyType( 3, Type.NUMBER ) );
            put( "manufactureId", new PropertyType( 12, Type.NUMBER ) );
            put( "manufactureName", new PropertyType( 12, Type.CHAR ) );
            put( "entry", new PropertyType( 12, Type.NUMBER ) );
        }};


        public IoT( Network network, Random random ) {
            super( types,
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, network.config.mobileDynConfigsMax ),
                    Network.generateNestedProperties( random, network.config.nestingDepth ) );
            this.random = random;
        }

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Mobile extends Node {

        Random random;

        public static Map<String, PropertyType> types = new HashMap<String, PropertyType>() {{
            put( "id", new PropertyType( 3, Type.NUMBER ) );
            put( "manufactureId", new PropertyType( 12, Type.NUMBER ) );
            put( "manufactureName", new PropertyType( 12, Type.CHAR ) );
            put( "entry", new PropertyType( 12, Type.NUMBER ) );
        }};


        public Mobile( Network network, Random random ) {
            super( types,
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, network.config.mobileDynConfigsMax ),
                    Network.generateNestedProperties( random, network.config.nestingDepth ) );
            this.random = random;
        }

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class PC extends Node {

        Random random;

        public static Map<String, PropertyType> types = new HashMap<String, PropertyType>() {{
            put( "id", new PropertyType( 3, Type.NUMBER ) );
            put( "manufactureId", new PropertyType( 12, Type.NUMBER ) );
            put( "manufactureName", new PropertyType( 12, Type.CHAR ) );
            put( "entry", new PropertyType( 12, Type.NUMBER ) );
        }};


        public PC( Network network, Random random ) {
            super( types,
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, network.config.pcDynConfigsMax ),
                    Network.generateNestedProperties( random, network.config.nestingDepth ) );
            this.random = random;
        }


    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Mac extends Node {

        Random random;

        public static Map<String, PropertyType> types = new HashMap<String, PropertyType>() {{
            put( "id", new PropertyType( 3, Type.NUMBER ) );
            put( "manufactureId", new PropertyType( 12, Type.NUMBER ) );
            put( "manufactureName", new PropertyType( 12, Type.CHAR ) );
            put( "entry", new PropertyType( 12, Type.NUMBER ) );
        }};


        public Mac( Network network, Random random ) {
            super( types,
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, network.config.pcDynConfigsMax ),
                    Network.generateNestedProperties( random, network.config.nestingDepth ) );
            this.random = random;
        }

    }

    //// Connections


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Lan extends Edge {

        Random random;

        public static Map<String, PropertyType> types = new HashMap<String, PropertyType>() {{
            put( "id", new PropertyType( 3, Type.NUMBER ) );
            put( "manufactureId", new PropertyType( 12, Type.NUMBER ) );
            put( "manufactureName", new PropertyType( 12, Type.CHAR ) );
            put( "entry", new PropertyType( 12, Type.NUMBER ) );
        }};


        public Lan( Network network, long from, long to, boolean directed, Random random ) {
            super(
                    types,
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, network.config.connectionConfigs ),
                    from,
                    to,
                    directed );
            this.random = random;
        }


    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class WLan extends Edge {

        Random random;

        public static Map<String, PropertyType> types = new HashMap<String, PropertyType>() {{
            put( "id", new PropertyType( 3, Type.NUMBER ) );
            put( "manufactureId", new PropertyType( 12, Type.NUMBER ) );
            put( "manufactureName", new PropertyType( 12, Type.CHAR ) );
            put( "entry", new PropertyType( 12, Type.NUMBER ) );
        }};


        public WLan( Network network, long from, long to, boolean directed, Random random ) {
            super(
                    types,
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, network.config.connectionConfigs ),
                    from,
                    to,
                    directed );
            this.random = random;
        }


    }


    enum Connection {
        WLAN,
        LAN
    }

}
