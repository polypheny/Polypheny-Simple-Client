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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.scenario.coms.ComsConfig;
import org.polypheny.simpleclient.scenario.coms.simulation.Graph.Edge;
import org.polypheny.simpleclient.scenario.coms.simulation.Graph.Node;
import org.polypheny.simpleclient.scenario.coms.simulation.PropertyType.Type;

@Slf4j
public class NetworkGenerator {

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

        public static long MOBILES = 200;

        public static long APS = 50;

        public static long SWITCHES = 10;
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
        public static ComsConfig config;
        int scale;


        public Network( Random random, ComsConfig config ) {
            this.random = random;
            this.scale = config.networkScale;
            Network.config = config;

            generateObject( SERVERS, () -> servers.add( new Server( random ) ) );
            generateObject( SWITCHES, () -> switches.add( new Switch( random ) ) );
            generateObject( APS, () -> aps.add( new AP( random ) ) );

            generateObject( CLIENTS * OS_DISTRIBUTION, () -> pcs.add( new PC( random ) ) );
            generateObject( CLIENTS * (1 - OS_DISTRIBUTION), () -> macs.add( new Mac( random ) ) );

            generateObject( MOBILES * MOBILE_DISTRIBUTION, () -> mobiles.add( new Mobile( random ) ) );
            generateObject( MOBILES * (1 - MOBILE_DISTRIBUTION), () -> ioTs.add( new IoT( random ) ) );

            generateConnection( this.servers, this.servers, this.servers.size(), Connection.LAN );

            generateConnection( this.servers, this.switches, this.switches.size() * 2, Connection.LAN );

            generateConnection( this.switches, this.pcs, this.switches.size() * 2, Connection.LAN );
            generateConnection( this.switches, this.macs, this.switches.size() * 2, Connection.LAN );

            generateConnection( this.servers, this.aps, this.aps.size() * 2, Connection.LAN );

            generateConnection( this.aps, this.mobiles, this.mobiles.size() * 2, Connection.WLAN );
            generateConnection( this.aps, this.ioTs, this.ioTs.size() * 2, Connection.WLAN );
        }


        private <L extends GraphElement, R extends GraphElement> void generateConnection( List<L> fromElements, List<R> toElements, int max, Connection connection ) {
            int min = max / 2;
            if ( min >= max ) {
                min = max - 1;
            }
            generateConnection( fromElements, toElements, min, max, connection );
        }


        private <L extends GraphElement, R extends GraphElement> void generateConnection( List<L> fromElements, List<R> toElements, int min, int max, Connection connection ) {

            for ( int i = 0; i < min + random.nextInt( max - min ) + 1; i++ ) {
                L from = fromElements.get( random.nextInt( fromElements.size() ) );
                R to = toElements.get( random.nextInt( toElements.size() ) );

                if ( connection == Connection.WLAN ) {
                    this.wlans.add( new WLan( from.getId(), to.getId(), false, random ) );
                } else if ( connection == Connection.LAN ) {
                    this.lans.add( new Lan( from.getId(), to.getId(), false, random ) );
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
            collectNodes( ioTs, nodes );
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
                properties.put( "key" + i, type.asString( random, 3, 250, Type.OBJECT, Type.ARRAY ) );
            }
            return properties;
        }


        public static Map<String, String> generateFixedTypedProperties( Random random, Map<String, PropertyType> types ) {
            Map<String, String> properties = new HashMap<>();

            for ( Entry<String, PropertyType> entry : types.entrySet() ) {
                StringBuilder value = new StringBuilder( String.valueOf( random.nextInt( 10 ) ) );
                switch ( entry.getValue().getType() ) {
                    case CHAR:
                        value = new StringBuilder( "'value" + value + "'" );
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

            for ( int i = 0; i < random.nextInt( 10 ) + 1; i++ ) {
                String key = "key" + random.nextInt( 10 );
                Type type = Type.getRandom( random );
                object.add( key, type.asJson( random, nestingDepth ) );
            }

            return object;
        }


        public List<Query> simulateRun() {
            return Stream.concat(
                    simulateDevices().stream(),
                    simulateLogs().stream() ).collect( Collectors.toList() );

        }


        @NonNull
        private List<Query> simulateLogs() {
            List<Query> queries = new ArrayList<>();

            //// remove logs
            handleRemoveLogs( queries );
            readEntities( queries );

            //// add logs
            handleAddLogs( queries );
            readEntities( queries );

            return queries;
        }


        private void handleAddLogs( List<Query> queries ) {
            queries.addAll(
                    merge(
                            doRandomly( mobiles, mobiles.size() / 10f, this::addLogs ),
                            doRandomly( ioTs, ioTs.size() / 10f, this::addLogs ),
                            doRandomly( pcs, pcs.size() / 10f, this::addLogs ),
                            doRandomly( aps, aps.size() / 10f, this::addLogs ),
                            doRandomly( switches, switches.size() / 10f, this::addLogs ),
                            doRandomly( servers, servers.size() / 10f, this::addLogs )
                    )
            );
        }


        private void handleRemoveLogs( List<Query> queries ) {
            queries.addAll(
                    merge(
                            doRandomly( mobiles, mobiles.size() / 10f, this::deleteLogs ),
                            doRandomly( ioTs, ioTs.size() / 10f, this::deleteLogs ),
                            doRandomly( pcs, pcs.size() / 10f, this::deleteLogs ),
                            doRandomly( aps, aps.size() / 10f, this::deleteLogs ),
                            doRandomly( switches, switches.size() / 10f, this::deleteLogs ),
                            doRandomly( servers, servers.size() / 10f, this::deleteLogs )
                    )
            );
        }


        private <T extends Node> List<Query> deleteLogs( T element ) {
            return element.deleteLogs( random );
        }


        private <T extends Node> List<Query> addLogs( T element ) {
            return element.addLog( random, config.nestingDepth );
        }


        @SafeVarargs
        private final List<Query> merge( List<Query>... multipleQueries ) {
            // order of single list has to be preserved but not their order
            return Arrays.stream( multipleQueries ).flatMap( Collection::stream ).collect( Collectors.toCollection( ArrayList::new ) );
        }


        @SafeVarargs
        private final List<Query> mergeRandomly( List<Query>... multipleQueries ) {
            // order of single list has to be preserved but not their order
            List<Query> queries = Arrays.stream( multipleQueries ).flatMap( Collection::stream ).collect( Collectors.toCollection( ArrayList::new ) );
            Collections.shuffle( queries, random );
            return queries;
        }


        private <T extends GraphElement> List<Query> doRandomly( List<T> elements, float roughAmount, Function<T, List<Query>> task ) {
            if ( roughAmount < 1 ) {
                return Collections.emptyList();
            }

            int amount = (int) roughAmount;

            List<Query> queries = new ArrayList<>();
            for ( int i = 0; i < amount; i++ ) {
                queries.addAll( task.apply( elements.get( random.nextInt( amount ) ) ) );
            }
            return queries;
        }


        @NonNull
        private List<Query> simulateDevices() {
            List<Query> queries = new ArrayList<>();

            for ( int i = 0; i < random.nextInt( 4 ); i++ ) {
                //// remove old devices
                simulateDeleteDevices( queries );
                readEntities( queries );

                //// add new devices
                simulateNewDevices( queries );
                readEntities( queries );

                //// change properties of devices
                simulateDeviceChanges( queries );
                readEntities( queries );
            }

            return queries;
        }


        private void readEntities( List<Query> queries ) {
            List<List<? extends GraphElement>> elements = Arrays.asList( mobiles, ioTs, pcs, macs, aps, servers, switches, wlans, lans );
            List<Function<GraphElement, List<Query>>> tasks = Arrays.asList( GraphElement::getReadAllDynamic, GraphElement::getReadAllFixed, GraphElement::getReadAllNested, GraphElement::readFullLog, e -> e.readPartialLog( random ) );

            for ( int i = 0; i < random.nextInt( config.numberOfThreads ); i++ ) {
                int randomType = random.nextInt( elements.size() + 1 );

                for ( int j = 0; j < random.nextInt(); j++ ) {
                    int randomElement = random.nextInt( elements.size() + 1 );

                    GraphElement element = elements.get( randomType ).get( randomElement );

                    int taskId = random.nextInt( tasks.size() + 1 );
                    queries.addAll( tasks.get( taskId ).apply( element ) );

                }

            }
        }


        private void simulateDeviceChanges( List<Query> queries ) {

            queries.addAll( merge(
                            // remove Mobile devices (lot)
                            doRandomly( mobiles, mobiles.size() / 10f, this::changeDevice ),
                            // remove Iot devices (small)
                            doRandomly( ioTs, ioTs.size() / 10f, this::changeDevice ),
                            // remove PC and Macs (small)
                            doRandomly( pcs, pcs.size() / 10f, this::changeDevice ),
                            doRandomly( macs, macs.size() / 10f, this::changeDevice ),
                            // remove AP (minimal)
                            doRandomly( aps, aps.size() / 10f, this::changeDevice ),
                            // remove Server (nearly none)
                            doRandomly( servers, servers.size() / 10f, this::changeDevice ),
                            // remove Swich (nearly none)
                            doRandomly( switches, switches.size() / 10f, this::changeDevice ),
                            // change connections (lot)
                            doRandomly( wlans, wlans.size() / 10f, this::changeDevice ),
                            doRandomly( lans, lans.size() / 10f, this::changeDevice )
                    )
            );

        }


        private <T extends GraphElement> List<Query> changeDevice( T element ) {
            List<Query> queries = new ArrayList<>();

            queries.addAll( element.changeDevice( random ) );
            queries.addAll( element.addDeviceAction( random ) );
            // todo add more changes

            return queries;
        }


        private void simulateDeleteDevices( List<Query> queries ) {

            queries.addAll( merge(
                    // remove Mobile devices (lot)
                    doRandomly( mobiles, mobiles.size() / 10f, e -> removeElement( e, mobiles ) ),
                    // remove Iot devices (small)
                    doRandomly( ioTs, ioTs.size() / 10f, e -> removeElement( e, ioTs ) ),
                    // remove PC and Macs (small)
                    doRandomly( pcs, pcs.size() / 25f, e -> removeElement( e, pcs ) ),
                    doRandomly( macs, macs.size() / 25f, e -> removeElement( e, macs ) ),
                    // remove AP (minimal)
                    doRandomly( aps, aps.size() / 10f, e -> removeElement( e, aps ) ),
                    // remove Server (nearly none)
                    doRandomly( servers, servers.size() / 10f, e -> removeElement( e, servers ) ),
                    doRandomly( switches, switches.size() / 10f, e -> removeElement( e, switches )
                    ) )
            );

        }


        private void simulateNewDevices( List<Query> queries ) {

            queries.addAll( merge(
                            // add Mobile devices (lot)
                            doRandomly( mobiles, mobiles.size() / 10f, e -> addElement( e, mobiles ) ),
                            // add Iot devices (lot)
                            doRandomly( ioTs, ioTs.size() / 10f, e -> addElement( e, ioTs ) ),
                            // add PC and Macs (small)
                            doRandomly( pcs, pcs.size() / 25f, e -> addElement( e, pcs ) ),
                            doRandomly( macs, macs.size() / 25f, e -> addElement( e, macs ) ),
                            // add AP (minimal)
                            doRandomly( aps, aps.size() / 10f, e -> addElement( e, aps ) ),
                            // add Server (nearly none)
                            doRandomly( servers, servers.size() / 10f, e -> addElement( e, servers ) ),
                            doRandomly( switches, switches.size() / 10f, e -> addElement( e, switches ) )
                    )
            );

        }


        private <E extends GraphElement> List<Query> addElement( E element, List<E> elements ) {
            elements.add( element );

            return element.asQuery();
        }


        private <T extends GraphElement> List<Query> removeElement( T element, List<T> elements ) {
            elements.remove( element );

            if ( element instanceof Node ) {
                // we need to remove connections first
                List<WLan> ws = this.wlans
                        .stream()
                        .filter( w -> w.getFrom() == element.getId() || w.getTo() == element.getId() ).collect( Collectors.toList() );
                List<Query> wQueries = ws
                        .stream()
                        .flatMap( e -> removeElement( e, wlans ).stream() )
                        .collect( Collectors.toList() );

                List<Lan> ls = this.lans
                        .stream()
                        .filter( w -> w.getFrom() == element.getId() || w.getTo() == element.getId() ).collect( Collectors.toList() );
                List<Query> lQueries = ls
                        .stream()
                        .flatMap( e -> removeElement( e, lans ).stream() )
                        .collect( Collectors.toList() );

                return Stream.concat( Stream.concat( wQueries.stream(), lQueries.stream() ), element.getRemoveQuery().stream() ).collect( Collectors.toList() );
            }

            return element.getRemoveQuery();
        }


        private boolean isNull( WLan w ) {
            if ( w == null ) {
                System.out.println( w );
            }

            return true;
        }


        @Override
        public String toString() {
            return "Network{\n" +
                    "\tservers: " + servers.size() + " with " + summarize( servers ) + ",\n" +
                    "\tswitches: " + switches.size() + " with " + summarize( switches ) + ",\n" +
                    "\taps: " + aps.size() + " with " + summarize( aps ) + ",\n" +
                    "\tioTs: " + ioTs.size() + " with " + summarize( ioTs ) + ",\n" +
                    "\tmobiles: " + mobiles.size() + " with " + summarize( mobiles ) + ",\n" +
                    "\tpcs: " + pcs.size() + " with " + summarize( pcs ) + ",\n" +
                    "\tmacs: " + macs.size() + " with " + summarize( macs ) + ",\n" +
                    "\tlans: " + lans.size() + " with " + summarize( lans ) + ",\n" +
                    "\twlans: " + wlans.size() + " with " + summarize( wlans ) + ",\n" +
                    '}';
        }


        private <E extends GraphElement> String summarize( List<E> elements ) {
            double avgGraph = elements.stream().mapToInt( e -> e.getDynProperties().size() ).average().orElseThrow( () -> new RuntimeException( "Error on avg." ) );
            double avgRel = elements.stream().mapToInt( e -> e.getFixedProperties().size() ).average().orElseThrow( () -> new RuntimeException( "Error on avg." ) );
            double avgDoc = elements.stream().filter( e -> e instanceof Node ).map( e -> (Node) e ).mapToInt( e -> e.getNestedQueries().size() ).average().orElse( 0 ); // only Node do have this
            return "{ avgGraphProps: " + avgGraph + ", avgRelProps: " + avgRel + ", avgDocProps: " + avgDoc + " }";
        }


    }

    //// Backbone


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Server extends Node {

        Random random;


        public Server( Random random ) {
            super(
                    Network.generateFixedTypedProperties( random, getTypes() ),
                    Network.generateProperties( random, Network.config.switchConfigs ),
                    Network.generateNestedProperties( random, Network.config.nestingDepth ) );
            this.random = random;
        }


    }

    //// Distributors


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Switch extends Node {

        Random random;


        public Switch( Random random ) {
            super(
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, Network.config.switchConfigs ),
                    Network.generateNestedProperties( random, Network.config.nestingDepth ) );
            this.random = random;
        }

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class AP extends Node {

        Random random;


        public AP( Random random ) {
            super(
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, Network.config.apDynConfigs ),
                    Network.generateNestedProperties( random, Network.config.nestingDepth ) );
            this.random = random;
        }


    }

    //// Devices


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class IoT extends Node {

        Random random;


        public IoT( Random random ) {
            super(
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, Network.config.mobileDynConfigsMax ),
                    Network.generateNestedProperties( random, Network.config.nestingDepth ) );
            this.random = random;
        }

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Mobile extends Node {

        Random random;


        public Mobile( Random random ) {
            super(
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, Network.config.mobileDynConfigsMax ),
                    Network.generateNestedProperties( random, Network.config.nestingDepth ) );
            this.random = random;
        }

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class PC extends Node {

        Random random;


        public PC( Random random ) {
            super(
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, Network.config.pcDynConfigsMax ),
                    Network.generateNestedProperties( random, Network.config.nestingDepth ) );
            this.random = random;
        }


    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Mac extends Node {

        Random random;


        public Mac( Random random ) {
            super(
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, Network.config.pcDynConfigsMax ),
                    Network.generateNestedProperties( random, Network.config.nestingDepth ) );
            this.random = random;
        }

    }

    //// Connections


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Lan extends Edge {

        Random random;


        public Lan( long from, long to, boolean directed, Random random ) {
            super(
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, Network.config.connectionConfigs ),
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


        public WLan( long from, long to, boolean directed, Random random ) {
            super(
                    Network.generateFixedTypedProperties( random, types ),
                    Network.generateProperties( random, Network.config.connectionConfigs ),
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
