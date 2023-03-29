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
import java.time.LocalDate;
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
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.util.Precision;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.scenario.coms.ComsConfig;
import org.polypheny.simpleclient.scenario.coms.simulation.PropertyType.Type;
import org.polypheny.simpleclient.scenario.coms.simulation.User.Login;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.AP;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.Graph;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.Graph.Edge;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.Graph.Node;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.IoT;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.Lan;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.Mobile;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.PC;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.Server;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.Switch;
import org.polypheny.simpleclient.scenario.coms.simulation.entites.Wlan;

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

        long startDay = LocalDate.now().toEpochDay();
        long endDay;

        public static AtomicLong idBuilder = new AtomicLong();
        public static long SERVERS = 1;

        public static long USERS = 10;

        public static long CLIENTS = 10;


        public static double MOBILE_DISTRIBUTION = 0.4;

        public static long MOBILES = (long) (CLIENTS * 1.5);

        public static long APS = CLIENTS / 10;

        public static long SWITCHES = CLIENTS / 10;
        Random random;
        List<Server> servers = new ArrayList<>();
        List<Switch> switches = new ArrayList<>();

        List<AP> aps = new ArrayList<>();
        List<IoT> ioTs = new ArrayList<>();

        List<Mobile> mobiles = new ArrayList<>();

        List<PC> pcs = new ArrayList<>();


        List<Lan> lans = new ArrayList<>();

        List<Wlan> wlans = new ArrayList<>();

        List<User> users = new ArrayList<>();

        public static ComsConfig config;
        int scale;


        public Network( Random random, ComsConfig config ) {
            this.random = random;
            this.scale = config.networkScale;
            Network.config = config;

            endDay = startDay + config.duration;

            generateObject( USERS, () -> users.add( new User( random ) ) );

            generateObject( SERVERS, () -> servers.add( new Server( random, this ) ) );
            generateObject( SWITCHES, () -> switches.add( new Switch( random, this ) ) );
            generateObject( APS, () -> aps.add( new AP( random, this ) ) );

            generateObject( CLIENTS, () -> pcs.add( new PC( random, this ) ) );

            generateObject( MOBILES * MOBILE_DISTRIBUTION, () -> mobiles.add( new Mobile( random, this ) ) );
            generateObject( MOBILES * (1 - MOBILE_DISTRIBUTION), () -> ioTs.add( new IoT( random, this ) ) );

            generateConnection( this.servers, this.servers, this.servers.size(), Connection.LAN );

            generateConnection( this.servers, this.switches, this.switches.size() * 2, Connection.LAN );

            generateConnection( this.switches, this.pcs, this.switches.size() * 2, Connection.LAN );

            generateConnection( this.servers, this.aps, this.aps.size() * 2, Connection.LAN );

            generateConnection( this.aps, this.mobiles, this.mobiles.size() * 2, Connection.WLAN );
            generateConnection( this.aps, this.ioTs, this.ioTs.size() * 2, Connection.WLAN );


        }


        public List<Login> generateAccess( long deviceId, List<User> users, boolean isAccessedFrequently ) {
            List<Login> logins = new ArrayList<>();

            for ( User user : users ) {
                if ( !isAccessedFrequently && random.nextFloat() < 0.2 ) {
                    // really accessed infrequently
                    continue;
                }
                for ( int i = 0; i < random.nextInt( config.loginsPerUser ); i++ ) {
                    logins.add( Login.createRandomLogin( user.id, deviceId, this ) );

                }
            }

            return logins;
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

                // only one way connections
                if ( Stream.concat( wlans.stream(), lans.stream() ).anyMatch( w ->
                        (w.getFrom() == from.getId() && w.getTo() == to.getId())
                                || (w.getTo() == from.getId() && w.getFrom() == to.getId()) ) ) {
                    continue;
                }

                if ( connection == Connection.WLAN ) {
                    this.wlans.add( new Wlan( from.getId(), to.getId(), false, random ) );
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
            collectNodes( switches, nodes );
            collectNodes( mobiles, nodes );
            collectNodes( ioTs, nodes );
            collectNodes( aps, nodes );

            Map<Long, Edge> edges = new HashMap<>();
            collectEdges( lans, edges );
            collectEdges( wlans, edges );

            return new Graph( nodes, edges, users.stream().collect( Collectors.toMap( u -> u.id, u -> u ) ) );
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
                    case BOOLEAN:
                        value.append( random.nextBoolean() );
                        break;
                    case NUMBER:
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
            return Stream.concat( Stream.concat(
                    simulateDevices().stream(),
                    simulateLogs().stream()
            ), simulateUsers().stream() ).collect( Collectors.toList() );

        }


        private List<Query> simulateUsers() {
            List<Query> queries = new ArrayList<>();

            readEntities( queries );
            handleRemoveUsers( queries );
            handleNewLogins( queries );

            readEntities( queries );
            handleAddUsers( queries );
            readEntities( queries );

            handleUserChange( queries );
            readEntities( queries );

            return queries;
        }


        private void handleNewLogins( List<Query> queries ) {
            queries.addAll( doRandomly( users, users.size() * config.newLogins, this::addLogin ) );
        }


        private List<Query> addLogin( User user ) {
            List<List<? extends Node>> targets = Arrays.asList( ioTs, pcs, mobiles, servers );

            List<? extends Node> target = targets.get( random.nextInt( targets.size() ) );
            Node device = target.get( random.nextInt( target.size() ) );

            return device.addAccess( user, this );
        }


        private void handleUserChange( List<Query> queries ) {
            queries.addAll( doRandomly( users, users.size() * config.removeUsers, u -> u.changeUser( random ) ) );
        }


        private void handleAddUsers( List<Query> queries ) {
            queries.addAll( doRandomly( users, users.size() * config.removeUsers, this::addUser ) );
        }


        private void handleRemoveUsers( List<Query> queries ) {
            queries.addAll( doRandomly( users, users.size() * config.removeUsers, this::removeUser ) );
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


        private List<Query> removeUser( User user ) {
            users.remove( user );

            return user.getRemoveQuery();
        }


        private List<Query> addUser( User user ) {
            users.add( user );

            return user.getInsertQuery( random );
        }


        private void handleAddLogs( List<Query> queries ) {
            for ( List<? extends Node> nodes : Arrays.asList( mobiles, ioTs, pcs, aps, servers ) ) {
                queries.addAll( doRandomly( nodes, nodes.size() * config.addLogs, this::addLogs ) );
            }
        }


        private void handleRemoveLogs( List<Query> queries ) {
            for ( List<? extends Node> nodes : Arrays.asList( mobiles, ioTs, pcs, aps, servers ) ) {
                queries.addAll( doRandomly( nodes, nodes.size() * config.addLogs, this::deleteLogs ) );
            }
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


        private <T> List<Query> doRandomly( List<T> elements, double roughAmount, Function<T, List<Query>> task ) {
            if ( roughAmount < 1 ) {
                return Collections.emptyList();
            }
            int amount = (int) roughAmount;

            List<Query> queries = new ArrayList<>();
            for ( int i = 0; i < amount; i++ ) {
                queries.addAll( task.apply( elements.get( random.nextInt( elements.size() ) ) ) );
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
            }

            return queries;
        }


        private void readEntities( List<Query> queries ) {
            List<List<? extends GraphElement>> elements = Arrays.asList( mobiles, ioTs, pcs, aps, servers, switches, wlans, lans );
            Map<Function<GraphElement, List<Query>>, Integer> tasks = new HashMap<Function<GraphElement, List<Query>>, Integer>() {{
                put( GraphElement::getReadAllDynamic, 1 );
                put( GraphElement::getReadAllNested, 1 );
                put( GraphElement::readFullLog, 1 );
                put( e -> e.readPartialLog( random ), 4 );
            }};

            Map<Function<User, List<Query>>, Integer> usersTask = new HashMap<Function<User, List<Query>>, Integer>() {{
                put( User::getReadAllFixed, 1 );
                put( u -> u.getReadSpecificPropFixed( random ), 1 );
            }};

            for ( int i = 0; i < 3 + random.nextInt( 10 ); i++ ) {
                int randomType = random.nextInt( elements.size() );

                List<? extends GraphElement> list = elements.get( randomType );
                if ( list.size() == 0 ) {
                    continue;
                }

                for ( int j = 0; j < 1 + random.nextInt( config.readQueries ); j++ ) {
                    if ( random.nextBoolean() ) {
                        int randomElement = random.nextInt( list.size() );

                        GraphElement element = list.get( randomElement );

                        Function<GraphElement, List<Query>> task = getRandomTask( random, tasks );
                        queries.addAll( task.apply( element ) );
                    } else {
                        int randomElement = random.nextInt( users.size() );

                        Function<User, List<Query>> task = getRandomTask( random, usersTask );
                        queries.addAll( task.apply( users.get( randomElement ) ) );
                    }
                }

            }
        }


        private <T> T getRandomTask( Random random, Map<T, Integer> tasks ) {
            List<T> elements = new ArrayList<>();

            tasks.forEach( ( k, v ) -> {
                for ( int i = 0; i < v; i++ ) {
                    elements.add( k );
                }
            } );

            int index = random.nextInt( elements.size() );

            return elements.get( index );
        }


        private void simulateDeleteDevices( List<Query> queries ) {
            for ( List<? extends Node> nodes : Arrays.asList( mobiles, ioTs, pcs, aps, servers ) ) {
                queries.addAll( doRandomly( nodes, nodes.size() * config.removeDevice, e -> removeElement( e, nodes ) ) );
            }
        }


        private void simulateNewDevices( List<Query> queries ) {
            for ( List<? extends Node> nodes : Arrays.asList( mobiles, ioTs, pcs, aps, servers ) ) {
                queries.addAll( doRandomly( nodes, nodes.size() * config.newDevice, e -> addElement( e, (List<Node>) nodes ) ) );
            }

        }


        private <E extends Node> List<Query> addElement( E element, List<E> elements ) {
            elements.add( element );

            List<Device> types = element.getPossibleConnectionTypes();

            List<Query> queries = new ArrayList<>( element.asQuery() );

            int addedConnections = random.nextInt( 2 ) + 1;
            for ( int i = 0; i < addedConnections; i++ ) {
                int typToConnect = random.nextInt( types.size() );
                boolean isLan = random.nextBoolean();

                long to;
                switch ( types.get( typToConnect ) ) {
                    case SERVER:
                        isLan = true;
                        to = servers.get( random.nextInt( servers.size() ) ).id;
                        break;
                    case SWITCH:
                        isLan = true;
                        to = switches.get( random.nextInt( switches.size() ) ).id;
                        break;
                    case AP:
                        to = aps.get( random.nextInt( aps.size() ) ).id;
                        break;
                    case IOT:
                        to = ioTs.get( random.nextInt( ioTs.size() ) ).id;
                        break;
                    case MOBILE:
                        to = mobiles.get( random.nextInt( mobiles.size() ) ).id;
                        break;
                    case PC:
                        to = pcs.get( random.nextInt( pcs.size() ) ).id;
                        break;
                    default:
                        throw new RuntimeException( "Not correctly configured" );
                }

                if ( isLan ) {
                    Lan lan = new Lan( element.id, to, true, random );
                    queries.addAll( lan.asQuery() );
                    lans.add( lan );
                } else {
                    Wlan wlan = new Wlan( element.id, to, true, random );
                    queries.addAll( wlan.asQuery() );
                    wlans.add( wlan );
                }

            }

            return queries;
        }


        private <T extends GraphElement> List<Query> removeElement( GraphElement element, List<T> elements ) {
            elements.remove( element );

            if ( element instanceof Node ) {
                // we need to remove connections first
                List<Wlan> ws = this.wlans
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


        @Override
        public String toString() {
            return "Network{\n" +
                    "\tservers: " + servers.size() + " with " + summarize( servers ) + ",\n" +
                    "\tswitches: " + switches.size() + " with " + summarize( switches ) + ",\n" +
                    "\taps: " + aps.size() + " with " + summarize( aps ) + ",\n" +
                    "\tioTs: " + ioTs.size() + " with " + summarize( ioTs ) + ",\n" +
                    "\tmobiles: " + mobiles.size() + " with " + summarize( mobiles ) + ",\n" +
                    "\tpcs: " + pcs.size() + " with " + summarize( pcs ) + ",\n" +
                    "\tlans: " + lans.size() + " with " + summarize( lans ) + ",\n" +
                    "\twlans: " + wlans.size() + " with " + summarize( wlans ) + ",\n" +
                    '}';
        }


        private <E extends GraphElement> String summarize( List<E> elements ) {

            double avgGraph = elements.stream().mapToInt( e -> e.getDynProperties().size() ).average().orElse( 0d );
            double avgDoc = elements.stream().filter( e -> e instanceof Node ).map( e -> (Node) e ).mapToInt( e -> e.getNestedQueries().size() ).average().orElse( 0 ); // only Node do have this
            return "{ avgGraphProps: " + Precision.round( avgGraph, 2 ) + ", avgDocProps: " + Precision.round( avgDoc, 2 ) + " }";
        }


        public String createRandomTimestamp() {
            long date = random.nextLong( startDay, endDay );
            return LocalDate.ofEpochDay( date ).toString();
        }

    }

    //// Backbone

    //// Distributors

    //// Devices

    //// Connections


    enum Connection {
        WLAN,
        LAN
    }


    public enum Device {
        SERVER,
        SWITCH,
        AP,
        IOT,
        MOBILE,
        PC
    }

}
