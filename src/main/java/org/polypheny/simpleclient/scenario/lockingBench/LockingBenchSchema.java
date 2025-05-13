/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/21/25, 1:39 PM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.lockingBench;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.scenario.multimedia.queryBuilder.CreateTable;
import org.polypheny.simpleclient.scenario.lockingBench.queryBuilder.CreateNamespace;

@Getter
public class LockingBenchSchema {

    private final int namespaceCount;
    private final int entitiesPerNamespace;
    private final List<Entity> entities;


    public LockingBenchSchema( LockingBenchConfig config ) {
        this.namespaceCount = config.namespaceCount;
        this.entitiesPerNamespace = config.entityCount;
        this.entities = initializeEntities();
    }


    private List<Entity> initializeEntities() {
        AtomicInteger entryIndexGenerator = new AtomicInteger( 0 );
        List<Entity> entities = new LinkedList<>();
        for ( int namespaceIndex = 0; namespaceIndex < namespaceCount; namespaceIndex++ ) {
            for ( int j = 0; j < entitiesPerNamespace; j++ ) {
                entities.add( new Entity( namespaceIndex, entryIndexGenerator.getAndIncrement() ) );
            }
        }
        return entities;
    }

    public List<Query> getCrateNamespaceQueries() {
        String createNamespaceTemplate = "CREATE RELATIONAL NAMESPACE %s";

        return entities.stream()
                .map( Entity::getNamespaceName )
                .distinct()
                .map( n -> new CreateNamespace( String.format( createNamespaceTemplate, n ) ).getNewQuery() )
                .toList();
    }

    public List<Query> getCrateTableQueries() {
        String createTableTemplate = """
                CREATE TABLE %s (
                    id BIGINT PRIMARY KEY,
                    category_id INT,
                    score DECIMAL(10, 2),
                    is_verified BOOLEAN
                );
                """;

        return entities.stream()
                .map( Entity::getFullName )
                .map( f -> new CreateTable( String.format( createTableTemplate, f ) ).getNewQuery() )
                .toList();
    }


    public String getRandomEntityFullName() {
        return entities.get( ThreadLocalRandom.current().nextInt( entities.size() ) ).getFullName();
    }
}
