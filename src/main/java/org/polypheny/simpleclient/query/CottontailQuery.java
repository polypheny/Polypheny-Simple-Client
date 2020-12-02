package org.polypheny.simpleclient.query;


public class CottontailQuery {

    public final QueryType type;
    public final Object query;

    public CottontailQuery( QueryType type, Object query ) {
        this.type = type;
        this.query = query;
    }


    public enum QueryType {
        // DQL
        QUERY, // Query
        QUERY_BATCH, // List<Query>

        // DML
        INSERT, // InsertMessage
        INSERT_BATCH, // List<InsertMessage>
        UPDATE, // UpdateMessage
        DELETE, // DeleteMessage

        // DDL
        SCHEMA_CREATE, // Schema
        SCHEMA_DROP, // Schema

        ENTITY_CREATE, // EntityDefintion
        ENTITY_DROP, // Entity

        TRUNCATE // Entity
    }
}
