package org.polypheny.simpleclient.scenario.parquet;

import com.google.gson.Gson;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor;
import org.polypheny.simpleclient.query.RawQuery;


/** Deploys missing Parquet sources through the existing JDBC executor. */
public class ParquetSetup {

    private final Properties properties;
    private final PolyphenyDbJdbcExecutor executor;


    public ParquetSetup( ParquetConfig config, PolyphenyDbJdbcExecutor executor ) {
        this.properties = config.properties;
        this.executor = executor;
    }


    public void run() throws ExecutorException {
        List<Source> sources = readSources();
        for ( Source source : sources ) {
            validateDirectory( source.directory() );
            for ( String table : source.tables() ) {
                validateDirectory( source.directory().resolve( table ) );
            }
        }

        for ( Source source : sources ) {
            boolean exists = sourceExists( source );

            if ( !exists ) {
                deploy( source );
                // Keep completed deployments if a later source fails.
                executor.executeCommit();
            }
            validateTables( source );
            System.out.println( (exists ? "Reusing" : "Deployed") + " Parquet source: " + source.name() );
        }
        executor.executeCommit();
        System.out.println( "Parquet setup complete. All required tables and collections are available." );
    }


    private boolean sourceExists( Source source ) throws ExecutorException {
        if ( source.adapter().equals( "Parquet Document" ) ) {
            return executor.namespaceExists( source.namespace() );
        }
        for ( String table : executor.getTableNames( source.namespace() ) ) {
            if ( table.startsWith( source.name() + "__" ) ) {
                return true;
            }
        }
        return false;
    }


    private void deploy( Source source ) throws ExecutorException {
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put( "mode", "embedded" );
        settings.put( "method", "link" );
        settings.put( "directoryName", source.directory().toString() );
        // Polypheny requires these settings even when using a local directory.
        settings.put( "directory", "[]" );
        settings.put( "url", "" );
        if ( !source.schemaMode().isEmpty() ) {
            settings.put( "schema mode", source.schemaMode() );
        }
        String json = new Gson().toJson( settings );
        // The JSON is embedded in an SQL string literal.
        executor.deployAdapter( source.name(), source.adapter(), "SOURCE", json.replace( "'", "''" ) );
    }


    private void validateTables( Source source ) throws ExecutorException {
        Set<String> tables = source.adapter().equals( "Parquet Document" )
                ? Set.of() : executor.getTableNames( source.namespace() );
        for ( String table : source.tables() ) {
            String name = source.name() + "__" + table;
            if ( source.adapter().equals( "Parquet Document" ) ) {
                // Document collections are absent from JDBC metadata. SQL can also query documents.
                String sql = "SELECT * FROM \"" + source.namespace().replace( "\"", "\"\"" )
                        + "\".\"" + name + "\" LIMIT 0";
                try {
                    executor.executeQuery( new RawQuery( sql, null, true ) );
                } catch ( ExecutorException e ) {
                    throw new IllegalStateException( "Could not access " + source.namespace() + "." + name
                            + ". Check the source and its data; an existing namespace alone does not guarantee a deployed source.", e );
                }
            } else if ( !tables.contains( name ) ) {
                throw new IllegalStateException( "Missing table: " + source.namespace() + "." + name
                        + ". Check the dataset and filenames, then redeploy the source manually if needed." );
            }
        }
    }


    private List<Source> readSources() {
        List<Source> sources = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for ( String name : csv( property( "setup.sources" ) ) ) {
            if ( !name.matches( "[a-z][a-z0-9_]*" ) || !names.add( name ) ) {
                throw new IllegalArgumentException( "Invalid or duplicate source name: " + name );
            }
            String prefix = "source." + name + ".";
            String adapter = property( prefix + "adapter" );
            String schemaMode = properties.getProperty( prefix + "schemaMode", "" ).trim();
            if ( "Parquet Relational".equals( adapter ) ) {
                if ( !List.of( "flat", "normalized" ).contains( schemaMode ) ) {
                    throw new IllegalArgumentException( "Expected flat or normalized schemaMode for " + name );
                }
            } else if ( !"Parquet Document".equals( adapter ) || !schemaMode.isEmpty() ) {
                throw new IllegalArgumentException( "Unsupported adapter or schemaMode for " + name );
            }
            List<String> tables = csv( property( prefix + "tables" ) );
            for ( String table : tables ) {
                if ( !table.matches( "[a-z][a-z0-9_]*" ) ) {
                    throw new IllegalArgumentException( "Invalid setup table name: " + table );
                }
            }
            sources.add( new Source( name, adapter,
                    Path.of( property( prefix + "directory" ) ).toAbsolutePath().normalize(),
                    schemaMode, property( prefix + "namespace" ),
                    tables ) );
        }
        return sources;
    }


    private void validateDirectory( Path directory ) {
        if ( !Files.isDirectory( directory ) || !Files.isReadable( directory ) ) {
            throw new IllegalArgumentException( "Parquet directory is missing or unreadable: " + directory
                    + ". Prepare the data first; setup does not download or generate files." );
        }
        try ( var files = Files.walk( directory ) ) {
            if ( files.noneMatch( path -> Files.isRegularFile( path ) && path.toString().endsWith( ".parquet" ) ) ) {
                throw new IllegalArgumentException( "No Parquet files found in " + directory );
            }
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not inspect " + directory, e );
        }
    }


    private String property( String name ) {
        String value = properties.getProperty( name );
        if ( value == null || value.isBlank() ) {
            throw new IllegalArgumentException( "Missing setup property: " + name );
        }
        return value.trim();
    }


    private List<String> csv( String value ) {
        List<String> values = new ArrayList<>();
        for ( String item : value.split( ",", -1 ) ) {
            if ( item.isBlank() ) {
                throw new IllegalArgumentException( "Empty item in setup list: " + value );
            }
            values.add( item.trim() );
        }
        return values;
    }


    private record Source( String name, String adapter, Path directory, String schemaMode,
                           String namespace, List<String> tables ) {
    }
}
