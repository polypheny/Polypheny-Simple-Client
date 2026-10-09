package org.polypheny.simpleclient.scenario.parquet;

import java.io.File;
import java.io.IOException;
import java.io.PushbackReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;


/** Checks the existing benchmark CSV without changing the original runners. */
public class ParquetResults {

    public static void check( File csvFile, int runs ) {
        int measured = 0;
        int failed = 0;
        try ( PushbackReader reader = new PushbackReader( Files.newBufferedReader( csvFile.toPath() ) ) ) {
            List<String> header = readRow( reader );
            if ( !header.equals( List.of( "timestamp", "query_id", "description", "phase", "run",
                    "elapsed_ms", "rows", "columns", "success", "error" ) ) ) {
                throw new IllegalStateException( "Unexpected Parquet CSV header: " + csvFile );
            }
            List<String> row;
            while ( !(row = readRow( reader )).isEmpty() ) {
                if ( row.size() != header.size() || !List.of( "true", "false" ).contains( row.get( 8 ) ) ) {
                    throw new IllegalStateException( "Invalid Parquet result row: " + csvFile );
                }
                if ( "measured".equals( row.get( 3 ) ) ) {
                    measured++;
                }
                if ( "false".equals( row.get( 8 ) ) ) {
                    failed++;
                }
            }
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not read Parquet results: " + csvFile.getAbsolutePath(), e );
        }
        if ( measured < runs || measured % runs != 0 || failed > 0 ) {
            throw new IllegalStateException( "Parquet results: " + measured + " measured runs, " + failed
                    + " failures. See " + csvFile.getAbsolutePath() );
        }
    }


    // Descriptions and error messages can contain commas, quotes and line breaks.
    private static List<String> readRow( PushbackReader reader ) throws IOException {
        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        int character;
        while ( (character = reader.read()) != -1 ) {
            if ( character == '"' ) {
                if ( quoted ) {
                    int next = reader.read();
                    if ( next == '"' ) {
                        field.append( '"' );
                    } else {
                        quoted = false;
                        if ( next != -1 ) {
                            reader.unread( next );
                        }
                    }
                } else {
                    quoted = true;
                }
            } else if ( character == ',' && !quoted ) {
                fields.add( field.toString() );
                field.setLength( 0 );
            } else if ( character == '\n' && !quoted ) {
                fields.add( field.toString() );
                return fields;
            } else if ( character != '\r' || quoted ) {
                field.append( (char) character );
            }
        }
        if ( quoted ) {
            throw new IOException( "Unterminated quoted CSV field" );
        }
        if ( !fields.isEmpty() || !field.isEmpty() ) {
            fields.add( field.toString() );
        }
        return fields;
    }
}
