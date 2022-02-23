package org.polypheny.simpleclient.cli;


import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Command(name = "dump", description = "Dump tupels of an entity to a file.")
public class DumpCommand implements CliRunnable {

    @SuppressWarnings("SpringAutowiredFieldsWarningInspection")
    @Inject
    private HelpOption<GavelCommand> help;

    @Arguments(description = "Entity Name")
    private List<String> args;

    @Option(name = { "-pdb", "--polyphenydb" }, title = "IP or Hostname", arity = 1, description = "IP or Hostname of  Polypheny-DB (default: 127.0.0.1).")
    public static String polyphenyDbHost = "127.0.0.1";


    @Override
    public int run() {
        if ( args == null || args.size() != 1 ) {
            System.err.println( "Missing entity name" );
            System.exit( 1 );
        }
        String table = args.get( 0 );

        Connection conn = null;
        try {
            conn = getConnection();
            Statement s = conn.createStatement();

            String filename = table + ".dump";
            File file = new File( filename );
            if ( file.exists() && file.isFile() ) {
                file.delete();
            }
            file.createNewFile();
            FileWriter fw = new FileWriter( file );

            fw.append( "\n\n--\n-- Data for table `" + table + "`\n--\n\n" );
            s.executeQuery( "SELECT * FROM " + table );
            ResultSet rs = s.getResultSet();
            ResultSetMetaData rsMetaData = rs.getMetaData();
            int columnCount = rsMetaData.getColumnCount();
            StringBuilder postfix;
            StringBuilder prefix = new StringBuilder( "INSERT INTO " + table + " (" );
            for ( int i = 1; i <= columnCount; i++ ) {
                if ( i == columnCount ) {
                    prefix.append( rsMetaData.getColumnName( i ) ).append( ") VALUES (" );
                } else {
                    prefix.append( rsMetaData.getColumnName( i ) ).append( "," );
                }
            }
            while ( rs.next() ) {
                postfix = new StringBuilder();
                for ( int i = 1; i <= columnCount; i++ ) {
                    try {
                        postfix.append( "'" ).append( rs.getString( i )
                                .replaceAll( "\n", "\\\\n" )
                                .replaceAll( "'", "\\\\'" ) ).append( "'" );
                    } catch ( Exception e ) {
                        postfix.append( "NULL," );
                    }
                    if ( i == columnCount ) {
                        postfix.append( ");" );
                    } else {
                        postfix.append( "," );
                    }
                }
                fw.append( prefix + postfix.toString() + "\n" );
            }
            fw.flush();
            fw.close();
            rs.close();
            s.close();
        } catch ( IOException | SQLException e ) {
            log.error( "Caught exception", e );
        } finally {
            if ( conn != null ) {
                try {
                    conn.close();
                } catch ( SQLException e ) {
                    log.error( "Exception while closing connection", e );
                }
            }
        }

        return 0;
    }


    private Connection getConnection() throws SQLException {
        try {
            Class.forName( "org.polypheny.jdbc.Driver" );
        } catch ( ClassNotFoundException e ) {
            throw new RuntimeException( "Driver not found.", e );
        }

        String url = "jdbc:polypheny://" + polyphenyDbHost + "/?serialization=PROTOBUF";

        Properties props = new Properties();
        props.setProperty( "user", "pa" );

        return DriverManager.getConnection( url, props );
    }

}
