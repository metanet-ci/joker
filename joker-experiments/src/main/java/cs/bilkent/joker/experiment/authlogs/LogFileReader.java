package cs.bilkent.joker.experiment.authlogs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

final class LogFileReader
{

    private LogFileReader ()
    {
    }

    public static void main ( String[] args )
    {
        List<String> lines = readFile( "/Users/ebkahveci/Dev/Personal/Repos/joker/joker-experiments/authlogs.txt" );
        lines.forEach( System.out::println );
    }

    static List<String> readFile ( final String path )
    {
        try
        {
            return Files.lines( Paths.get( path ) )
                        .filter( line -> line.startsWith( "Aug" ) && ( !line.contains( "sshd" )
                                                                       || !line.contains( "authentication failure;" ) ) )
                        .map( line -> Arrays.stream( line.split( " " ) ).skip( 3 ).collect( joining( " " ) ) )
                        .collect( toList() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

}
