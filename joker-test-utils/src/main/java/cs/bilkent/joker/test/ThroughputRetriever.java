package cs.bilkent.joker.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ThroughputRetriever
{
    private static final String TEST_OUTPUT_FILE_PATH_TEMPLATE = "target/surefire-reports/%s-output.txt";
    private static final String THROUGHPUT_RETRIEVER_FILE_PATH = "../joker-engine/src/test/resources/grepThroughput.sh";

    private List<Integer> lastThroughputValueCounts;
    private final List<String> pipelineSpecs;
    private final String testOutputFilePath;

    public ThroughputRetriever ( final List<String> pipelineSpecs, final Class<?> testClass ) throws Exception
    {
        this.pipelineSpecs = pipelineSpecs;
        testOutputFilePath = String.format( TEST_OUTPUT_FILE_PATH_TEMPLATE, testClass.getCanonicalName() );
        lastThroughputValueCounts = new ArrayList<>( pipelineSpecs.size() );
        for ( String pipelineSpec : pipelineSpecs )
        {
            lastThroughputValueCounts.add( retrieveThroughputValues( pipelineSpec ).size() );
        }
    }

    public double retrieveThroughput () throws Exception
    {
        List<Integer> t = new ArrayList<>();
        double throughput = 0;
        for ( int i = 0; i < pipelineSpecs.size(); i++ )
        {
            final List<Double> throughputValues = retrieveThroughputValues( pipelineSpecs.get( i ) );
            final int currentThroughputValueCount = throughputValues.size();
            t.add( currentThroughputValueCount );
            final int numNewThroughputValueCount = currentThroughputValueCount - lastThroughputValueCounts.get( i );
            if ( numNewThroughputValueCount == 0 )
            {
                throw new RuntimeException( "failed to find new throughput values" );
            }
            throughput += throughputValues.subList( lastThroughputValueCounts.get( i ), currentThroughputValueCount )
                                          .stream()
                                          .mapToDouble( Double::doubleValue )
                                          .average()
                                          .orElse( 0d );
        }

        lastThroughputValueCounts.clear();
        lastThroughputValueCounts.addAll( t );
        return throughput;
    }

    private static File createTempFile ( final String prefix, final String use ) throws Exception
    {
        try
        {
            return File.createTempFile( prefix, "txt" );
        }
        catch ( final IOException e )
        {
            throw new Exception( String.format( "failed to create a temporary file for %s", use ), e );
        }
    }

    private List<Double> retrieveThroughputValues ( final String pipelineSpec ) throws Exception
    {
        final File outputFile = createTempFile( "standardErrorAndOutput-", "throughput retriever standard output/error" );
        try ( final AutoCloseable onClose1 = outputFile::delete )
        {
            final File throughputFile = createTempFile( "throughput-", "throughput values" );
            try ( final AutoCloseable onClose2 = throughputFile::delete )
            {
                return retrieveThroughputValues( pipelineSpec, outputFile, throughputFile );
            }
            catch ( final IOException e )
            {
                throw new Exception( "failed to create a throughput file for the throughput retriever", e );
            }
        }
    }

    private List<Double> retrieveThroughputValues ( final String pipelineSpec,
                                                    final File outputFile,
                                                    final File throughputFile ) throws Exception
    {
        final Process process;
        final String[] commandLine = { THROUGHPUT_RETRIEVER_FILE_PATH, testOutputFilePath, pipelineSpec, throughputFile.toString() };
        try
        {
            process = new ProcessBuilder().command( commandLine )
                                          .inheritIO()
                                          .redirectErrorStream( true )
                                          .redirectOutput( outputFile )
                                          .start();
        }
        catch ( final IOException e )
        {
            throw new Exception( String.format( "failed to launch the throughput retriever process with the command line: %s",
                                                Arrays.toString( commandLine ) ), e );
        }
        final int exitValue;
        try
        {
            exitValue = process.waitFor();
        }
        catch ( final InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new Exception( "interrupted while waiting for the throughput retriever to complete", e );
        }
        if ( exitValue != 0 )
        {
            String outputString;
            try
            {
                outputString = readFileContents( outputFile );
            }
            catch ( final IOException e )
            {
                outputString = "<failed to retrieve the script output>";
            }
            throw new Exception( String.format(
                    "failed to execute the throughput retriever process, the exit value was %d, the combined standard output/error "
                    + "was:\n%s",
                    exitValue,
                    outputString ) );
        }
        final String throughputOutputString;
        try
        {
            throughputOutputString = readFileContents( throughputFile );
        }
        catch ( final IOException e )
        {
            throw new Exception( String.format( "failed to read the throughput file '%s'", throughputFile ), e );
        }
        final String[] throughputStrings = throughputOutputString.split( System.lineSeparator() );
        return Arrays.stream( throughputStrings ).map( Double::parseDouble ).collect( Collectors.toList() );
    }

    private static String readFileContents ( final File file ) throws IOException
    {
        return new String( Files.readAllBytes( file.toPath() ), StandardCharsets.UTF_8 );
    }
}
