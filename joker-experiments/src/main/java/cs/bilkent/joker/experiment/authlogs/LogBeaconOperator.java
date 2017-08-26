package cs.bilkent.joker.experiment.authlogs;

import java.util.List;

import static cs.bilkent.joker.experiment.authlogs.LogFileReader.readFile;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchemaScope;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

@OperatorSchema( outputs = @PortSchema( portIndex = 0, scope = PortSchemaScope.EXACT_FIELD_SET, fields = { @SchemaField( name = LogBeaconOperator.LINE_FIELD_NAME, type = String.class ) } ) )
@OperatorSpec( inputPortCount = 0, outputPortCount = 1, type = STATEFUL )
public class LogBeaconOperator implements Operator
{

    static final String LINE_FIELD_NAME = "line";


    private int batchSize;

    private LogLineGenerator generator;

    private Thread generatorThread;

    private List<String> logs;

    private TupleSchema outputSchema;

    private int idx;

    private long currentTimestamp = System.currentTimeMillis();

    private int logsPerSecond;

    private int count;

    private int tuplesPerInvocation;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();
        final List<String> lines = readFile( config.get( "filePath" ) );
        batchSize = config.get( "batchSize" );
        final int uidRange = config.get( "uidRange" );
        final int euidRange = config.get( "euidRange" );
        final int rhostCount = config.get( "rhostCount" );
        final int userCount = config.get( "userCount" );
        final double authFailureRatio = config.get( "authFailureRatio" );
        logsPerSecond = config.get( "logsPerSecond" );
        tuplesPerInvocation = config.get( "tuplesPerInvocation" );

        generator = new LogLineGenerator( lines, batchSize, uidRange, euidRange, rhostCount, userCount, authFailureRatio );
        generatorThread = new Thread( generator );
        generatorThread.start();

        while ( logs == null )
        {
            parkNanos( 1000 );
            logs = generator.getLogs();
        }

        outputSchema = context.getOutputPortSchema( 0 );

        return ScheduleWhenAvailable.INSTANCE;
    }

    @Override
    public void invoke ( final InvocationContext context )
    {
        final Tuples output = context.getOutput();
        for ( int i = 0; i < tuplesPerInvocation; i++ )
        {
            output.add( createOutputTuple( nextTimestamp(), nextLog() ) );
        }
    }

    private long nextTimestamp ()
    {
        if ( count == logsPerSecond )
        {
            count = 0;
            currentTimestamp += SECONDS.toMillis( 1 );
        }
        else
        {
            count++;
        }

        return currentTimestamp;
    }

    private String nextLog ()
    {
        final String log = logs.get( idx++ );
        if ( idx == batchSize )
        {
            final List<String> logs = generator.getLogs();
            if ( logs != null )
            {
                this.logs = logs;
            }

            idx = 0;
        }

        return log;
    }

    private Tuple createOutputTuple ( final long timestamp, final String log )
    {
        final Tuple tuple = new Tuple( outputSchema );
        final String line = timestamp + " " + log;
        tuple.set( LINE_FIELD_NAME, line );

        return tuple;
    }

    @Override
    public void shutdown ()
    {
        generator.shutdown( generatorThread );
    }

}
