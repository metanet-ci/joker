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
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import cs.bilkent.joker.utils.Triple;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

@OperatorSchema( outputs = @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = LogBeaconOperator
                                                                                                                        .TIMESTAMP_FIELD_NAME, type = Long.class ),
                                                                                           @SchemaField( name = LogBeaconOperator
                                                                                                                        .HOST_FIELD_NAME,
                                                                                                   type = String.class ),
                                                                                           @SchemaField( name = LogBeaconOperator
                                                                                                                        .SERVICE_FIELD_NAME, type = String.class ),
                                                                                           @SchemaField( name = LogBeaconOperator
                                                                                                                        .MESSAGE_FIELD_NAME, type = String[].class ) } ) )
@OperatorSpec( inputPortCount = 0, outputPortCount = 1, type = STATEFUL )
public class LogBeaconOperator implements Operator
{

    static final String TIMESTAMP_FIELD_NAME = "ts";

    static final String HOST_FIELD_NAME = "host";

    static final String SERVICE_FIELD_NAME = "srvc";

    static final String MESSAGE_FIELD_NAME = "msg";


    private int batchSize;

    private LogLineGenerator generator;

    private Thread generatorThread;

    private List<Triple<String, String, String[]>> logs;

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

    private Triple<String, String, String[]> nextLog ()
    {
        final Triple<String, String, String[]> log = logs.get( idx++ );
        if ( idx == batchSize )
        {
            final List<Triple<String, String, String[]>> logs = generator.getLogs();
            if ( logs != null )
            {
                this.logs = logs;
            }

            idx = 0;
        }

        return log;
    }

    private Tuple createOutputTuple ( final long timestamp, final Triple<String, String, String[]> log )
    {
        final Tuple output = new Tuple( outputSchema );
        output.set( TIMESTAMP_FIELD_NAME, timestamp );
        output.set( HOST_FIELD_NAME, log._1 );
        output.set( SERVICE_FIELD_NAME, log._2 );
        output.set( MESSAGE_FIELD_NAME, log._3 );

        return output;
    }

    @Override
    public void shutdown ()
    {
        generator.shutdown( generatorThread );
    }

}
