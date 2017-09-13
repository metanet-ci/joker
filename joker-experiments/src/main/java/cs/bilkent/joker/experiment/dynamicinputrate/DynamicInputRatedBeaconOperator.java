package cs.bilkent.joker.experiment.dynamicinputrate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Queue;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import com.google.common.base.Charsets;

import cs.bilkent.joker.experiment.MemorizingBeaconOperator;
import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXTENDABLE_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;

@OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
@OperatorSchema( outputs = @PortSchema( portIndex = 0, scope = EXTENDABLE_FIELD_SET, fields = { @SchemaField( name = "key1", type = Integer.class ),
                                                                                                @SchemaField( name = "key2", type = Integer.class ),
                                                                                                @SchemaField( name = "val1", type = Integer.class ),
                                                                                                @SchemaField( name = "val2", type = Integer.class ) } ) )
public class DynamicInputRatedBeaconOperator implements Operator
{

    public static final String TOKEN_COUNT_CONFIG_PARAMETER = "tokenCount";

    public static final String PERIOD_IN_MILLIS_CONFIG_PARAMETER = "periodInMillis";

    private static volatile int TOKEN_COUNT;

    private static volatile boolean DISABLE_TOKENS;


    private Queue<Long> tokenQueue;

    private int tokenCount;

    private long periodInMillis;

    private Operator beaconOperator;

    private volatile boolean running = true;

    private Thread commandThread;


    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();
        this.tokenCount = config.getInteger( TOKEN_COUNT_CONFIG_PARAMETER );
        this.tokenQueue = new CircularFifoQueue<>( tokenCount );
        this.periodInMillis = config.getLong( PERIOD_IN_MILLIS_CONFIG_PARAMETER );
        TOKEN_COUNT = this.tokenCount;

        this.beaconOperator = new MemorizingBeaconOperator();
        this.commandThread = createCommanderThread();
        this.commandThread.start();

        return this.beaconOperator.init( context );
    }

    @Override
    public void invoke ( final InvocationContext context )
    {
        if ( tryAcquireToken() )
        {
            beaconOperator.invoke( context );
        }
    }

    private boolean tryAcquireToken ()
    {
        if ( DISABLE_TOKENS )
        {
            return true;
        }

        checkTokenCount();

        final long currentTime = System.currentTimeMillis();
        if ( tokenQueue.size() < tokenCount || ( currentTime - tokenQueue.peek() ) >= periodInMillis )
        {
            tokenQueue.add( currentTime );
            return true;
        }

        return false;
    }

    private void checkTokenCount ()
    {
        if ( TOKEN_COUNT != tokenCount )
        {
            this.tokenCount = TOKEN_COUNT;
            this.tokenQueue = new CircularFifoQueue<>( tokenCount );
        }
    }

    @Override
    public void shutdown ()
    {
        running = false;
        beaconOperator.shutdown();
        try
        {
            commandThread.join();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
    }

    private Thread createCommanderThread ()
    {
        return new Thread( () -> {
            String line;
            final BufferedReader reader = new BufferedReader( new InputStreamReader( System.in, Charsets.UTF_8 ) );
            try
            {
                while ( running && ( line = reader.readLine() ) != null )
                {
                    final String command = line.trim();
                    if ( command.isEmpty() )
                    {
                        continue;
                    }

                    if ( "inc".equals( command ) )
                    {
                        TOKEN_COUNT = (int) ( 1.5 * TOKEN_COUNT );
                        System.out.println( "TOKEN COUNT INCREASED." );
                    }
                    else if ( "dis".equals( command ) )
                    {
                        DISABLE_TOKENS = true;
                        System.out.println( "TOKEN COUNT DISABLED." );
                    }
                    else
                    {
                        System.out.println( "WRONG COMMAND" );
                    }
                }
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        } );
    }

}
