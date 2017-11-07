package cs.bilkent.joker.experiment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXTENDABLE_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.util.Collections.shuffle;

@OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
@OperatorSchema( outputs = @PortSchema( portIndex = 0, scope = EXTENDABLE_FIELD_SET, fields = { @SchemaField( name = "key1", type = Integer.class ),
                                                                                                @SchemaField( name = "key2", type = Integer.class ),
                                                                                                @SchemaField( name = "val1", type = Integer.class ),
                                                                                                @SchemaField( name = "val2", type = Integer.class ) } ) )
public class MemorizingBeaconOperator implements Operator
{

    private static final Logger LOGGER = LoggerFactory.getLogger( MemorizingBeaconOperator.class );

    private static final int MEMORIZED_INVOCATION_COUNT = 2000;

    static final String KEY_RANGE_CONFIG_PARAMETER = "keyRange";

    static final String VALUE_RANGE_CONFIG_PARAMETER = "valueRange";

    static final String TUPLES_PER_KEY_CONFIG_PARAMETER = "tuplesPerKey";

    static final String KEYS_PER_INVOCATION_CONFIG_PARAMETER = "keysPerInvocation";

    private TupleSchema outputSchema;

    private int tuplesPerKey;

    private int keysPerInvocation;

    private int keyRange;

    private int valueRange;

    private int[] keys;

    private int keyIndex;

    private int value;

    private List<List<Tuple>> outputs = new ArrayList<>( MEMORIZED_INVOCATION_COUNT );

    private List<List<Tuple>> currentOutputs;

    private AtomicReference<List<List<Tuple>>> shuffledOutputsRef = new AtomicReference<>();

    private int inv;

    private Thread shuffler;

    private volatile boolean shutdown;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        this.outputSchema = context.getOutputPortSchema( 0 );
        final OperatorConfig config = context.getConfig();
        this.tuplesPerKey = config.getInteger( TUPLES_PER_KEY_CONFIG_PARAMETER );
        this.keysPerInvocation = config.getInteger( KEYS_PER_INVOCATION_CONFIG_PARAMETER );
        this.valueRange = config.getInteger( VALUE_RANGE_CONFIG_PARAMETER );
        this.keyRange = config.getInteger( KEY_RANGE_CONFIG_PARAMETER );

        final List<Integer> keys = new ArrayList<>( keyRange );
        for ( int i = 0; i < keyRange; i++ )
        {
            keys.add( i );
        }

        shuffle( keys );

        this.keys = new int[ keyRange ];
        for ( int i = 0; i < keyRange; i++ )
        {
            this.keys[ i ] = keys.get( i );
        }

        for ( int i = 0; i < MEMORIZED_INVOCATION_COUNT; i++ )
        {
            outputs.add( produceOutput() );
        }

        currentOutputs = new ArrayList<>( outputs );

        shuffler = new Thread( new Shuffler() );
        shuffler.start();

        return ScheduleWhenAvailable.INSTANCE;
    }

    @Override
    public void invoke ( final InvocationContext context )
    {
        final Tuples output = context.getOutput();
        if ( inv == currentOutputs.size() )
        {
            final List<List<Tuple>> newShuffledOutputs = shuffledOutputsRef.get();
            if ( newShuffledOutputs != null && shuffledOutputsRef.compareAndSet( newShuffledOutputs, null ) )
            {
                currentOutputs = newShuffledOutputs;
            }

            inv = 0;
        }

        output.addAll( currentOutputs.get( inv++ ) );
    }

    private List<Tuple> produceOutput ()
    {
        final List<Tuple> output = new ArrayList<>();
        for ( int i = 0; i < keysPerInvocation; i++ )
        {
            final int key = keys[ keyIndex ];
            keyIndex = ++keyIndex % keyRange;
            for ( int j = 0; j < tuplesPerKey; j++ )
            {
                final Tuple tuple = new Tuple( outputSchema );
                tuple.set( "key1", key );
                tuple.set( "key2", key );
                tuple.set( "val1", value );
                value = value++ % valueRange;
                tuple.set( "val2", value );
                output.add( tuple );
            }
        }

        return output;
    }

    @Override
    public void shutdown ()
    {
        shutdown = true;
        try
        {
            shuffler.join( TimeUnit.SECONDS.toMillis( 30 ) );
        }
        catch ( InterruptedException e )
        {
            currentThread().interrupt();
            LOGGER.error( "shuffler join failed" );
        }
    }

    private class Shuffler implements Runnable
    {

        @Override
        public void run ()
        {
            while ( !shutdown )
            {
                final List<List<Tuple>> newShuffle = new ArrayList<>( outputs );
                shuffle( newShuffle );

                while ( !shuffledOutputsRef.compareAndSet( null, newShuffle ) )
                {
                    try
                    {
                        if ( shutdown )
                        {
                            break;
                        }

                        sleep( 1 );
                    }
                    catch ( InterruptedException e )
                    {
                        currentThread().interrupt();
                    }
                }

                LOGGER.info( "Shuffled..." );
            }
        }
    }

}
