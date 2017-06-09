package cs.bilkent.joker.experiment;

import java.util.Random;

import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
import cs.bilkent.joker.operator.kvstore.KVStore;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXTENDABLE_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import static java.lang.Math.max;

@OperatorSchema( inputs = @PortSchema( portIndex = 0, scope = EXTENDABLE_FIELD_SET, fields = { @SchemaField( name = "key1", type = Integer.class ),
                                                                                               @SchemaField( name = "key2", type = Integer.class ),
                                                                                               @SchemaField( name = "val1", type = Integer.class ),
                                                                                               @SchemaField( name = "val2", type = Integer.class ) } ), outputs = @PortSchema( portIndex = 0, scope = EXTENDABLE_FIELD_SET, fields = { @SchemaField( name = "key1", type = Integer.class ),
                                                                                                                                                                                                                                       @SchemaField( name = "key2", type = Integer.class ),
                                                                                                                                                                                                                                       @SchemaField( name = "val1", type = Integer.class ),
                                                                                                                                                                                                                                       @SchemaField( name = "val2", type = Integer.class ) } ) )
public abstract class BaseMultiplierOperator implements Operator
{

    static final String MULTIPLICATION_COUNT = "multiplicationCount";

    private static final int RANDOMIZATION_BOUND = 16;


    private TupleSchema outputSchema;

    private int multiplicationCount;

    private int extra;

    private int i;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        this.outputSchema = context.getOutputPortSchema( 0 );
        this.multiplicationCount = context.getConfig().getInteger( MULTIPLICATION_COUNT );
        this.extra = max( 1, ( multiplicationCount / 64 ) );
        i = new Random().nextInt( RANDOMIZATION_BOUND );
        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public void invoke ( final InvocationContext invocationContext )
    {
        Tuples input = invocationContext.getInput();
        Tuples output = invocationContext.getOutput();
        for ( Tuple tuple : input.getTuplesByDefaultPort() )
        {
            final Tuple result = new Tuple( outputSchema );
            final Object pKey1 = tuple.get( "key1" );
            final Object pKey2 = tuple.get( "key2" );
            result.set( "key1", pKey1 );
            result.set( "key2", pKey2 );
            int sum = tuple.getInteger( "val1" ) + tuple.getInteger( "val2" );
            final int m = getMultiplicationCount();
            final double val1 = tuple.getDouble( "val1" );
            for ( int i = 0; i < m; i++ )
            {
                sum *= val1;
            }
            result.set( "val1", sum );
            result.set( "val2", -sum );
            output.add( result );
        }

        final KVStore kvStore = invocationContext.getKVStore();
        if ( kvStore != null )
        {
            final int count = kvStore.getIntegerOrDefault( "count", 0 );
            kvStore.set( "count", count + input.getTupleCount( 0 ) );
        }
    }

    private int getMultiplicationCount ()
    {
        if ( i == RANDOMIZATION_BOUND )
        {
            i = 0;
            return multiplicationCount + extra;
        }
        else
        {
            i++;
            return multiplicationCount;
        }
    }

}
