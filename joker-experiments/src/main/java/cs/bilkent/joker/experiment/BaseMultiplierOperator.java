package cs.bilkent.joker.experiment;

import java.util.Random;

import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.Tuple;
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
                                                                                               @SchemaField( name = "val2", type = Integer.class ) } ), outputs = @PortSchema( portIndex = 0, scope = EXTENDABLE_FIELD_SET, fields = {
        @SchemaField( name = "key1", type = Integer.class ),
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
    public SchedulingStrategy init ( final InitCtx ctx )
    {
        this.outputSchema = ctx.getOutputPortSchema( 0 );
        this.multiplicationCount = ctx.getConfig().getInteger( MULTIPLICATION_COUNT );
        this.extra = max( 1, ( multiplicationCount / 64 ) );
        i = new Random().nextInt( RANDOMIZATION_BOUND );
        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public void invoke ( final InvocationCtx ctx )
    {
        for ( Tuple input : ctx.getInputTuplesByDefaultPort() )
        {
            final Object pKey1 = input.get( "key1" );
            final Object pKey2 = input.get( "key2" );
            final Tuple result = Tuple.of( outputSchema, "key1", pKey1, "key2", pKey2 );
            result.attachTo( input );
            int sum = input.getInteger( "val1" ) + input.getInteger( "val2" );
            final int m = getMultiplicationCount();
            final double val1 = input.getDouble( "val1" );
            for ( int i = 0; i < m; i++ )
            {
                sum *= val1;
            }
            result.set( "val1", sum ).set( "val2", -sum );
            ctx.output( result );
        }

        final KVStore kvStore = ctx.getKVStore();
        if ( kvStore != null )
        {
            final int count = kvStore.getIntegerOrDefault( "count", 0 );
            kvStore.set( "count", count + ctx.getInputTupleCount( 0 ) );
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
