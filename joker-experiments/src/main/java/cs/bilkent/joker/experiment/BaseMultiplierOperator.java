package cs.bilkent.joker.experiment;

import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
import cs.bilkent.joker.operator.kvstore.KVStore;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;

public abstract class BaseMultiplierOperator implements Operator
{

    static final String MULTIPLICATION_COUNT = "multiplicationCount";


    private TupleSchema outputSchema;

    private int multiplicationCount;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        this.outputSchema = context.getOutputPortSchema( 0 );
        this.multiplicationCount = context.getConfig().getInteger( MULTIPLICATION_COUNT );
        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public void invoke ( final InvocationContext invocationContext )
    {
        Tuples input = invocationContext.getInput();
        Tuples output = invocationContext.getOutput();
        for ( Tuple tuple : input.getTuplesByDefaultPort() )
        {
            Tuple summed = new Tuple( outputSchema );
            Object pKey = tuple.get( "key" );
            summed.set( "key", pKey );
            int sum = tuple.getInteger( "val1" ) + tuple.getInteger( "val2" );
            for ( int i = 0; i < multiplicationCount; i++ )
            {
                sum *= tuple.getDouble( "val1" );
            }
            summed.set( "val1", sum );
            summed.set( "val2", -sum );
            output.add( summed );
        }

        final KVStore kvStore = invocationContext.getKVStore();
        if ( kvStore != null )
        {
            final int count = kvStore.getIntegerOrDefault( "count", 0 );
            kvStore.set( "count", count + input.getTupleCount( 0 ) );
        }
    }

}
