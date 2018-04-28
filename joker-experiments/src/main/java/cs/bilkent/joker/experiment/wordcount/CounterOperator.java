package cs.bilkent.joker.experiment.wordcount;

import java.util.List;

import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.kvstore.KVStore;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;

@OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
public class CounterOperator implements Operator
{

    public static final String COUNT_FIELD = "count";

    private TupleSchema outputSchema;

    private List<String> partitionFieldNames;

    @Override
    public SchedulingStrategy init ( final InitCtx ctx )
    {
        outputSchema = ctx.getOutputPortSchema( 0 );
        partitionFieldNames = ctx.getPartitionFieldNames();
        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public void invoke ( final InvocationCtx ctx )
    {
        final KVStore kvStore = ctx.getKVStore();

        final int inputTupleCount = ctx.getInputTupleCount( 0 );
        final int count = kvStore.getIntegerOrDefault( COUNT_FIELD, 0 ) + inputTupleCount;
        kvStore.set( COUNT_FIELD, count );

        final Tuple result = new Tuple( outputSchema );
        if ( inputTupleCount > 0 )
        {
            result.attachTo( ctx.getInputTupleOrFail( 0, 0 ) );
        }

        final Tuple keyTuple = ctx.getInputTupleOrFail( 0, 0 );
        for ( int i = 0; i < partitionFieldNames.size(); i++ )
        {
            final String partitionFieldName = partitionFieldNames.get( i );
            result.set( partitionFieldName, keyTuple.get( partitionFieldName ) );
        }

        result.set( COUNT_FIELD, count );

        ctx.output( result );
    }

}
