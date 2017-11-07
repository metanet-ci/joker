package cs.bilkent.joker.experiment.wordcount;

import java.util.List;

import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
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
    public SchedulingStrategy init ( final InitializationContext context )
    {
        outputSchema = context.getOutputPortSchema( 0 );
        partitionFieldNames = context.getPartitionFieldNames();
        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public void invoke ( final InvocationContext context )
    {
        final Tuples input = context.getInput();
        final Tuples output = context.getOutput();
        final KVStore kvStore = context.getKVStore();

        final int count = kvStore.getIntegerOrDefault( COUNT_FIELD, 0 ) + input.getTupleCount( 0 );
        kvStore.set( COUNT_FIELD, count );

        final Tuple result = new Tuple( outputSchema );

        final Tuple keyTuple = input.getTupleOrFail( 0, 0 );
        for ( int i = 0; i < partitionFieldNames.size(); i++ )
        {
            final String partitionFieldName = partitionFieldNames.get( i );
            result.set( partitionFieldName, keyTuple.get( partitionFieldName ) );
        }

        result.set( COUNT_FIELD, count );

        output.add( result );
    }

}
