package cs.bilkent.joker.operators;

import java.util.List;
import java.util.function.BiConsumer;

import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;


/**
 * Maps the input tuples into new output tuples with the provided mapper function.
 */
@OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
public class PartitionedMapperOperator implements Operator
{

    public static final String MAPPER_CONFIG_PARAMETER = "mapper";

    private static final int DEFAULT_TUPLE_COUNT_CONFIG_VALUE = 1;


    private BiConsumer<Tuple, Tuple> mapper;

    private TupleSchema outputSchema;

    @Override
    public SchedulingStrategy init ( final InitCtx ctx )
    {
        final OperatorConfig config = ctx.getConfig();

        this.mapper = config.getOrFail( MAPPER_CONFIG_PARAMETER );
        this.outputSchema = ctx.getOutputPortSchema( 0 );
        return scheduleWhenTuplesAvailableOnDefaultPort( DEFAULT_TUPLE_COUNT_CONFIG_VALUE );
    }

    @Override
    public void invoke ( final InvocationCtx ctx )
    {
        final List<Tuple> tuples = ctx.getInputTuplesByDefaultPort();
        for ( int i = 0, j = tuples.size(); i < j; i++ )
        {
            final Tuple input = tuples.get( i );
            // TODO provide output schema to the Tuple c'tor
            final Tuple result = new Tuple();
            mapper.accept( input, result );
            result.attachTo( input );
            ctx.output( result );
        }
    }

}
