package cs.bilkent.joker.operators;

import java.util.function.BiConsumer;

import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.runtime.TupleSchema;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;


/**
 * Maps the input tuples into new output tuples with the provided mapper function.
 */
@OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
public class MapperOperator implements Operator
{

    public static final String MAPPER_CONFIG_PARAMETER = "mapper";

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";

    public static final int DEFAULT_TUPLE_COUNT_CONFIG_VALUE = 1;


    private BiConsumer<Tuple, Tuple> mapper;

    private TupleSchema outputSchema;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();

        this.mapper = config.getOrFail( MAPPER_CONFIG_PARAMETER );
        this.outputSchema = context.getOutputPortSchema( 0 );
        final int tupleCount = config.getIntegerOrDefault( TUPLE_COUNT_CONFIG_PARAMETER, DEFAULT_TUPLE_COUNT_CONFIG_VALUE );
        return scheduleWhenTuplesAvailableOnDefaultPort( tupleCount );
    }

    @Override
    public void invoke ( final InvocationContext invocationContext )
    {
        final Tuples input = invocationContext.getInput();
        final Tuples output = invocationContext.getOutput();

        for ( Tuple tuple : input.getTuplesByDefaultPort() )
        {
            final Tuple mapped = new Tuple( outputSchema );
            mapper.accept( tuple, mapped );
            output.add( mapped );
        }
    }

}
