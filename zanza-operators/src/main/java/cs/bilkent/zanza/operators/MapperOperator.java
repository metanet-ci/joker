package cs.bilkent.zanza.operators;

import java.util.function.Function;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.Tuples;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;


/**
 * Maps the input tuples into new output tuples with the provided mapper function.
 * Output tuples have same sequence number with their corresponding input tuples.
 */
@OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
public class MapperOperator implements Operator
{

    public static final String MAPPER_CONFIG_PARAMETER = "mapper";

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";


    private Function<Tuple, Tuple> mapper;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();

        final Function<Tuple, Tuple> userMapper = config.getOrFail( MAPPER_CONFIG_PARAMETER );
        this.mapper = tuple -> {
            final Tuple mapped = userMapper.apply( tuple );
            mapped.setSequenceNumber( tuple.getSequenceNumber() );
            return mapped;
        };
        final int tupleCount = config.getIntegerOrDefault( TUPLE_COUNT_CONFIG_PARAMETER, 1 );
        return scheduleWhenTuplesAvailableOnDefaultPort( tupleCount );
    }

    @Override
    public void invoke ( final InvocationContext invocationContext )
    {
        final Tuples input = invocationContext.getInput();
        final Tuples output = invocationContext.getOutput();

        input.getTuplesByDefaultPort().stream().map( mapper ).forEach( output::add );
    }

}
