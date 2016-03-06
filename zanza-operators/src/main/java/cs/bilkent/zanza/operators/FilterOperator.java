package cs.bilkent.zanza.operators;

import java.util.function.Predicate;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import cs.bilkent.zanza.operator.spec.OperatorType;
import cs.bilkent.zanza.scheduling.ScheduleNever;
import cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.ANY_NUMBER_OF_TUPLES;
import static cs.bilkent.zanza.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.scheduling.SchedulingStrategy;


/**
 * Applies the given predicate function to each input tuple and only returns the ones that satisfy the predicate
 */
@OperatorSpec( type = OperatorType.STATELESS, inputPortCount = 1, outputPortCount = 1 )
public class FilterOperator implements Operator
{

    public static final String PREDICATE_CONFIG_PARAMETER = "predicate";

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";

    private Predicate<Tuple> predicate;

    private ScheduleWhenTuplesAvailable schedulingStrategy;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();

        this.predicate = config.getOrFail( PREDICATE_CONFIG_PARAMETER );
        final int tupleCount = config.getIntegerOrDefault( TUPLE_COUNT_CONFIG_PARAMETER, ANY_NUMBER_OF_TUPLES );
        schedulingStrategy = scheduleWhenTuplesAvailableOnDefaultPort( tupleCount );
        return schedulingStrategy;
    }

    @Override
    public InvocationResult invoke ( final InvocationContext invocationContext )
    {
        final PortsToTuples output = invocationContext.getInputTuples()
                                                      .getTuplesByDefaultPort()
                                                      .stream()
                                                      .filter( predicate )
                                                      .collect( PortsToTuples.COLLECT_TO_DEFAULT_PORT );

        final SchedulingStrategy nextStrategy = invocationContext.isSuccessfulInvocation() ? schedulingStrategy : ScheduleNever.INSTANCE;

        return new InvocationResult( nextStrategy, output );
    }

}
