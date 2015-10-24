package cs.bilkent.zanza.operators;

import java.util.function.Function;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.InvocationResult;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.OperatorSpec;
import cs.bilkent.zanza.operator.OperatorType;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.SchedulingStrategy;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.ANY_NUMBER_OF_TUPLES;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;

@OperatorSpec( type = OperatorType.STATELESS, inputPortCount = 1, outputPortCount = 1 )
public class MapperOperator implements Operator
{

    public static final String MAPPER_CONFIG_PARAMETER = "mapper";

    public static final String TUPLE_COUNT_CONFIG_PARAMETER = "tupleCount";

    private Function<Tuple, Tuple> mapper;

    private int tupleCount = ANY_NUMBER_OF_TUPLES;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        final OperatorConfig config = context.getConfig();

        Object mapperObject = config.getObject( MAPPER_CONFIG_PARAMETER );
        if ( mapperObject instanceof Function )
        {
            this.mapper = (Function<Tuple, Tuple>) mapperObject;
        }
        else
        {
            throw new IllegalArgumentException( "mapper function is not provided" );
        }

        if ( config.contains( TUPLE_COUNT_CONFIG_PARAMETER ) )
        {
            this.tupleCount = config.getInteger( TUPLE_COUNT_CONFIG_PARAMETER );
        }

        return scheduleWhenTuplesAvailableOnDefaultPort( this.tupleCount );
    }

    @Override
    public InvocationResult process ( final InvocationContext invocationContext )
    {
        final PortsToTuples output = invocationContext.getTuples().getTuplesByDefaultPort()
                                                  .stream()
                                                  .map( mapper )
                                                  .collect( PortsToTuples.COLLECT_TO_DEFAULT_PORT );

        final SchedulingStrategy nextStrategy = invocationContext.isSuccessfulInvocation()
                                                ? scheduleWhenTuplesAvailableOnDefaultPort( tupleCount )
                                                : ScheduleNever.INSTANCE;

        return new InvocationResult( nextStrategy, output );
    }

}
