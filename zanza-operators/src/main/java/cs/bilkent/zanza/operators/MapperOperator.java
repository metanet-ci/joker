package cs.bilkent.zanza.operators;

import java.util.function.Function;

import cs.bilkent.zanza.operator.InvocationReason;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.OperatorContext;
import cs.bilkent.zanza.operator.OperatorSpec;
import cs.bilkent.zanza.operator.OperatorType;
import cs.bilkent.zanza.operator.PortsToTuples;
import cs.bilkent.zanza.operator.ProcessingResult;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.ANY_NUMBER_OF_TUPLES;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

@OperatorSpec( type = OperatorType.STATELESS, inputPortCount = 1, outputPortCount = 1 )
public class MapperOperator implements Operator
{

    public static final String MAPPER_FUNCTION_CONFIG_PARAMETER = "mapper";

    private Function<Tuple, Tuple> mapper;

    @Override
    public SchedulingStrategy init ( final OperatorContext context )
    {
        final OperatorConfig config = context.getConfig();

        Object mapperObject = config.getObject( MAPPER_FUNCTION_CONFIG_PARAMETER );

        if ( mapperObject instanceof Function )
        {
            this.mapper = (Function<Tuple, Tuple>) mapperObject;
        }
        else
        {
            throw new IllegalArgumentException( "mapper function is not provided" );
        }

        return scheduleWhenTuplesAvailableOnDefaultPort( ANY_NUMBER_OF_TUPLES );
    }

    @Override
    public ProcessingResult process ( final PortsToTuples portsToTuples, final InvocationReason reason )
    {
        final ProcessingResult output = new ProcessingResult();
        portsToTuples.getTuplesByDefaultPort().stream().map( mapper ).forEach( output::add );

        final SchedulingStrategy nextStrategy = reason.isSuccessful()
                                                ? scheduleWhenTuplesAvailableOnDefaultPort( ANY_NUMBER_OF_TUPLES )
                                                : ScheduleNever.INSTANCE;
        output.setSchedulingStrategy( nextStrategy );

        return output;
    }

}
