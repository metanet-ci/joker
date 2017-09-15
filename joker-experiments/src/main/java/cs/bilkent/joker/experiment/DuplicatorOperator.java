package cs.bilkent.joker.experiment;

import java.util.List;

import cs.bilkent.joker.operator.InitializationContext;
import cs.bilkent.joker.operator.InvocationContext;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.Tuples;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;

@OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
public class DuplicatorOperator implements Operator
{

    static final String DUPLICATE_COUNT_PARAMETER = "duplicateCount";

    private int duplicateCount;

    @Override
    public SchedulingStrategy init ( final InitializationContext context )
    {
        this.duplicateCount = context.getConfig().getInteger( DUPLICATE_COUNT_PARAMETER );
        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public void invoke ( final InvocationContext context )
    {
        final List<Tuple> inputTuples = context.getInput().getTuplesByDefaultPort();
        final Tuples output = context.getOutput();
        for ( int i = 0; i < duplicateCount; i++ )
        {
            output.addAll( inputTuples );
        }
    }

}
