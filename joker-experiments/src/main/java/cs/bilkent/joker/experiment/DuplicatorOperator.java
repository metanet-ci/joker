package cs.bilkent.joker.experiment;

import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.Tuple;
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
    public SchedulingStrategy init ( final InitCtx ctx )
    {
        this.duplicateCount = ctx.getConfig().getInteger( DUPLICATE_COUNT_PARAMETER );
        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public void invoke ( final InvocationCtx ctx )
    {
        for ( Tuple tuple : ctx.getInputTuplesByDefaultPort() )
        {
            for ( int i = 0; i < duplicateCount; i++ )
            {
                ctx.output( tuple );
            }
        }
    }

}
