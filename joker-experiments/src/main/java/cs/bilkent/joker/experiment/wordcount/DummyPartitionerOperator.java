package cs.bilkent.joker.experiment.wordcount;

import java.util.List;

import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.Tuple;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;

@OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
public class DummyPartitionerOperator implements Operator
{

    @Override
    public SchedulingStrategy init ( final InitCtx ctx )
    {
        return scheduleWhenTuplesAvailableOnDefaultPort( 1 );
    }

    @Override
    public void invoke ( final InvocationCtx ctx )
    {
        final List<Tuple> tuples = ctx.getInputTuplesByDefaultPort();
        for ( int i = 0, j = tuples.size(); i < j; i++ )
        {
            ctx.output( tuples.get( i ) );
        }
    }

}
