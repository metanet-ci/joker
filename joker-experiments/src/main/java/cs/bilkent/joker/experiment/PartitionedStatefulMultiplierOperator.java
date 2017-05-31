package cs.bilkent.joker.experiment;

import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;

@OperatorSpec( inputPortCount = 1, outputPortCount = 1, type = PARTITIONED_STATEFUL )
public class PartitionedStatefulMultiplierOperator extends BaseMultiplierOperator
{

}
