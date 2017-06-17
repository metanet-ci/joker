package cs.bilkent.joker.experiment;

import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;

@OperatorSpec( inputPortCount = 1, outputPortCount = 1, type = STATEFUL )
public class StatefulMultiplierOperator extends BaseMultiplierOperator
{

}
