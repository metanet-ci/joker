package cs.bilkent.joker.experiment;

import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;

@OperatorSpec( inputPortCount = 1, outputPortCount = 1, type = STATELESS )
public class StatelessMultiplierOperator extends BaseMultiplierOperator
{

}
