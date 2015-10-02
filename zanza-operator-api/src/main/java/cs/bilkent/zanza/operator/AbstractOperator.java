package cs.bilkent.zanza.operator;

import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public abstract class AbstractOperator implements Operator
{

	public SchedulingStrategy onInit(OperatorContext context)
	{
		return null;
	}

	public void onDestroy() // TODO reason object?
	{

	}

}
