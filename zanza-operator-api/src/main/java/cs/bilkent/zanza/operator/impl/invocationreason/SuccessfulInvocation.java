package cs.bilkent.zanza.operator.impl.invocationreason;

import cs.bilkent.zanza.operator.InvocationReason;

public final class SuccessfulInvocation implements InvocationReason
{
	public static final InvocationReason INSTANCE = new SuccessfulInvocation();

	private SuccessfulInvocation()
	{

	}

	@Override
	public boolean isSuccessful()
	{
		return true;
	}
}
