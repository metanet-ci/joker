package cs.bilkent.zanza.operator.impl.invocationreason;

import cs.bilkent.zanza.operator.InvocationReason;

public class PortsClosed implements InvocationReason
{

	private final int[] ports;

	public PortsClosed(int[] ports)
	{
		this.ports = ports;
	}

	@Override
	public boolean isSuccessful()
	{
		return false;
	}

	public int[] getPorts()
	{
		return ports;
	}

}
