package cs.bilkent.zanza.operator;

public class Port
{
	public static final int DEFAULT_PORT_INDEX = 0;

	public final String operatorName;

	public final int portIndex;

	public Port(String operatorName, int portIndex)
	{
		this.operatorName = operatorName;
		this.portIndex = portIndex;
	}
}
