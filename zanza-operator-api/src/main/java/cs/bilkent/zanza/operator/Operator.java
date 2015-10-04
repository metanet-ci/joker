package cs.bilkent.zanza.operator;

public interface Operator
{
	ProcessingResult process(PortsToTuples portsToTuples, InvocationReason reason);
}
