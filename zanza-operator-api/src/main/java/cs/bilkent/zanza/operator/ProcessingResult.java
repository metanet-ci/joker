package cs.bilkent.zanza.operator;

import java.util.List;

import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public class ProcessingResult extends PortsToTuples
{

	private SchedulingStrategy schedulingStrategy;

	public ProcessingResult()
	{
	}

	public ProcessingResult(Tuple tuple)
	{
		super(tuple);
	}

	public ProcessingResult(Tuple tuple, SchedulingStrategy schedulingStrategy)
	{
		super(tuple);
		this.schedulingStrategy = schedulingStrategy;
	}

	public ProcessingResult(List<Tuple> tuples, SchedulingStrategy schedulingStrategy)
	{
		super(tuples);
		this.schedulingStrategy = schedulingStrategy;
	}

	public SchedulingStrategy getSchedulingStrategy()
	{
		return schedulingStrategy;
	}

	public void setSchedulingStrategy(SchedulingStrategy schedulingStrategy)
	{
		this.schedulingStrategy = schedulingStrategy;
	}

}
