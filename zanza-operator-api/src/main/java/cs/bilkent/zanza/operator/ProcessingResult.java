package cs.bilkent.zanza.operator;

import java.util.List;

import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public class ProcessingResult extends PortsToTuples
{
	private SchedulingStrategy schedulingStrategy;

	public ProcessingResult()
	{
	}

	public ProcessingResult(final Tuple tuple)
	{
		super(tuple);
	}

	public ProcessingResult(final Tuple tuple, final SchedulingStrategy schedulingStrategy)
	{
		super(tuple);
		this.schedulingStrategy = schedulingStrategy;
	}

	public ProcessingResult(final List<Tuple> tuples, final SchedulingStrategy schedulingStrategy)
	{
		super(tuples);
		this.schedulingStrategy = schedulingStrategy;
	}

	public SchedulingStrategy getSchedulingStrategy()
	{
		return schedulingStrategy;
	}

	public void setSchedulingStrategy(final SchedulingStrategy schedulingStrategy)
	{
		this.schedulingStrategy = schedulingStrategy;
	}
}
