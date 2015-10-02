package cs.bilkent.zanza.operator;

import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public class OperatorConfig
{

	private SchedulingStrategy schedulingStrategy;

	private PartitionKeyExtractor partitionKeyExtractor;

	public SchedulingStrategy getSchedulingStrategy()
	{
		return schedulingStrategy;
	}

	public void setSchedulingStrategy(SchedulingStrategy schedulingStrategy)
	{
		this.schedulingStrategy = schedulingStrategy;
	}

	public PartitionKeyExtractor getPartitionKeyExtractor()
	{
		return partitionKeyExtractor;
	}

	public void setPartitionKeyExtractor(PartitionKeyExtractor partitionKeyExtractor)
	{
		this.partitionKeyExtractor = partitionKeyExtractor;
	}

	// setInt getInt setBool getBool ...

}
