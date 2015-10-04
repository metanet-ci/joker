package cs.bilkent.zanza.operator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OperatorConfig implements Fields
{
    private final Map<String, Object> values = new HashMap<>();

    private PartitionKeyExtractor partitionKeyExtractor;

    public OperatorConfig()
    {
    }

    public OperatorConfig(final PartitionKeyExtractor partitionKeyExtractor)
    {
        this.partitionKeyExtractor = partitionKeyExtractor;
    }

    public OperatorConfig(final Map<String, Object> values, final PartitionKeyExtractor partitionKeyExtractor)
    {
        this.values.putAll(values);
        this.partitionKeyExtractor = partitionKeyExtractor;
    }

    public PartitionKeyExtractor getPartitionKeyExtractor()
    {
        return partitionKeyExtractor;
    }

    public void setPartitionKeyExtractor(final PartitionKeyExtractor partitionKeyExtractor)
    {
        this.partitionKeyExtractor = partitionKeyExtractor;
    }

    public Map<String, Object> getValues()
    {
        return values;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(final String field)
    {
        return (T) values.get(field);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getOrDefault(final String field, final T defaultVal)
    {
        return (T) values.getOrDefault(field, defaultVal);
    }

    @Override
    public void set(final String field, final Object value)
    {
        this.values.put(field, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T put(final String field, final T value)
    {
        return (T) this.values.put(field, value);
    }

    @Override
    public Object remove(final String field)
    {
        return this.values.remove(field);
    }

    @Override
    public boolean delete(final String field)
    {
        return this.values.remove(field) != null;
    }

    @Override
    public void clear()
    {
        this.values.clear();
    }

    @Override
    public int size()
    {
        return this.values.size();
    }

    @Override
    public Collection<String> fieldNames()
    {
        return Collections.unmodifiableCollection(this.values.keySet());
    }
}
