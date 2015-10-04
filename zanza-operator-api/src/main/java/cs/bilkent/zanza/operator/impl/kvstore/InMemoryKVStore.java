package cs.bilkent.zanza.operator.impl.kvstore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import cs.bilkent.zanza.operator.kvstore.KVStore;

public class InMemoryKVStore implements KVStore
{
    private final Map<String, Object> values = new ConcurrentHashMap<>();

    public InMemoryKVStore()
    {
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
        values.put(field, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T put(final String field, final T value)
    {
        return (T) values.put(field, value);
    }

    @Override
    public Object remove(final String field)
    {
        return values.remove(field);
    }

    @Override
    public boolean delete(final String field)
    {
        return values.remove(field) != null;
    }
}
