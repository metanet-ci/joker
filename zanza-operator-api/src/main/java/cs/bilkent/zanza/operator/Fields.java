package cs.bilkent.zanza.operator;

import java.util.*;

public interface Fields
{

	<T> T get(String field);

	<T> T getOrDefault(String field, T defaultVal);

	void set(String field, Object value);

	<T> T put(String field, T value);

	Object remove(String field);

	boolean delete(String field);

	void clear();

	int size();

	/**
	 * Returns immutable collection of field names
	 * 
	 * @return immutable collection of field names
	 */
	Collection<String> fieldNames();

	default boolean contains(String field)
	{
		return getObject(field) != null;
	}

	default Object getObject(String field)
	{
		return get(field);
	}

	default Object getObjectOrDefault(String field, Object defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default String getString(String field)
	{
		return get(field);
	}

	default String getStringOrDefault(String field, String defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default Integer getInteger(String field)
	{
		return get(field);
	}

	default Integer getIntegerOrDefault(String field, Integer defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default Long getLong(String field)
	{
		return get(field);
	}

	default Long getLongOrDefault(String field, Long defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default Boolean getBoolean(String field)
	{
		return get(field);
	}

	default Boolean getBooleanOrDefault(String field, Boolean defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default Short getShort(String field)
	{
		return get(field);
	}

	default Short getShortOrDefault(String field, Short defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default Byte getByte(String field)
	{
		return get(field);
	}

	default Byte getByteOrDefault(String field, Byte defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default Double getDouble(String field)
	{
		return get(field);
	}

	default Double getDoubleOrDefault(String field, Double defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default Float getFloat(String field)
	{
		return get(field);
	}

	default Float getFloatOrDefault(String field, Float defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default byte[] getBinary(String field)
	{
		return get(field);
	}

	default byte[] getBinaryOrDefault(String field, byte[] defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default <T> Collection<T> getCollection(String field)
	{
		return get(field);
	}

	default <T> Collection<T> getCollectionOrDefault(String field, Collection<T> defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default <T> Collection<T> getCollectionOrEmpty(String field)
	{
		return getOrDefault(field, Collections.emptyList());
	}

	default <T> List<T> getList(String field)
	{
		return get(field);
	}

	default <T> List<T> getListOrDefault(String field, List<T> defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default <T> List<T> getListOrEmpty(String field)
	{
		return getOrDefault(field, Collections.emptyList());
	}

	default <T> Set<T> getSet(String field)
	{
		return get(field);
	}

	default <T> Set<T> getSetOrDefault(String field, Set<T> defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default <T> Set<T> getSetOrEmpty(String field)
	{
		return getOrDefault(field, Collections.emptySet());
	}

	default <K, V> Map<K, V> getMap(String field)
	{
		return get(field);
	}

	default <K, V> Map<K, V> getMapOrDefault(String field, Map<K, V> defaultVal)
	{
		return getOrDefault(field, defaultVal);
	}

	default <K, V> Map<K, V> getMapOrEmpty(String field)
	{
		return getOrDefault(field, Collections.emptyMap());
	}

}
