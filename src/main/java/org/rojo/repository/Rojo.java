package org.rojo.repository;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.rojo.annotations.Index;
import org.rojo.exceptions.InvalidTypeException;
import org.rojo.exceptions.RepositoryError;
import redis.clients.jedis.Jedis;

public class Rojo
{

	private final RedisFacade store;
	private static final Map<Class, Map<Long, SoftObjectReference<Object>>> cache = new HashMap<Class, Map<Long, SoftObjectReference<Object>>>();
	private static final ReferenceQueue<SoftObjectReference<Object>> rq = new ReferenceQueue<SoftObjectReference<Object>>();
	private static int TIMES_CACHE_CLEAR;
	private static int timeCache;
	private static final ReadWriteLock lock = new ReentrantReadWriteLock();
	private static final Lock readLock = lock.readLock();
	private static final Lock writeLock = lock.writeLock();
	private static int cachedObjectCounter;

	static
	{
		try
		{
			TIMES_CACHE_CLEAR = Integer.parseInt(System.getProperty("rojo.times.cache.clear", "15000"));
		} catch (Exception e)
		{
		}
	}

	public Rojo(Jedis je)
	{
		store = new RedisFacade(je);
	}

	public void flush()
	{
		store.flush();
	}

	public long writeAndFlush(Object entity)
	{
		long id;
		if ((id = write(entity)) > 0)
		{
			store.flush();
		}
		return id;
	}

	public long write(Object entity)
	{
		writeLock.lock();
		try
		{
			boolean exist = false;
			boolean noExist = false;
			EntityRepresentation representation = EntityRepresentation.forClass(entity.getClass());
			long id = representation.getId(entity);
			if (representation.getIdGenerator().isEmpty())
			{
				if (id <= 0)
				{
					return 0;
				}
			} else
			{
				if (id <= 0)
				{
					id = store.incr(representation.getIdGenerator());
					noExist = true;
				}
			}
			Field unique = representation.getUnique();
			if (unique != null)
			{
				if (!noExist)
				{
					exist = store.exists(entity.getClass(), id);
				}
				if (!exist)
				{
					boolean result = store.writeUnique(entity, unique, id);
					if (!result)
					{
						return 0;
					}
				}
			}
			store.writeId(entity.getClass(), id);
			for (Field field : representation.getFields())
			{
				try
				{
					if (field.get(entity) != null)
					{
						if (Collection.class.isAssignableFrom(field.getType()))
						{
							@SuppressWarnings("unchecked")
							Collection<? extends Object> collection = (Collection<? extends Object>) field.get(entity);
							store.writeCollection(entity.getClass(), collection, id, field);
						} else if (Map.class.isAssignableFrom(field.getType()))
						{
							Map map = (Map) field.get(entity);
							store.writeMap(entity.getClass(), map, id, field);
						} else
						{
							store.write(entity, id, field);
						}
					}
				} catch (Exception e)
				{
					throw new RepositoryError("error writing " + entity.getClass() + " - " + id + " - " + field.getName(), e);
				}
			}
			representation.setId(entity, id);
			if (representation.isCache())
			{
				cache(entity, id);
			}
			return id;
		} finally
		{
			writeLock.unlock();
		}
	}

	public boolean write(Class claz, long id, String p, Object v)
	{
		EntityRepresentation representation = EntityRepresentation.forClass(claz);
		Field f = representation.getField(p);
		if (store.exists(claz, id))
		{
			if (f != null)
			{
				if (Collection.class.isAssignableFrom(f.getType()) && Collection.class.isAssignableFrom(v.getClass()))
				{
					store.writeCollection(claz, (Collection) v, id, f);
				} else if (Map.class.isAssignableFrom(f.getType()) && Map.class.isAssignableFrom(v.getClass()))
				{
					store.writeMap(claz, (Map) v, id, f);
				} else
				{
					store.write(claz, id, f, v);
				}
				if (representation.isCache())
				{
					readLock.lock();
					try
					{
						Object entity = getFromCache(claz, id);
						if (entity != null)
						{
							try
							{
								claz.getField(p).set(entity, v);
							} catch (Exception e)
							{
							}
						}
					} finally
					{
						readLock.unlock();
					}
				}
				return true;
			}
		}
		return false;
	}

	public boolean writeAndFlush(Class claz, long id, final String p, Object v)
	{
		if (this.write(claz, id, p, v))
		{
			this.flush();
			return true;
		}
		return false;
	}

	public <T> T get(Class<T> claz, long id)
	{
		readLock.lock();
		try
		{
			T entity = (T) getFromCache(claz, id);
			if (entity != null)
			{
				return entity;
			}
			EntityRepresentation representation = EntityRepresentation.forClass(claz);
			if (!store.exists(claz, id))
			{
				return null;
			}
			readLock.unlock();
			writeLock.lock();
			try
			{
				entity = claz.newInstance();
				representation.setId(entity, id);
				store.processFields(entity, id, representation.getFields());
				if (representation.isCache())
				{
					cache(entity, id);
				}
				return entity;
			} catch (Exception e)
			{
				throw new RepositoryError(e);
			} finally
			{
				readLock.lock();
				writeLock.unlock();
			}
		} finally
		{
			readLock.unlock();
		}
	}

	public <T> T get(Class<T> claz, long id, String p)
	{
		readLock.lock();
		try
		{
			EntityRepresentation representation = EntityRepresentation.forClass(claz);
			Field f = representation.getField(p);
			if (representation.isCache())
			{
				Object entity = getFromCache(claz, id);
				if (entity != null)
				{
					return (T) f.get(entity);
				}
			}
			if (Collection.class.isAssignableFrom(f.getType()))
			{
				Collection holder = RedisFacade.initCollectionHolder(f);
				store.readValues(claz, id, f, holder);
				return (T) holder;
			} else if (Map.class.isAssignableFrom(f.getType()))
			{
				Map map = RedisFacade.initMapHolder(f);
				store.readValues(claz, id, f, map);
			} else
			{
				return store.readValue(claz, id, f);
			}
		} catch (Exception e)
		{
		} finally
		{
			readLock.unlock();
		}
		return null;
	}

	/**
	 * delete entity
	 *
	 * @param entity
	 */
	public void delete(Object entity)
	{
		Class claz = entity.getClass();
		EntityRepresentation representation = EntityRepresentation.forClass(claz);
		long id = representation.getId(entity);
		if (id <= 0)
		{
			return;
		}
		writeLock.lock();
		try
		{
			store.removeId(claz, id);
			Field unique = representation.getUnique();
			if (unique != null)
			{
				store.removeUnique(entity, unique);
			}
			for (Field field : representation.getFields())
			{
				store.delete(claz, id, field);
				Index index = field.getAnnotation(Index.class);
				if (index != null)
				{
					try
					{
						Object v = field.get(entity);
						if (v != null)
						{
							store.deleteIndex(claz, field, v.toString(), id);
						}
					} catch (Exception e)
					{
					}
				}
			}
			flush();
			if (representation.isCache())
			{
				deleteFromCache(claz, id);
			}
		} finally
		{
			writeLock.unlock();
		}
	}

	/**
	 * the range
	 *
	 * @param claz
	 * @param p
	 * @param start
	 * @param end
	 * @return
	 */
	public List<Long> range(Class claz, String p, long start, long end)
	{
		EntityRepresentation representation = EntityRepresentation.forClass(claz);
		Field f = representation.getField(p);
		if (f != null)
		{
			Set<String> r = store.range(claz, f, start, end);
			if (r != null)
			{
				List<Long> rank = new ArrayList<Long>(r.size());
				for (String item : r)
				{
					rank.add(Long.parseLong(item));
				}
				return rank;
			}
			return null;
		} else
		{
			throw new InvalidTypeException("miss field:" + p + " of " + claz);
		}
	}

	/**
	 * index of rank
	 *
	 * @param claz
	 * @param id
	 * @param p
	 * @return
	 */
	public long rank(Class claz, long id, String p)
	{
		EntityRepresentation representation = EntityRepresentation.forClass(claz);
		Field f = representation.getField(p);
		if (f != null)
		{
			return store.rank(claz, f, id);
		} else
		{
			return -1;
		}
	}

	/**
	 * the indexing ids
	 *
	 * @param claz
	 * @param p
	 * @param v
	 * @return
	 */
	public Set<Long> index(Class claz, String p, Object v)
	{
		return store.index(claz, p, v);
	}

	/**
	 * cache
	 *
	 * @param entity
	 */
	private void cache(Object entity, long id)
	{
		Map<Long, SoftObjectReference<Object>> c = cache.get(entity.getClass());
		if (c == null)
		{
			c = new HashMap<Long, SoftObjectReference<Object>>();
			cache.put(entity.getClass(), c);
		}
		c.put(id, new SoftObjectReference<Object>(entity, rq, id));
		cachedObjectCounter++;
		timeCache++;
		if (timeCache >= TIMES_CACHE_CLEAR)
		{
			SoftObjectReference sr;
			while ((sr = (SoftObjectReference) rq.poll()) != null)
			{
				c = cache.get(sr.claz);
				c.remove(sr.id);
				cachedObjectCounter--;
			}
			timeCache = 0;
		}
	}

	/**
	 *
	 * @param <T>
	 * @param claz
	 * @param id
	 * @return
	 */
	private Object getFromCache(Class claz, long id)
	{
		Map<Long, SoftObjectReference<Object>> c = cache.get(claz);
		if (c != null)
		{
			SoftReference<Object> sr = c.get(id);
			if (sr != null)
			{
				return sr.get();
			}
		}
		return null;
	}

	private void deleteFromCache(Class claz, long id)
	{
		Map<Long, SoftObjectReference<Object>> c = cache.get(claz);
		if (c != null)
		{
			c.remove(id);
		}
	}

	/**
	 * clear cache
	 */
	@SuppressWarnings("empty-statement")
	public static void clearCache()
	{
		writeLock.lock();
		try
		{
			cache.clear();
			while (rq.poll() != null) ;
			cachedObjectCounter = 0;
		} finally
		{
			writeLock.unlock();
		}
	}

	/**
	 * cache size
	 *
	 * @return
	 */
	public static int getCachedObjectCounter()
	{
		return cachedObjectCounter;
	}

	private class SoftObjectReference<Object> extends SoftReference<Object>
	{

		public final long id;
		public final Class claz;

		public SoftObjectReference(Object r, ReferenceQueue q, long id)
		{
			super(r, q);
			this.id = id;
			this.claz = r.getClass();
		}
	}
}
