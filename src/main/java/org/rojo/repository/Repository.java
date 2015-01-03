package org.rojo.repository;

import java.lang.ref.SoftReference;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.rojo.exceptions.InvalidTypeException;
import org.rojo.exceptions.RepositoryError;
import redis.clients.jedis.Jedis;

public class Repository {

	private final RedisFacade store;
	private static final Map<Class, Map<Long, SoftReference<Object>>> cache = new HashMap<Class, Map<Long, SoftReference<Object>>>();
	private static final ReadWriteLock lock = new ReentrantReadWriteLock();
	private static final Lock readLock = lock.readLock();
	private static final Lock writeLock = lock.writeLock();

	public Repository(Jedis je) {
		store = new RedisFacade(je);
	}

	public void flush() {
		store.flush();
	}

	public long writeAndFlush(Object entity) {
		long id;
		if ((id = write(entity)) > 0) {
			store.flush();
		}
		return id;
	}

	public long write(Object entity) {
		writeLock.lock();
		try {
			boolean exist = false;
			boolean noExist = false;
			EntityRepresentation representation = EntityRepresentation.forClass(entity.getClass());
			long id = representation.getId(entity);
			if (representation.getIdGenerator().isEmpty()) {
				if (id <= 0) {
					return 0;
				}
			} else {
				if (id <= 0) {
					id = store.incr(representation.getIdGenerator());
					noExist = true;
				}
			}
			Field unique = representation.getUnique();
			if (unique != null) {
				if (!noExist) {
					exist = store.exists(entity.getClass(), id);
				}
				if (!exist) {
					boolean result = store.writeUnique(entity, representation.getUnique(), id);
					if (!result) {
						return 0;
					}
				}
			}
			store.writeId(entity.getClass(), id);
			for (Field field : representation.getFields()) {
				try {
					if (field.get(entity) != null) {
						if (Collection.class.isAssignableFrom(field.getType())) {
							@SuppressWarnings("unchecked")
							Collection<? extends Object> collection = (Collection<? extends Object>) field.get(entity);
							store.writeCollection(entity.getClass(), collection, id, field);
						} else if (Map.class.isAssignableFrom(field.getType())) {
							Map map = (Map) field.get(entity);
							store.writeMap(entity.getClass(), map, id, field);
						} else {
							store.write(entity, id, field);
						}
					}
				} catch (Exception e) {
					throw new RepositoryError("error writing " + entity.getClass() + " - " + id + " - " + field.getName(), e);
				}
			}
			representation.setId(entity, id);
			if (representation.isCache()) {
				cache(entity, id);
			}
			return id;
		} finally {
			writeLock.unlock();
		}
	}

	public boolean write(Class claz, long id, String p, Object v) {
		if (store.exists(claz, id)) {
			EntityRepresentation representation = EntityRepresentation.forClass(claz);
			Field f = representation.getField(p);
			if (Collection.class.isAssignableFrom(f.getType()) && Collection.class.isAssignableFrom(v.getClass())) {
				store.writeCollection(claz, (Collection) v, id, f);
			} else if (Map.class.isAssignableFrom(f.getType()) && Map.class.isAssignableFrom(v.getClass())) {
				store.writeMap(claz, (Map) v, id, f);
			} else {
				store.write(claz, id, f, v);
			}
			if (representation.isCache()) {
				readLock.lock();
				try {
					Object entity = getFromCache(claz, id);
					if (entity != null) {
						try {
							claz.getField(p).set(entity, v);
						} catch (Exception e) {
						}
					}
				} finally {
					readLock.unlock();
				}
			}
			return true;
		}
		return false;
	}

	public boolean writeAndFlush(Class claz, long id, final String p, Object v) {
		if (this.write(claz, id, p, v)) {
			this.flush();
			return true;
		}
		return false;
	}

	public <T> T get(Class<T> claz, long id) {
		readLock.lock();
		try {
			T entity = (T) getFromCache(claz, id);
			if (entity != null) {
				return entity;
			}
			EntityRepresentation representation = EntityRepresentation.forClass(claz);
			if (!store.exists(claz, id)) {
				return null;
			}
			readLock.unlock();
			writeLock.lock();
			try {
				entity = claz.newInstance();
				representation.setId(entity, id);
				for (Field field : representation.getFields()) {
					processField(entity, id, field);
				}
				if (representation.isCache()) {
					cache(entity, id);
				}
				return entity;
			} catch (Exception e) {
				throw new RepositoryError(e);
			} finally {
				readLock.lock();
				writeLock.unlock();
			}
		} finally {
			readLock.unlock();
		}
	}

	public <T> T get(Class<T> claz, long id, String p) {
		readLock.lock();
		try {
			EntityRepresentation representation = EntityRepresentation.forClass(claz);
			Field f = representation.getField(p);
			if (representation.isCache()) {
				Object entity = getFromCache(claz, id);
				if (entity != null) {
					return (T) f.get(entity);
				}
			}
			if (Collection.class.isAssignableFrom(f.getType())) {
				Collection holder = initCollectionHolder(f);
				store.readValues(claz, id, f, holder);
				return (T) holder;
			} else if (Map.class.isAssignableFrom(f.getType())) {
				Map map = initMapHolder(f);
				store.readValues(claz, id, f, map);
			} else {
				return store.readValue(claz, id, f);
			}
		} catch (Exception e) {
		} finally {
			readLock.unlock();
		}
		return null;
	}

	public void delete(Class claz, long id) {
		if (id <= 0) {
			return;
		}
		writeLock.lock();
		try {
			EntityRepresentation representation = EntityRepresentation.forClass(claz);
			store.removeId(claz, id);
			for (Field field : representation.getFields()) {
				store.delete(claz, id, field);
			}
			flush();
			if (representation.isCache()) {
				deleteFromCache(claz, id);
			}
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * the rank
	 *
	 * @param claz
	 * @param p
	 * @param start
	 * @param end
	 * @return
	 */
	public List<Long> rank(Class claz, String p, long start, long end) {
		EntityRepresentation representation = EntityRepresentation.forClass(claz);
		Field f = representation.getField(p);
		if (f != null) {
			Set<String> r = store.rank(claz, f, start, end);
			List<Long> rank = new ArrayList<Long>(r.size());
			for (String item : r) {
				rank.add(Long.parseLong(item));
			}
			return rank;
		} else {
			throw new InvalidTypeException("miss field:" + p + " of " + claz);
		}
	}

	private <T> void processField(T entity, long id, Field field) {
		try {
			if (Collection.class.isAssignableFrom(field.getType())) {
				Collection holder = initCollectionHolder(field);
				store.readValues(entity.getClass(), id, field, holder);
				field.set(entity, holder);
			} else if (Map.class.isAssignableFrom(field.getType())) {
				Map map = initMapHolder(field);
				store.readValues(entity.getClass(), id, field, map);
				field.set(entity, map);
			} else {
				field.set(entity, store.readValue(entity.getClass(), id, field));
			}
		} catch (Exception e) {
			throw new InvalidTypeException(e);
		}
	}

	private static Collection initCollectionHolder(Field field) {
		if (field.getType() == List.class || field.getType() == Collection.class) {
			return new ArrayList();
		} else if (field.getType() == Set.class) {
			return new HashSet();
		} else {
			throw new InvalidTypeException("unsupported Collection subtype");
		}
	}

	private static Map initMapHolder(Field f) {
		return new HashMap();
	}

	/**
	 * cache
	 *
	 * @param entity
	 */
	private void cache(Object entity, long id) {
		Map<Long, SoftReference<Object>> c = cache.get(entity.getClass());
		if (c == null) {
			c = new HashMap<Long, SoftReference<Object>>();
			cache.put(entity.getClass(), c);
		}
		c.put(id, new SoftReference<Object>(entity));
	}

	/**
	 *
	 * @param <T>
	 * @param claz
	 * @param id
	 * @return
	 */
	private Object getFromCache(Class claz, long id) {
		Map<Long, SoftReference<Object>> c = cache.get(claz);
		if (c != null) {
			SoftReference<Object> sr = c.get(id);
			if (sr != null) {
				return sr.get();
			}
		}
		return null;
	}

	private void deleteFromCache(Class claz, long id) {
		Map<Long, SoftReference<Object>> c = cache.get(claz);
		if (c != null) {
			c.remove(id);
		}
	}

	/**
	 * the cache
	 *
	 * @return
	 */
	public static Map<Class, Map<Long, SoftReference<Object>>> getCache() {
		return cache;
	}

	/**
	 * clear cache
	 */
	public static void clearCache() {
		writeLock.lock();
		try {
			cache.clear();
		} finally {
			writeLock.unlock();
		}
	}
}
