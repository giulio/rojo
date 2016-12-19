package org.rojo.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * for stats
 *
 * @author beykery
 */
public class Stats
{

  final AtomicLong accessCounter = new AtomicLong(0),
          putCounter = new AtomicLong(0),
          missCounter = new AtomicLong();
  final AtomicInteger size = new AtomicInteger();
  AtomicLong evictionCounter = new AtomicLong();

  public long getLookups()
  {
    return (accessCounter.get() - putCounter.get() ) + missCounter.get();
  }

  public long getHits()
  {
    return accessCounter.get() - putCounter.get() ;
  }

  public long getPuts()
  {
    return putCounter.get();
  }

  public long getEvictions()
  {
    return evictionCounter.get();
  }

  public int getSize()
  {
    return size.get();
  }

  public long getMisses()
  {
    return missCounter.get();
  }

  public void add(Stats other)
  {
    accessCounter.addAndGet(other.accessCounter.get());
    putCounter.addAndGet(other.putCounter.get());
    missCounter.addAndGet(other.missCounter.get());
    evictionCounter.addAndGet(other.evictionCounter.get());
    size.set(Math.max(size.get(), other.size.get()));
  }
}
