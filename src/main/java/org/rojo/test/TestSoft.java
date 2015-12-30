/**
 * test
 */
package org.rojo.test;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author beykery
 */
public class TestSoft
{

  public static void main(String[] args)
  {
    Map<String, SoftObjectReference<Object>> c = new HashMap<String, SoftObjectReference<Object>>();
    ReferenceQueue<SoftObjectReference<Object>> rq = new ReferenceQueue<SoftObjectReference<Object>>();
    for (int i = 0;; i++)
    {
      Enty e = new Enty();
      String id = String.valueOf(i);
      c.put(id, new SoftObjectReference<Object>(e, rq, id));
      SoftObjectReference sr;
      int sum = 0;
      while ((sr = (SoftObjectReference) rq.poll()) != null)
      {
        sum++;
        c.remove(sr.id);
      }
      if (sum > 0)
      {
        System.out.println("out!" + i + ":" + sum + ":" + c.size());
      }
    }
  }

  private static class Enty
  {

    public Enty()
    {
      content = new byte[1024 * 1024];
    }

    private byte[] content;
  }

  private static class SoftObjectReference<Object> extends SoftReference<Object>
  {

    public final String id;
    public final Class claz;

    public SoftObjectReference(Object r, ReferenceQueue q, String id)
    {
      super(r, q);
      this.id = id;
      this.claz = r.getClass();
    }
  }
}
