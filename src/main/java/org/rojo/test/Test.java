/**
 * 测试
 */
package org.rojo.test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.rojo.repository.Rojo;
import redis.clients.jedis.Jedis;

/**
 *
 * @author beykery
 */
public class Test
{

  public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException
  {
    long os = System.currentTimeMillis();
    int times = 100;
    Jedis je = new Jedis("localhost", 6379);
    je.auth("ssm123");
    Rojo re = new Rojo(je);
    new TestGenerator(je, "common:id").configue("common:id");//configue id generator
    for (int i = 1; i <= times; i++)
    {
      TestEntity ss = new TestEntity();
      ss.setName("test名字" + i);
      ss.setAge(i);
      ss.setTests(null);
      List<String> l = new ArrayList<String>();
      l.add("fd");
      l.add("akjl;sfd");
      l.add(null);
      ss.setList(l);
      Map<String, Integer> map = new HashMap<String, Integer>();
      ss.setMap(map);
      map.put("a", 1);
      map.put("b", 2);
      map.put("c", null);
      Set<Float> set = new HashSet<Float>();
      ss.setSet(set);
      set.add(1.2f);
      set.add(3.14159f);
      ss.setSex(i % 2 == 1 ? 1 : 0);
      ss.setContent("哈哈".getBytes("UTF-8"));
      re.saveAndFlush(ss);
      ss.setT1(41);
      ss.setT2(20);
      ss.setContent("哈哈111".getBytes("UTF-8"));
      re.updateAndFlush(ss, "t1", "t2", "content");
      ss = re.get(TestEntity.class, ss.getId());
      System.out.println(ss);
      Thread.sleep(10);
    }

    Rojo.clearCache();
    for (int i = 1; i <= times; i++)
    {
      TestEntity te = re.get(TestEntity.class, i + "fkey");
      System.out.println(te);
    }

    for (int i = 1; i <= times; i++)
    {
      TestEntity te = re.get(TestEntity.class, (i) + "fkey");
      System.out.println(te);
    }

    Rojo.clearCache();
    Set<TestEntity> l = re.index(TestEntity.class, "sex", 0);
    System.out.println(l);

    TestEntity te = re.unique(TestEntity.class, "test名字6");
    System.out.println("unique:" + te);

    Set<TestEntity> r = re.range(TestEntity.class, "age", -5, -1);
    System.out.println(r);

    long rank = re.rank(TestEntity.class, "10fkey", "age");
    System.out.println(rank);
    Set<TestEntity> tes = re.all(TestEntity.class, 0, -1);
    System.out.println(tes);
    re.deleteAndFlush(te);
    tes = re.all(TestEntity.class, 0, -1);
    System.out.println(tes);
    Date ctime = re.createTime(TestEntity.class, "8fkey");
    System.out.println(ctime);
    tes = re.all(TestEntity.class, new Date(os), ctime);
    System.out.println(tes);
  }

}
