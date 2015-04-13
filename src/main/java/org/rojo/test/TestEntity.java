/**
 * 用来测试
 */
package org.rojo.test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.rojo.annotations.Entity;
import org.rojo.annotations.Index;
import org.rojo.annotations.Value;

@Entity(table = "te", cache = true)
public class TestEntity extends BaseEntity
{

  @Value(column = "l")
  private List<String> list;
  @Value(column = "s")
  private Set<Float> set;
  @Value(column = "m")
  private Map<String, Integer> map;
  @Value(column = "a", sort = true, bigFirst = false, size = 5)
  private int age;
  @Value(column = "sex")
  @Index
  private int sex;
  @Value(column = "t1")
  private int t1;
  @Value(column = "t2")
  private long t2;
  @Value
  private byte[] content;

  public void setT1(int t1)
  {
    this.t1 = t1;
  }

  public int getT1()
  {
    return t1;
  }

  public void setT2(long t2)
  {
    this.t2 = t2;
  }

  public long getT2()
  {
    return t2;
  }

  public int getSex()
  {
    return sex;
  }

  public void setSex(int sex)
  {
    this.sex = sex;
  }

  public void setAge(int age)
  {
    this.age = age;
  }

  public int getAge()
  {
    return age;
  }

  public void setMap(Map<String, Integer> map)
  {
    this.map = map;
  }

  public Map<String, Integer> getMap()
  {
    return map;
  }

  public void setSet(Set<Float> set)
  {
    this.set = set;
  }

  public Set<Float> getSet()
  {
    return set;
  }

  public List<String> getList()
  {
    return list;
  }

  public void setList(List<String> strings)
  {
    this.list = strings;
  }

  public byte[] getContent()
  {
    return content;
  }

  public void setContent(byte[] content)
  {
    this.content = content;
  }

  @Override
  public String toString()
  {
    return this.getId();
  }

}
