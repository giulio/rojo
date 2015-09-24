/**
 * 基础entity（用于被继承）
 */
package org.rojo.test;

import org.rojo.annotations.Entity;
import org.rojo.annotations.Id;
import org.rojo.annotations.Value;

/**
 *
 * @author beykery
 */
@Entity(table = "be")
public class BaseEntity
{

  @Id(auto = true, generator = "common:id")
  private String id;
  @Value(column = "n", unique = true)
  private String name;
  @Value
  private String tests;

  public String getId()
  {
    return id;
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public String getTests()
  {
    return tests;
  }

  public void setTests(String tests)
  {
    this.tests = tests;
  }

}
