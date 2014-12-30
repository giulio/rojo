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
@Entity
public class BaseEntity {

    @Id(Generator = "common::id")
    private long id;
    @Value(unique = true)
    private String name;

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
