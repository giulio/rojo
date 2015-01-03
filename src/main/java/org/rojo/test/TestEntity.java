/**
 * 用来测试
 */
package org.rojo.test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.rojo.annotations.Entity;
import org.rojo.annotations.Value;

@Entity(Cache = true)
public class TestEntity extends BaseEntity {

	@Value
	private List<String> list;
	@Value
	private Set<Float> set;
	@Value
	private Map<String, Integer> map;
	@Value(sort = true, bigFirst = true, size = 100)
	private int age;

	public void setAge(int age) {
		this.age = age;
	}

	public int getAge() {
		return age;
	}

	public void setMap(Map<String, Integer> map) {
		this.map = map;
	}

	public Map<String, Integer> getMap() {
		return map;
	}

	public void setSet(Set<Float> set) {
		this.set = set;
	}

	public Set<Float> getSet() {
		return set;
	}

	public List<String> getList() {
		return list;
	}

	public void setList(List<String> strings) {
		this.list = strings;
	}

}
