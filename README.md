# rojo  


rojo是github上的一个开源项目（https://github.com/giulio/rojo），本作进行了重构和修剪，
使得它更符合beykery使用习惯，另外性能略有提高-`-

## 问题

使用redis的java客户端jedis的时候需要操作一系列key，保存某个对象的时候，需要把一个
个属性组合为key，然后set到redis，较繁琐。

## 解决方案

rojo是为了简化对象持久化到redis时的操作的，它提供了几个运行时解析的annotation来做
这件事情，把用户从设计对象属性的各种key的繁重劳作中解脱出来，而把精力主要放在对象
模型的设计上。

## 使用

请参看代码中的test


## 结语

enjoy it.

	
	
