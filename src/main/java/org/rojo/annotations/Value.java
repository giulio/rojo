package org.rojo.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Value
{

	String column() default "";

	boolean unique() default false;

	boolean sort() default false;

	boolean bigFirst() default false;

	long size() default 0;
}
