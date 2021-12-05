package com.atguigu.gmall.realtime.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD) //注解的注解：当前注解作用域 未来在字段上用
@Retention(RetentionPolicy.RUNTIME) //注解的注解：当前注解生效时间
public @interface TransientSink {
}
