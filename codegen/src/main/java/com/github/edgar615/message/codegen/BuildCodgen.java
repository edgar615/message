package com.github.edgar615.message.codegen;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface BuildCodgen {

  /**
   * 信道，消息主题，如果指定了信道，在自动生成Builder的时候会将它作为常量
   * @return
   */
  String to() default "";

  /**
   * 资源，同一个消息的分类，如果指定了信道，在自动生成Builder的时候会将它作为常量
   * @return
   */
  String resource() default "";

}
