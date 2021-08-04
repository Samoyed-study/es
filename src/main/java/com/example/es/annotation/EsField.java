package com.example.es.annotation;

import java.lang.annotation.*;

/**
 * <p>
 *
 * </p>
 *
 * @author heshuyao
 * @since 2021/8/3 - 17:26
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface EsField {

    String name() default "";

    boolean isExist() default true;
}
