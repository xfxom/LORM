package com.db.kurs.orm.annotation;

import org.springframework.stereotype.Component;
import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Component
public @interface Repository {
    /**
     * Can set bean name
     */
    String value() default "";
}