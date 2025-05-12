package com.db.kurs.orm.annotation.link;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OneToMany {
    String mappedBy();
    FetchType fetch() default FetchType.LAZY;
}
