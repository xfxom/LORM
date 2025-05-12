package com.db.kurs.orm.annotation.link;

import java.lang.annotation.*;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface JoinColumns {
    JoinColumn[] value();
}
