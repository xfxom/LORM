package com.db.kurs.orm.annotation.link;

import java.lang.annotation.*;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface JoinColumn {
    /** Колонка в этой таблице */
    String name();
    /** Колонка в целевой таблице */
    String referencedColumnName();
}
