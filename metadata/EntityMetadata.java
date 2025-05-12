package com.db.kurs.orm.metadata;

import com.db.kurs.orm.annotation.Column;
import com.db.kurs.orm.annotation.Id;
import com.db.kurs.orm.annotation.Table;
import com.db.kurs.orm.annotation.link.JoinColumn;
import com.db.kurs.orm.annotation.link.ManyToMany;
import com.db.kurs.orm.annotation.link.ManyToOne;
import com.db.kurs.orm.annotation.link.OneToMany;
import com.db.kurs.orm.annotation.link.OneToOne;

import java.lang.reflect.Field;
import java.util.*;

/**
 * Метаданные сущности: имя таблицы, составной PK, обычные колонки и связи.
 */
public class EntityMetadata {

    public final String tableName;
    /** Список всех полей, помеченных @Id (для составного PK) */
    public final List<Field> idFields = new ArrayList<>();
    /** Имена столбцов в БД для каждого поля из idFields */
    public final List<String> idColumns = new ArrayList<>();
    /** Все остальные простые колонки (name→Field) */
    public final Map<String, Field> columns = new LinkedHashMap<>();
    /** Метаданные всех связей (@OneToMany, @ManyToOne и т.д.) */
    public final List<RelationshipMetadata> relations = new ArrayList<>();

    public EntityMetadata(Class<?> type) {
        // 1) Определяем имя таблицы
        Table tbl = type.getAnnotation(Table.class);
        this.tableName = (tbl != null && !tbl.name().isEmpty())
                ? tbl.name()
                : type.getSimpleName();

        // 2) Собираем все @Id поля (с учётом @JoinColumn для ManyToOne)
        for (Field f : type.getDeclaredFields()) {
            if (f.isAnnotationPresent(Id.class)) {
                f.setAccessible(true);

                // если это поле связано через @ManyToOne
                JoinColumn jc = f.getAnnotation(JoinColumn.class);
                if (jc != null && !jc.name().isEmpty()) {
                    // берем имя FK‑столбца
                    idColumns.add(jc.name());
                } else {
                    // иначе — либо @Column, либо имя поля
                    Column c = f.getAnnotation(Column.class);
                    String col = (c != null && !c.name().isEmpty()) ? c.name() : f.getName();
                    idColumns.add(col);
                }

                idFields.add(f);
            }
        }


        // 3) Простые колонки (не @Id и не связи)
        for (Field f : type.getDeclaredFields()) {
            if (f.isAnnotationPresent(Column.class) && !f.isAnnotationPresent(Id.class)) {
                Column c = f.getAnnotation(Column.class);
                String name = c.name().isEmpty() ? f.getName() : c.name();
                f.setAccessible(true);
                columns.put(name, f);
            }
        }

        // 4) Все связи
        for (Field f : type.getDeclaredFields()) {
            if (f.isAnnotationPresent(OneToMany.class)
                    || f.isAnnotationPresent(ManyToMany.class)
                    || f.isAnnotationPresent(OneToOne.class)
                    || f.isAnnotationPresent(ManyToOne.class)) {
                f.setAccessible(true);
                relations.add(new RelationshipMetadata(f));
            }
        }
    }
}
