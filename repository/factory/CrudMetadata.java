package com.db.kurs.orm.repository.factory;

import com.db.kurs.exception.RepositoryException;
import com.db.kurs.orm.annotation.Column;
import com.db.kurs.orm.annotation.Id;
import com.db.kurs.orm.annotation.Table;
import lombok.Getter;

import java.lang.reflect.Field;
import java.util.Arrays;

@Getter
public class CrudMetadata {
    private final String tableName;
    private final String idColumn;
    private final String idFieldName;
    private final Class<?> entityType;

    public CrudMetadata(String tableName,
                        String idColumn,
                        String idFieldName,
                        Class<?> entityType) {
        this.tableName   = tableName;
        this.idColumn    = idColumn;
        this.idFieldName = idFieldName;
        this.entityType  = entityType;
    }

    public static CrudMetadata from(Class<?> entityType) {
        Table tbl = entityType.getAnnotation(Table.class);
        String tableName = (tbl != null && !tbl.name().isEmpty())
                ? tbl.name()
                : entityType.getSimpleName();

        Field idField = Arrays.stream(entityType.getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .findFirst()
                .orElseThrow(() ->
                        new RepositoryException("No @Id field in " + entityType.getName()));
        Column c = idField.getAnnotation(Column.class);
        String idColumn = (c != null && !c.name().isEmpty())
                ? c.name()
                : idField.getName();

        return new CrudMetadata(tableName, idColumn, idField.getName(), entityType);
    }
}
