package com.db.kurs.orm.mapper;

import com.db.kurs.orm.annotation.Column;
import com.db.kurs.orm.annotation.Id;
import com.db.kurs.orm.annotation.link.ManyToOne;
import com.db.kurs.orm.metadata.EntityMetadata;
import com.db.kurs.orm.metadata.RelationshipMetadata;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class EntityMapper {
    private final Map<Class<?>, Map<String, Field>> cache = new java.util.HashMap<>();

    public <T> T map(ResultSet rs, Class<T> clazz) {
        try {
            T entity = clazz.getDeclaredConstructor().newInstance();
            EntityMetadata meta = new EntityMetadata(clazz);

            // 1) PK‑поля
            for (int i = 0; i < meta.idFields.size(); i++) {
                Field idFld = meta.idFields.get(i);
                String colName = meta.idColumns.get(i);
                if (idFld.isAnnotationPresent(ManyToOne.class)) {
                    // если это stub‑ссылка, пропускаем — загрузится RelationQueryExecutor'ом
                    continue;
                }
                Object raw = rs.getObject(colName);
                Object val = convertValueToFieldType(raw, idFld.getType());
                idFld.set(entity, val);
            }

            // 2) остальные @Column
            for (var e : meta.columns.entrySet()) {
                String col = e.getKey();
                Field fld = e.getValue();
                Object raw = rs.getObject(col);
                Object val = convertValueToFieldType(raw, fld.getType());
                fld.set(entity, val);
            }

            // 3) все @ManyToOne (ставим только stub с ID)
            for (RelationshipMetadata rel : meta.relations) {
                if (rel.type != RelationshipMetadata.RelationType.MANY_TO_ONE) continue;
                String fkCol = rel.joinColumns.get(0).name();
                Object rawFk = rs.getObject(fkCol);
                if (rawFk == null) {
                    rel.field.set(entity, null);
                } else {
                    Class<?> target = rel.field.getType();
                    Object stub = target.getDeclaredConstructor().newInstance();
                    EntityMetadata tm = new EntityMetadata(target);
                    Field tid = tm.idFields.get(0);
                    Object idVal = convertValueToFieldType(rawFk, tid.getType());
                    tid.setAccessible(true);
                    tid.set(stub, idVal);
                    rel.field.set(entity, stub);
                }
            }

            return entity;
        } catch (SQLException ex) {
            throw new RuntimeException("Ошибка чтения из ResultSet", ex);
        } catch (Exception ex) {
            throw new RuntimeException("Ошибка маппинга " + clazz.getSimpleName(), ex);
        }
    }

    private Object convertValueToFieldType(Object value, Class<?> targetType) {
        if (value == null) return null;

        // 1) напрямую подходящий тип
        if (targetType.isInstance(value)) {
            return value;
        }

        // 2) BigDecimal → числовые
        if (value instanceof BigDecimal bd) {
            if (targetType == Double.class || targetType == double.class)   return bd.doubleValue();
            if (targetType == Float.class  || targetType == float.class)    return bd.floatValue();
            if (targetType == Long.class   || targetType == long.class)     return bd.longValueExact();
            if (targetType == Integer.class|| targetType == int.class)      return bd.intValueExact();
            if (targetType == Short.class  || targetType == short.class)    return bd.shortValueExact();
            if (targetType == Byte.class   || targetType == byte.class)     return bd.byteValueExact();
            if (targetType == BigInteger.class)                             return bd.toBigInteger();
        }

        // 3) BigInteger → числовые
        if (value instanceof BigInteger bi) {
            if (targetType == BigDecimal.class) return new BigDecimal(bi);
            if (targetType == Long.class   || targetType == long.class)   return bi.longValue();
            if (targetType == Integer.class|| targetType == int.class)    return bi.intValue();
            if (targetType == Short.class  || targetType == short.class)  return bi.shortValue();
            if (targetType == Byte.class   || targetType == byte.class)   return bi.byteValue();
        }

        // 4) SQL‑типы дат/времени
        if (value instanceof java.sql.Date sqlDate) {
            if (targetType == LocalDate.class)      return sqlDate.toLocalDate();
            if (targetType == java.util.Date.class) return new java.util.Date(sqlDate.getTime());
        }
        if (value instanceof java.sql.Time sqlTime) {
            if (targetType == LocalTime.class)      return sqlTime.toLocalTime();
            if (targetType == java.util.Date.class) return new java.util.Date(sqlTime.getTime());
        }
        if (value instanceof java.sql.Timestamp sqlTs) {
            if (targetType == LocalDateTime.class)      return sqlTs.toLocalDateTime();
            if (targetType == Instant.class)            return sqlTs.toInstant();
            if (targetType == java.util.Date.class)     return new java.util.Date(sqlTs.getTime());
        }

        // java.util.Date → java.time
        if (value instanceof java.util.Date uDate) {
            Instant inst = Instant.ofEpochMilli(uDate.getTime());
            if (targetType == LocalDateTime.class)  return LocalDateTime.ofInstant(inst, ZoneId.systemDefault());
            if (targetType == LocalDate.class)      return LocalDateTime.ofInstant(inst, ZoneId.systemDefault()).toLocalDate();
            if (targetType == LocalTime.class)      return LocalDateTime.ofInstant(inst, ZoneId.systemDefault()).toLocalTime();
            if (targetType == Instant.class)        return inst;
        }

        // Boolean/boolean
        if (value instanceof Boolean b) {
            if (targetType == boolean.class || targetType == Boolean.class) return b;
        }
        // some DB drivers return number for boolean
        if (value instanceof Number bn
                && (targetType == boolean.class || targetType == Boolean.class)) {
            return bn.intValue() != 0;
        }
        if (value instanceof String bs
                && (targetType == boolean.class || targetType == Boolean.class)) {
            return Boolean.parseBoolean(bs);
        }

        // UUID
        if (value instanceof UUID u && targetType == UUID.class) {
            return u;
        }
        if (value instanceof String us && targetType == UUID.class) {
            return UUID.fromString(us);
        }

        // Enum
        if (targetType.isEnum()) {
            if (value instanceof String vs) {
                @SuppressWarnings("unchecked")
                Object enumVal = Enum.valueOf((Class<Enum>) targetType, vs);
                return enumVal;
            }
            if (value instanceof Number vn) {
                @SuppressWarnings("unchecked")
                Object[] constants = targetType.getEnumConstants();
                int idx = vn.intValue();
                if (idx >= 0 && idx < constants.length) return constants[idx];
            }
        }

        // Преобразование Number → простые числовые
        if (value instanceof Number n) {
            if (targetType == Integer.class|| targetType == int.class)    return n.intValue();
            if (targetType == Long.class   || targetType == long.class)   return n.longValue();
            if (targetType == Double.class || targetType == double.class) return n.doubleValue();
            if (targetType == Float.class  || targetType == float.class)  return n.floatValue();
            if (targetType == Short.class  || targetType == short.class)  return n.shortValue();
            if (targetType == Byte.class   || targetType == byte.class)   return n.byteValue();
        }

        // String → примитивы и java.time через парсинг
        if (value instanceof String s) {
            try {
                if (targetType == Integer.class|| targetType == int.class)      return Integer.parseInt(s);
                if (targetType == Long.class   || targetType == long.class)     return Long.parseLong(s);
                if (targetType == Double.class || targetType == double.class)   return Double.parseDouble(s);
                if (targetType == Float.class  || targetType == float.class)    return Float.parseFloat(s);
                if (targetType == Short.class  || targetType == short.class)    return Short.parseShort(s);
                if (targetType == Byte.class   || targetType == byte.class)     return Byte.parseByte(s);
                if (targetType == Boolean.class|| targetType == boolean.class)  return Boolean.parseBoolean(s);
                if (targetType == LocalDate.class)      return LocalDate.parse(s);
                if (targetType == LocalTime.class)      return LocalTime.parse(s);
                if (targetType == LocalDateTime.class)  return LocalDateTime.parse(s);
                if (targetType == Instant.class)        return Instant.parse(s);
                if (targetType == UUID.class)           return UUID.fromString(s);
                if (targetType == byte[].class)         return Base64.getDecoder().decode(s);
            } catch (Exception ex) {
                throw new IllegalArgumentException("Failed to parse String '" + s + "' to " + targetType, ex);
            }
        }

        // SQL‑BLOB → byte[]
        if (value instanceof Blob blob && targetType == byte[].class) {
            try {
                return blob.getBytes(1, (int) blob.length());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        // byte[]
        if (value instanceof byte[] ba && targetType == byte[].class) {
            return ba;
        }

        // Непосредственное приведение
        try {
            return targetType.cast(value);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(
                    "Cannot convert value of type " + value.getClass().getName() +
                            " to " + targetType.getName(), e
            );
        }
    }

    public Map<String, Field> getFields(Class<?> entityType) {
        return cache.computeIfAbsent(entityType, type -> {
            Map<String, Field> fields = new LinkedHashMap<>();
            for (Field field : type.getDeclaredFields()) {
                if (field.isAnnotationPresent(Id.class)) continue;
                if (field.isAnnotationPresent(com.db.kurs.orm.annotation.link.OneToMany.class)
                        || field.isAnnotationPresent(com.db.kurs.orm.annotation.link.ManyToMany.class)
                        || field.isAnnotationPresent(com.db.kurs.orm.annotation.link.OneToOne.class)
                        || field.isAnnotationPresent(com.db.kurs.orm.annotation.link.ManyToOne.class)) continue;
                Column c = field.getAnnotation(Column.class);
                String columnName = (c != null && !c.name().isEmpty())
                        ? c.name().toLowerCase()
                        : field.getName().toLowerCase();
                field.setAccessible(true);
                fields.put(columnName, field);
            }
            return fields;
        });
    }

    public Field getFieldWithIdAnnotation(Class<?> entityClass) {
        EntityMetadata em = new EntityMetadata(entityClass);
        Field idField = em.idFields.get(0);
        idField.setAccessible(true);
        return idField;
    }


}
