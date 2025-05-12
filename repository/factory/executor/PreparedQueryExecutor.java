package com.db.kurs.orm.repository.factory.executor;

import com.db.kurs.orm.mapper.EntityMapper;
import com.db.kurs.orm.mapper.QueryExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PreparedQueryExecutor implements QueryExecutor {

    private final JdbcTemplate jdbcTemplate;
    private final EntityMapper entityMapper;
    private final String parsedSql;
    private final int[] paramOrder;
    private final Class<?> elementType;
    private final boolean isList;

    public PreparedQueryExecutor(JdbcTemplate jdbcTemplate,
                                 EntityMapper entityMapper,
                                 String sql,
                                 Class<?> elementType,
                                 boolean isList) {
        this.jdbcTemplate = jdbcTemplate;
        this.entityMapper = entityMapper;
        this.elementType = elementType;
        this.isList = isList;

        // 1) Найти все вхождения ?<number> и собрать номера
        Pattern p = Pattern.compile("\\?(\\d+)");
        Matcher m = p.matcher(sql);
        StringBuffer sb = new StringBuffer();
        List<Integer> order = new ArrayList<>();
        while (m.find()) {
            int idx = Integer.parseInt(m.group(1)) - 1; // 1‑based -> 0‑based
            order.add(idx);
            m.appendReplacement(sb, "?"); // заменяем "?2" → "?"
        }
        m.appendTail(sb);

        this.parsedSql = sb.toString();
        this.paramOrder = order.isEmpty()
                ? null
                : order.stream().mapToInt(i -> i).toArray();
    }

    @Override
    public Object execute(Object[] args) {
        Object[] finalArgs = args;
        if (paramOrder != null) {
            finalArgs = new Object[paramOrder.length];
            for (int i = 0; i < paramOrder.length; i++) {
                finalArgs[i] = args[paramOrder[i]];
            }
        }

        if (void.class.equals(elementType)) {
            // DML
            if (finalArgs != null && finalArgs.length > 0) {
                jdbcTemplate.update(parsedSql, finalArgs);
            } else {
                jdbcTemplate.update(parsedSql);
            }
            return null;
        }

        RowMapper<?> rm = (rs, rowNum) -> entityMapper.map(rs, elementType);
        List<?> result;

        if (finalArgs != null && finalArgs.length > 0) {
            // Определяем JDBC-типы для каждого аргумента
            int[] types = resolveTypes(finalArgs);
            result = jdbcTemplate.query(parsedSql, finalArgs, types, rm);
        } else {
            result = jdbcTemplate.query(parsedSql, rm);
        }

        return isList ? result : (result.isEmpty() ? null : result.get(0));
    }

    /**
     * Вспомогательный метод: на основе типа Java-объекта возвращает java.sql.Types
     */
    private int[] resolveTypes(Object[] args) {
        int[] types = new int[args.length];
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg == null) {
                types[i] = Types.NULL;
            } else if (arg instanceof String) {
                types[i] = Types.VARCHAR;
            } else if (arg instanceof Integer) {
                types[i] = Types.INTEGER;
            } else if (arg instanceof Long) {
                types[i] = Types.BIGINT;
            } else if (arg instanceof Boolean) {
                types[i] = Types.BOOLEAN;
            } else if (arg instanceof Double) {
                types[i] = Types.DOUBLE;
            } else if (arg instanceof Float) {
                types[i] = Types.FLOAT;
            } else if (arg instanceof Short) {
                types[i] = Types.SMALLINT;
            } else if (arg instanceof java.math.BigDecimal) {
                types[i] = Types.NUMERIC;
            } else if (arg instanceof java.sql.Date) {
                types[i] = Types.DATE;
            } else if (arg instanceof java.sql.Time) {
                types[i] = Types.TIME;
            } else if (arg instanceof java.sql.Timestamp) {
                types[i] = Types.TIMESTAMP;
            } else {
                types[i] = Types.JAVA_OBJECT;
            }
        }
        return types;
    }
}
