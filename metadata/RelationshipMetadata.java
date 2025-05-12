package com.db.kurs.orm.metadata;


import com.db.kurs.orm.annotation.link.*;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RelationshipMetadata {

    public enum RelationType { ONE_TO_MANY, MANY_TO_MANY, ONE_TO_ONE, MANY_TO_ONE }

    public final Field field;
    public final RelationType type;
    public final String mappedBy;             // для @OneToMany / inverse @OneToOne
    public final String joinTable;            // для @ManyToMany
    public final List<JoinColumn> joinColumns;// @JoinColumn или @JoinColumns
    public final FetchType fetch;

    public RelationshipMetadata(Field f) {
        this.field = f;
        f.setAccessible(true);

        OneToMany otm  = f.getAnnotation(OneToMany.class);
        ManyToMany mtm = f.getAnnotation(ManyToMany.class);
        OneToOne  oto  = f.getAnnotation(OneToOne.class);
        ManyToOne mto  = f.getAnnotation(ManyToOne.class);

        // контейнерная и одиночная аннотации
        JoinColumns multi = f.getAnnotation(JoinColumns.class);
        JoinColumn  single = f.getAnnotation(JoinColumn.class);

        if (multi != null) {
            joinColumns = Arrays.asList(multi.value());
        } else if (single != null) {
            joinColumns = Collections.singletonList(single);
        } else {
            joinColumns = Collections.emptyList();
        }

        if (otm != null) {
            type      = RelationType.ONE_TO_MANY;
            mappedBy  = otm.mappedBy();
            fetch     = otm.fetch();
            joinTable = null;
        }
        else if (mtm != null) {
            type               = RelationType.MANY_TO_MANY;
            mappedBy           = null;
            fetch              = mtm.fetch();
            joinTable          = mtm.joinTable();
        }
        else if (oto != null) {
            type               = RelationType.ONE_TO_ONE;
            mappedBy           = oto.mappedBy();
            fetch              = oto.fetch();
            joinTable          = null;
        }
        else if (mto != null) {
            type               = RelationType.MANY_TO_ONE;
            mappedBy           = null;
            fetch              = mto.fetch();
            joinTable          = null;
        }
        else {
            throw new IllegalArgumentException("Not a relation: " + f.getName());
        }
    }
}
