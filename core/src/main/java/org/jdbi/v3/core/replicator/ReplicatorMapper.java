package org.jdbi.v3.core.replicator;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.jdbi.v3.core.generic.GenericTypes;
import org.jdbi.v3.core.mapper.ColumnMapper;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.mapper.reflect.ReflectionMappers;
import org.jdbi.v3.core.statement.StatementContext;

class ReplicatorMapper implements RowMapper<Object> {
    private final Type type;

    ReplicatorMapper(Type type) {
        this.type = type;
    }

    @Override
    public Object map(ResultSet rs, StatementContext ctx) throws SQLException {
        return specialize(rs, ctx).map(rs, ctx);
    }

    @Override
    public RowMapper<Object> specialize(ResultSet rs, StatementContext ctx) throws SQLException {
        final ResultSetMetaData md = rs.getMetaData();
        final BeanInfo info;
        try {
            info = Introspector.getBeanInfo(GenericTypes.getErasedType(type));
        } catch (IntrospectionException e) {
            throw new IllegalArgumentException("while inspecting " + type, e);
        }
        final ReflectionMappers rm = ctx.getConfig().get(ReflectionMappers.class);
        final Map<Method, Function<ResultSet, Object>> providers = new HashMap<>();
        for (PropertyDescriptor p : info.getPropertyDescriptors()) {
            Function<ResultSet, Object> provider = null;
            // First try to match a column and ColumnMapper to provide
            for (int i = 1; i < md.getColumnCount(); i++) {
                if (rm.columnNameMatches(md.getColumnLabel(i), p.getName())) {
                    if (provider != null) {
                        throw new IllegalStateException("Duplicate column provider found for " + p);
                    }
                    final ColumnMapper<?> m = ctx.findColumnMapperFor(p.getPropertyType())
                        .orElseThrow(() ->
                            new IllegalStateException("No column mapper found for type " + p.getPropertyType() + " of property " + p));
                    provider = r -> m.map(r, i, ctx);
                }
            }
            if (providers.put(p.getReadMethod(), provider) != null) {

            }
        }
    }

    @FunctionalInterface

}
