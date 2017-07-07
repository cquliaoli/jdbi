package org.jdbi.v3.core.replicator;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.jdbi.v3.core.config.JdbiConfig;
import org.jdbi.v3.core.generic.GenericTypes;
import org.jdbi.v3.core.mapper.RowMapper;

public class ReplicatedTypes implements JdbiConfig<ReplicatedTypes> {

    private final Map<Type, Optional<RowMapper<?>>> replicatedTypes = new HashMap<>();

    public ReplicatedTypes() { }
    public ReplicatedTypes(ReplicatedTypes other) {
        replicatedTypes.putAll(other.replicatedTypes);
    }

    @Override
    public ReplicatedTypes createCopy() {
        return new ReplicatedTypes(this);
    }

    Optional<RowMapper<?>> mapperFor(Type type) {
        return replicatedTypes.computeIfAbsent(type, this::constructMapper);
    }

    private Optional<RowMapper<?>> constructMapper(Type type) {
        if (GenericTypes.getErasedType(type).getAnnotation(Replicated.class) == null) {
            return Optional.empty();
        }
        return Optional.of(new ReplicatorMapper(type));
    }
}
