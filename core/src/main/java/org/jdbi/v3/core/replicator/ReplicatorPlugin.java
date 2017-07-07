package org.jdbi.v3.core.replicator;

import java.lang.reflect.Type;
import java.util.Optional;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.config.ConfigRegistry;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.mapper.RowMapperFactory;
import org.jdbi.v3.core.spi.JdbiPlugin;

public class ReplicatorPlugin implements JdbiPlugin {
    @Override
    public void customizeJdbi(Jdbi db) {
        db.registerRowMapper(new ReplicatorRowMapperFactory());
    }

    class ReplicatorRowMapperFactory implements RowMapperFactory {
        @Override
        public Optional<RowMapper<?>> build(Type type, ConfigRegistry config) {
            return config.get(ReplicatedTypes.class).mapperFor(type);
        }
    }
}
