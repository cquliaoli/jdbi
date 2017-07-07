package org.jdbi.v3.core.replicator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.mapper.reflect.ColumnName;
import org.jdbi.v3.core.rule.H2DatabaseRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestReplicator {
    @Rule
    public H2DatabaseRule db = new H2DatabaseRule().withPlugin(new ReplicatorPlugin());
    private Handle h;

    @Before
    public void setup() throws Exception {
        h = db.getSharedHandle();
        h.execute("CREATE TABLE replicator (name varchar, value int, something uuid)");
        h.execute("INSERT INTO replicator VALUES (?,?,?)", "alice", 42, new UUID(42, 42));
    }

    @Test
    public void testAutoTypeReplicator() throws Exception {
        final SimpleResult r = h.createQuery("SELECT * FROM replicator").mapTo(SimpleResult.class).findOnly();
        assertThat(r.getName()).isEqualTo("alice");
        assertThat(r.getValue()).isEqualTo(42);
        assertThat(r.getUuid()).isEqualTo(new UUID(42, 42));
    }

    @Replicated
    public interface SimpleResult {
        String getName();
        int getValue();
        @ColumnName("something")
        UUID getUuid();
    }
}
