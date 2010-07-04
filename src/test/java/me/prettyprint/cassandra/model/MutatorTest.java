package me.prettyprint.cassandra.model;

import static me.prettyprint.cassandra.model.HFactory.createColumn;
import static me.prettyprint.cassandra.model.HFactory.createKeyspaceOperator;
import static me.prettyprint.cassandra.model.HFactory.createMutator;
import static me.prettyprint.cassandra.model.HFactory.getOrCreateCluster;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import me.prettyprint.cassandra.BaseEmbededServerSetupTest;
import me.prettyprint.cassandra.extractors.StringExtractor;
import me.prettyprint.cassandra.service.Cluster;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
public class MutatorTest extends BaseEmbededServerSetupTest {

  private static final StringExtractor se = new StringExtractor();

  private Cluster cluster;
  private KeyspaceOperator keyspaceOperator;

  @Before
  public void doSetup() {
    cluster = getOrCreateCluster("Test Cluster", "127.0.0.1:9170");
    keyspaceOperator = createKeyspaceOperator("Keyspace1", cluster);
  }

  @Test
  @Ignore
  public void testInsert() {
    /*
    Mutator m = MutatorFactory.createMutator(keyspaceOperator);
    MutationResult mr = m.insert("testInsertGetRemove", "Standard1",
        m.createColumn("testInsertGetRemove", "testInsertGetRemove_value_"));
     */
  }

  @Test
  public void testBatchMutationManagement() {
    String cf = "Standard1";

    Mutator m = createMutator(keyspaceOperator);
    for (int i = 0; i < 5; i++) {
      m.addInsertion("k" + i, cf, createColumn("name", "value" + i, se, se));
    }
    MutationResult r = m.execute();
    assertTrue("Execute time should be > 0", r.getExecutionTimeMicro() > 0);

    // Execute an empty mutation
    r = m.execute();
    assertEquals("Execute time should be 0", 0, r.getExecutionTimeMicro());

    // Test discard and then exec an empty mutation
    for (int i = 0; i < 5; i++) {
      m.addInsertion("k" + i, cf, createColumn("name", "value" + i, se, se));
    }
    m.discardPendingMutations();
    r = m.execute();
    assertEquals("Execute time should be 0", 0, r.getExecutionTimeMicro());

    // cleanup
    for (int i = 0; i < 5; i++) {
      m.addDeletion("k" + i, cf, "name", se);
    }
    m.execute();
  }

}
