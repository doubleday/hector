package me.prettyprint.cassandra.service;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.connection.HConnectionManager;
import me.prettyprint.cassandra.model.thrift.ThriftConverter;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.exceptions.HectorTransportException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a Keyspace
 *
 * @author Ran Tavory (rantav@gmail.com)
 *
 */
public class KeyspaceServiceImpl extends ThriftServiceImpl implements KeyspaceService {
  private static final Map<String, String> EMPTY_CREDENTIALS = Collections.emptyMap();

  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(KeyspaceServiceImpl.class);

  public KeyspaceServiceImpl(String keyspaceName,
      ConsistencyLevelPolicy consistencyLevel,
      HConnectionManager connectionManager,
      FailoverPolicy failoverPolicy)
      throws HectorTransportException {
    this(keyspaceName, consistencyLevel, connectionManager, failoverPolicy, EMPTY_CREDENTIALS);
  }

  public KeyspaceServiceImpl(String keyspaceName,
      ConsistencyLevelPolicy consistencyLevel,
      HConnectionManager connectionManager,
      FailoverPolicy failoverPolicy,
      Map<String, String> credentials)
      throws HectorTransportException {
      super(keyspaceName, consistencyLevel, connectionManager, failoverPolicy, credentials);
  }


  @SuppressWarnings({"unchecked"})
  @Override
  public void batchMutate(BatchMutation batchMutate) throws HectorException {
    batchMutate(batchMutate.getMutationMap());
  }


  private void operateWithFailover(Operation<?> op) throws HectorException {
    connectionManager.operateWithFailover(op);
    this.cassandraHost = op.getCassandraHost();
  }

  @Override
  public List<Column> getSlice(String key, ColumnParent columnParent, SlicePredicate predicate)
  throws HectorException {
      return getSlice(StringSerializer.get().toByteBuffer(key), columnParent, predicate);
  }

  @Override
  public List<SuperColumn> getSuperSlice(String key, ColumnParent columnParent,
          SlicePredicate predicate) throws HectorException {
      return getSuperSlice(StringSerializer.get().toByteBuffer(key), columnParent, predicate);
  }

  @Override
  public SuperColumn getSuperColumn(String key, ColumnPath columnPath) throws HectorException {
      return getSuperColumn(StringSerializer.get().toByteBuffer(key), columnPath);
  }

  @Override
  public void insert(String key, ColumnPath columnPath, ByteBuffer value) throws HectorException {
//    valideColumnPath(columnPath);
      insert(StringSerializer.get().toByteBuffer(key), columnPath, value);
  }

  @Override
  public void insert(String key, ColumnPath columnPath, ByteBuffer value, long timestamp) throws HectorException {
//    valideColumnPath(columnPath);
      insert(StringSerializer.get().toByteBuffer(key), columnPath, value, timestamp);
  }

  @Override
  public void remove(String key, ColumnPath columnPath) throws HectorException {
    remove(StringSerializer.get().toByteBuffer(key), columnPath);
  }

  /**
   * Same as two argument version, but the caller must specify their own timestamp
   */
  @Override
  public void remove(String key, ColumnPath columnPath, long timestamp) throws HectorException {
    remove(StringSerializer.get().toByteBuffer(key), columnPath, timestamp);
  }


  @Override
  public Column getColumn(String key, ColumnPath columnPath) throws HectorException {
      return getColumn(StringSerializer.get().toByteBuffer(key), columnPath);
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("KeyspaceImpl<");
    b.append(keyspaceName);
    b.append(">");
    return b.toString();
  }
}
