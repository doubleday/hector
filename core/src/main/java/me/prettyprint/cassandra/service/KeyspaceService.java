package me.prettyprint.cassandra.service;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.exceptions.HNotFoundException;
import me.prettyprint.hector.api.exceptions.HectorException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SuperColumn;

/**
 * The keyspace is a high level handle to all read/write operations to cassandra.
 *
 * A Keyspace object is NOT THREAD SAFE. Use one keyspace per thread please!
 *
 * @author rantav
 */
public interface KeyspaceService extends ThriftService {

  public static String CF_TYPE = "Type" ;
  public static String CF_TYPE_STANDARD = "Standard" ;
  public static String CF_TYPE_SUPER = "Super" ;

  Column getColumn(String key, ColumnPath columnPath) throws HectorException;

  SuperColumn getSuperColumn(String key, ColumnPath columnPath) throws HectorException;

  List<Column> getSlice(String key, ColumnParent columnParent, SlicePredicate predicate)
  throws HectorException;

  List<SuperColumn> getSuperSlice(String key, ColumnParent columnParent,
          SlicePredicate predicate) throws HectorException;

  void insert(String key, ColumnPath columnPath, ByteBuffer value) throws HectorException;

  void insert(String key, ColumnPath columnPath, ByteBuffer value, long timestamp) throws HectorException;

  /**
   * Call batch mutate with the BatchMutation object which encapsulates some of the complexity
   * of the batch_mutate API signature
   */
  void batchMutate(BatchMutation batchMutation) throws HectorException;

  void remove(String key, ColumnPath columnPath) throws HectorException;

  void remove(String key, ColumnPath columnPath, long timestamp) throws HectorException;
}
