/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.rocksdb.index;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;

/**
 * Index implementation which indexes the values for a single column in the base
 * table and which stores its index data in a local, hidden RocksDB table.
 */
public class RocksandraClusteringColumnIndex implements Index
{
    private static final Logger logger = LoggerFactory.getLogger(RocksandraClusteringColumnIndex.class);

    public static final Pattern TARGET_REGEX = Pattern.compile("^(keys|entries|values|full)\\((.+)\\)$");

    public final ColumnFamilyStore baseCfs;
    public IndexMetadata metadata;
    public ColumnFamilyStore indexCfs;
    public ColumnDefinition indexedColumn;

    public RocksandraClusteringColumnIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef)
    {
        this.baseCfs = baseCfs;
        setMetadata(indexDef);
    }

    /**
     * Returns true if an index of this type can support search predicates of the form [column] OPERATOR [value]
     * @param indexedColumn
     * @param operator
     * @return
     */
    public boolean supportsOperator(ColumnDefinition indexedColumn, Operator operator)
    {
        return operator == Operator.EQ;
    }

    /**
     * Used to construct an the clustering for an entry in the index table based on values from the base data.
     * The clustering columns in the index table encode the values required to retrieve the correct data from the base
     * table and varies depending on the kind of the indexed column. See indexCfsMetadata for more details
     * Used whenever a row in the index table is written or deleted.
     * @param partitionKey from the base data being indexed
     * @param prefix from the base data being indexed
     * @param path from the base data being indexed
     * @return a clustering prefix to be used to insert into the index table
     */
    protected CBuilder buildIndexClusteringPrefix(ByteBuffer partitionKey,
                                                  ClusteringPrefix prefix,
                                                  CellPath path)
    {
        CBuilder builder = CBuilder.create(getIndexComparator());
        builder.add(partitionKey);
        for (int i = 0; i < Math.min(indexedColumn.position(), prefix.size()); i++)
            builder.add(prefix.get(i));
        for (int i = indexedColumn.position() + 1; i < prefix.size(); i++)
            builder.add(prefix.get(i));
        return builder;
    }

    /**
     * Used at search time to convert a row in the index table into a simple struct containing the values required
     * to retrieve the corresponding row from the base table.
     * @param indexedValue the partition key of the indexed table (i.e. the value that was indexed)
     * @param indexEntry a row from the index table
     * @return
     */
    public IndexEntry decodeEntry(DecoratedKey indexedValue, Row indexEntry)
    {
        int ckCount = baseCfs.metadata.clusteringColumns().size();

        Clustering clustering = indexEntry.clustering();
        CBuilder builder = CBuilder.create(baseCfs.getComparator());
        for (int i = 0; i < indexedColumn.position(); i++)
            builder.add(clustering.get(i + 1));

        builder.add(indexedValue.getKey());

        for (int i = indexedColumn.position() + 1; i < ckCount; i++)
            builder.add(clustering.get(i));

        return new IndexEntry(indexedValue,
                              clustering,
                              indexEntry.primaryKeyLivenessInfo().timestamp(),
                              clustering.get(0),
                              builder.build());
    }

    /**
     * Extract the value to be inserted into the index from the components of the base data
     * @param partitionKey from the primary data
     * @param clustering from the primary data
     * @param path from the primary data
     * @param cellValue from the primary data
     * @return a ByteBuffer containing the value to be inserted in the index. This will be used to make the partition
     * key in the index table
     */
    protected ByteBuffer getIndexedValue(ByteBuffer partitionKey,
                                         Clustering clustering,
                                         CellPath path, ByteBuffer cellValue)
    {
        return clustering.get(indexedColumn.position());
    }

    public ClusteringComparator getIndexComparator()
    {
        return indexCfs.metadata.comparator;
    }

    public void register(IndexRegistry registry)
    {
        registry.registerIndex(this);
    }

    public Callable<?> getInitializationTask()
    {
        return () -> {
            RocksandraSecondaryIndexBuilder builder = new RocksandraSecondaryIndexBuilder(baseCfs);
            builder.build();
            baseCfs.indexManager.markIndexBuilt(metadata.name);
            return null;
        };
    }

    public IndexMetadata getIndexMetadata()
    {
        return metadata;
    }

    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return indexCfs == null ? Optional.empty() : Optional.of(indexCfs);
    }

    public Callable<Void> getBlockingFlushTask()
    {
        return null;
    }

    public Callable<?> getInvalidateTask()
    {
        return () -> {
            invalidate();
            return null;
        };
    }

    public Callable<?> getMetadataReloadTask(IndexMetadata indexDef)
    {
        return null;
    }

    @Override
    public void validate(ReadCommand command) throws InvalidRequestException
    {
        Optional<RowFilter.Expression> target = getTargetExpression(command.rowFilter().getExpressions());

        if (target.isPresent())
        {
            ByteBuffer indexValue = target.get().getIndexValue();
            checkFalse(indexValue.remaining() > FBUtilities.MAX_UNSIGNED_SHORT,
                       "Index expression values may not be larger than 64K");
        }
    }

    private void setMetadata(IndexMetadata indexDef)
    {
        metadata = indexDef;
        Pair<ColumnDefinition, IndexTarget.Type> target = parseTarget(baseCfs.metadata, indexDef);
        CFMetaData cfm = indexCfsMetadata(baseCfs.metadata, indexDef);
        indexCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.keyspace,
                                                             cfm.cfName,
                                                             cfm,
                                                             false);
        indexedColumn = target.left;
    }

    public Callable<?> getTruncateTask(final long truncatedAt)
    {
        return null;
    }

    public boolean shouldBuildBlocking()
    {
        // built-in indexes are always included in builds initiated from SecondaryIndexManager
        return true;
    }

    public boolean dependsOn(ColumnDefinition column)
    {
        return indexedColumn.name.equals(column.name);
    }

    public boolean supportsExpression(ColumnDefinition column, Operator operator)
    {
        return indexedColumn.name.equals(column.name)
               && supportsOperator(indexedColumn, operator);
    }

    private boolean supportsExpression(RowFilter.Expression expression)
    {
        return supportsExpression(expression.column(), expression.operator());
    }

    public AbstractType<?> customExpressionValueType()
    {
        return null;
    }

    public long getEstimatedResultRows()
    {
        return indexCfs.getMeanColumns();
    }

    /**
     * No post processing of query results, just return them unchanged
     */
    public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(ReadCommand command)
    {
        return (partitionIterator, readCommand) -> partitionIterator;
    }

    public RowFilter getPostIndexQueryFilter(RowFilter filter)
    {
        return getTargetExpression(filter.getExpressions()).map(filter::without)
                                                           .orElse(filter);
    }

    private Optional<RowFilter.Expression> getTargetExpression(List<RowFilter.Expression> expressions)
    {
        return expressions.stream().filter(this::supportsExpression).findFirst();
    }

    public Index.Searcher searcherFor(ReadCommand command)
    {
        Optional<RowFilter.Expression> target = getTargetExpression(command.rowFilter().getExpressions());

        if (target.isPresent())
        {
            target.get().validateForIndexing();
            return new CompositesSearcher(command, target.get(), this);
        }

        return null;
    }

    public void validate(PartitionUpdate update) throws InvalidRequestException
    {
        validateClusterings(update);
    }

    public Indexer indexerFor(final DecoratedKey key,
                              final PartitionColumns columns,
                              final int nowInSec,
                              final OpOrder.Group opGroup,
                              final IndexTransaction.Type transactionType)
    {
        /**
         * Indexes on regular and static columns (the non primary-key ones) only care about updates with live
         * data for the column they index. In particular, they don't care about having just row or range deletions
         * as they don't know how to update the index table unless they know exactly the value that is deleted.
         *
         * Note that in practice this means that those indexes are only purged of stale entries on compaction,
         * when we resolve both the deletion and the prior data it deletes. Of course, such stale entries are also
         * filtered on read.
         */
        if (!isPrimaryKeyIndex() && !columns.contains(indexedColumn))
            return null;

        return new Indexer()
        {
            public void begin()
            {
            }

            public void partitionDelete(DeletionTime deletionTime)
            {
            }

            public void rangeTombstone(RangeTombstone tombstone)
            {
            }

            public void insertRow(Row row)
            {
                if (row.isStatic() && !indexedColumn.isStatic() && !indexedColumn.isPartitionKey())
                    return;

                // scope of Rocksandra secondary index is that the index will always be on a primary key
                assert isPrimaryKeyIndex();

                indexPrimaryKey(row.clustering(),
                                getPrimaryKeyIndexLiveness(row),
                                row.deletion());
            }

            public void removeRow(Row row)
            {
            }

            public void updateRow(Row oldRow, Row newRow)
            {
            }

            public void finish()
            {
            }

            private void indexPrimaryKey(final Clustering clustering,
                                         final LivenessInfo liveness,
                                         final Row.Deletion deletion)
            {
                if (liveness.timestamp() != LivenessInfo.NO_TIMESTAMP)
                    insert(key.getKey(), clustering, null, liveness, opGroup);

                if (!deletion.isLive())
                    delete(key.getKey(), clustering, deletion.time(), opGroup);
            }

            private LivenessInfo getPrimaryKeyIndexLiveness(Row row)
            {
                long timestamp = row.primaryKeyLivenessInfo().timestamp();
                int ttl = row.primaryKeyLivenessInfo().ttl();
                for (Cell cell : row.cells())
                {
                    long cellTimestamp = cell.timestamp();
                    if (cell.isLive(nowInSec))
                    {
                        if (cellTimestamp > timestamp)
                        {
                            timestamp = cellTimestamp;
                            ttl = cell.ttl();
                        }
                    }
                }
                return LivenessInfo.create(baseCfs.metadata, timestamp, ttl, nowInSec);
            }
        };
    }

    /**
     * Called when adding a new entry to the index
     */
    private void insert(ByteBuffer rowKey,
                        Clustering clustering,
                        Cell cell,
                        LivenessInfo info,
                        OpOrder.Group opGroup)
    {
        DecoratedKey valueKey = getIndexKeyFor(getIndexedValue(rowKey,
                                                               clustering,
                                                               cell));
        Row row = BTreeRow.noCellLiveRow(buildIndexClustering(rowKey, clustering, cell), info);
        PartitionUpdate upd = partitionUpdate(valueKey, row);
        assert baseCfs.keyspace.engine != null;
        indexCfs.keyspace.engineApply(indexCfs, upd, UpdateTransaction.NO_OP, opGroup, null, false);
        logger.trace("Inserted entry into index for value {}", valueKey);
    }

    /**
     * Called when deleting entries from indexes on primary key columns
     */
    private void delete(ByteBuffer rowKey,
                        Clustering clustering,
                        DeletionTime deletion,
                        OpOrder.Group opGroup)
    {
        DecoratedKey valueKey = getIndexKeyFor(getIndexedValue(rowKey,
                                                               clustering,
                                                               null));
        doDelete(valueKey,
                 buildIndexClustering(rowKey, clustering, null),
                 deletion,
                 opGroup);
    }

    private void doDelete(DecoratedKey indexKey,
                          Clustering indexClustering,
                          DeletionTime deletion,
                          OpOrder.Group opGroup)
    {
        Row row = BTreeRow.emptyDeletedRow(indexClustering, Row.Deletion.regular(deletion));
        PartitionUpdate upd = partitionUpdate(indexKey, row);
        indexCfs.keyspace.engineApply(indexCfs, upd, UpdateTransaction.NO_OP, opGroup, null, false);
        logger.trace("Removed index entry for value {}", indexKey);
    }

    private void validateClusterings(PartitionUpdate update) throws InvalidRequestException
    {
        assert indexedColumn.isClusteringColumn();
        for (Row row : update)
            validateIndexedValue(getIndexedValue(null, row.clustering(), null));
    }

    private void validateIndexedValue(ByteBuffer value)
    {
        if (value != null && value.remaining() >= FBUtilities.MAX_UNSIGNED_SHORT)
            throw new InvalidRequestException(String.format(
                                                           "Cannot index value of size %d for index %s on %s.%s(%s) (maximum allowed size=%d)",
                                                           value.remaining(),
                                                           metadata.name,
                                                           baseCfs.metadata.ksName,
                                                           baseCfs.metadata.cfName,
                                                           indexedColumn.name.toString(),
                                                           FBUtilities.MAX_UNSIGNED_SHORT));
    }

    private ByteBuffer getIndexedValue(ByteBuffer rowKey,
                                       Clustering clustering,
                                       Cell cell)
    {
        return getIndexedValue(rowKey,
                               clustering,
                               cell == null ? null : cell.path(),
                               cell == null ? null : cell.value()
        );
    }

    private Clustering buildIndexClustering(ByteBuffer rowKey,
                                            Clustering clustering,
                                            Cell cell)
    {
        return buildIndexClusteringPrefix(rowKey,
                                          clustering,
                                          cell == null ? null : cell.path()).build();
    }

    private DecoratedKey getIndexKeyFor(ByteBuffer value)
    {
        return indexCfs.decorateKey(value);
    }

    private PartitionUpdate partitionUpdate(DecoratedKey valueKey, Row row)
    {
        return PartitionUpdate.singleRowUpdate(indexCfs.metadata, valueKey, row);
    }

    private void invalidate()
    {
        Keyspace.writeOrder.awaitNewBarrier();
        indexCfs.forceBlockingFlush();
        indexCfs.readOrdering.awaitNewBarrier();
        indexCfs.engine.truncate(indexCfs);
        indexCfs.invalidate();
    }

    private boolean isPrimaryKeyIndex()
    {
        return indexedColumn.isPrimaryKeyColumn();
    }

    /**
     * Construct the CFMetadata for an index table, the clustering columns in the index table
     * vary dependent on the kind of the indexed value.
     * @param baseCfsMetadata
     * @param indexMetadata
     * @return
     */
    public static final CFMetaData indexCfsMetadata(CFMetaData baseCfsMetadata, IndexMetadata indexMetadata)
    {
        Pair<ColumnDefinition, IndexTarget.Type> target = parseTarget(baseCfsMetadata, indexMetadata);
        ColumnDefinition indexedColumn = target.left;
        AbstractType<?> indexedValueType = indexedColumn.type;

        // Tables for legacy KEYS indexes are non-compound and dense
        CFMetaData.Builder builder = CFMetaData.Builder.create(baseCfsMetadata.ksName,
                                                                 baseCfsMetadata.indexColumnFamilyName(indexMetadata));

        builder =  builder.withId(baseCfsMetadata.cfId)
                          .withPartitioner(new LocalPartitioner(indexedValueType))
                          .addPartitionKey(indexedColumn.name, indexedColumn.type)
                          .addClusteringColumn("partition_key", baseCfsMetadata.partitioner.partitionOrdering());

        builder = addIndexClusteringColumns(builder, baseCfsMetadata, indexedColumn);

        return builder.build().reloadIndexMetadataProperties(baseCfsMetadata);
    }

    // Public because it's also used to convert index metadata into a thrift-compatible format
    public static Pair<ColumnDefinition, IndexTarget.Type> parseTarget(CFMetaData cfm,
                                                                       IndexMetadata indexDef)
    {
        String target = indexDef.options.get("target");
        assert target != null : String.format("No target definition found for index %s", indexDef.name);

        // if the regex matches then the target is in the form "keys(foo)", "entries(bar)" etc
        // if not, then it must be a simple column name and implictly its type is VALUES
        Matcher matcher = TARGET_REGEX.matcher(target);
        String columnName;
        IndexTarget.Type targetType;
        if (matcher.matches())
        {
            targetType = IndexTarget.Type.fromString(matcher.group(1));
            columnName = matcher.group(2);
        }
        else
        {
            columnName = target;
            targetType = IndexTarget.Type.VALUES;
        }

        // in the case of a quoted column name the name in the target string
        // will be enclosed in quotes, which we need to unwrap. It may also
        // include quote characters internally, escaped like so:
        //      abc"def -> abc""def.
        // Because the target string is stored in a CQL compatible form, we
        // need to un-escape any such quotes to get the actual column name
        if (columnName.startsWith("\""))
        {
            columnName = StringUtils.substring(StringUtils.substring(columnName, 1), 0, -1);
            columnName = columnName.replaceAll("\"\"", "\"");
        }

        // if it's not a CQL table, we can't assume that the column name is utf8, so
        // in that case we have to do a linear scan of the cfm's columns to get the matching one
        if (cfm.isCQLTable())
            return Pair.create(cfm.getColumnDefinition(new ColumnIdentifier(columnName, true)), targetType);
        else
            for (ColumnDefinition column : cfm.allColumns())
                if (column.name.toString().equals(columnName))
                    return Pair.create(column, targetType);

        throw new RuntimeException(String.format("Unable to parse targets for index %s (%s)", indexDef.name, target));
    }

    private static final CFMetaData.Builder addIndexClusteringColumns(CFMetaData.Builder builder,
                                                                      CFMetaData baseMetadata,
                                                                      ColumnDefinition columnDef)
    {
        List<ColumnDefinition> cks = baseMetadata.clusteringColumns();
        for (int i = 0; i < columnDef.position(); i++)
        {
            ColumnDefinition def = cks.get(i);
            builder.addClusteringColumn(def.name, def.type);
        }
        for (int i = columnDef.position() + 1; i < cks.size(); i++)
        {
            ColumnDefinition def = cks.get(i);
            builder.addClusteringColumn(def.name, def.type);
        }
        return builder;
    }
}
