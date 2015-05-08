/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.mapreduce;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.naming.NamingException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.StringUtils;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.KeyRange;

/**
 * {@link InputFormat} implementation from Phoenix.
 *
 * @param <T>
 */
public class PhoenixInputFormat<T extends DBWritable>
        extends InputFormat<NullWritable, T>
        implements Configurable {

    private static final Log LOG = LogFactory.getLog(PhoenixInputFormat.class);

    private Configuration conf;
    private HTable table;
    private String nameServer;
    private HashMap<InetAddress, String> reverseDNSCacheMap = new HashMap<>();

    /**
     * instantiated by framework
     */
    public PhoenixInputFormat() {
    }

    @Override
    public RecordReader<NullWritable, T> createRecordReader(InputSplit split,
            TaskAttemptContext context)
            throws IOException, InterruptedException {

        final Configuration configuration = context.getConfiguration();
        final QueryPlan queryPlan = getQueryPlan(context, configuration);
        @SuppressWarnings("unchecked")
        final Class<T> inputClass = (Class<T>) PhoenixConfigurationUtil.getInputClass(
                configuration);
        return new PhoenixRecordReader<>(inputClass, configuration, queryPlan);
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException,
            InterruptedException {
        final Configuration configuration = context.getConfiguration();
        final QueryPlan queryPlan = getQueryPlan(context, configuration);
        final List<KeyRange> allSplits = queryPlan.getSplits();
        final List<InputSplit> splits = generateSplits(context
                , queryPlan, allSplits);
        return splits;
    }

    private List<InputSplit> generateSplits(final JobContext context
            , final QueryPlan qplan, final List<KeyRange> splits)
                    throws IOException {
        Preconditions.checkNotNull(qplan);
        Preconditions.checkNotNull(splits);
        TreeMap<String, String[]> partitionLocations
                = getPartitionLocations(context);
        final List<InputSplit> psplits = Lists.newArrayListWithExpectedSize(
                splits.size());
        for (List<Scan> scans : qplan.getScans()) {
            for (Scan scan : scans) {
                PhoenixInputSplit curr
                        = new PhoenixInputSplit(Lists.newArrayList(scan));

                String startKey = Bytes.toString(scan.getStartRow());

                if (partitionLocations != null) {
                    Map.Entry<String, String[]> partitionLocationEntry
                            = partitionLocations.floorEntry(startKey);

                    if (partitionLocationEntry != null) {
                        curr.setLocations(partitionLocationEntry.getValue());
                    }
                }

                psplits.add(curr);
            }
        }

        return psplits;
    }

    /**
     * Returns the query plan associated with the select query.
     *
     * @param context
     * @return
     * @throws IOException
     * @throws SQLException
     */
    private QueryPlan getQueryPlan(final JobContext context,
            final Configuration configuration) throws IOException {
        Preconditions.checkNotNull(context);
        try {
            final Connection connection = ConnectionUtil.getConnection(
                    configuration);
            final String selectStatement = PhoenixConfigurationUtil.getSelectStatement(
                    configuration);
            Preconditions.checkNotNull(selectStatement);
            final Statement statement = connection.createStatement();
            final PhoenixStatement pstmt = statement.unwrap(
                    PhoenixStatement.class);
            // Optimize the query plan so that we potentially use secondary indexes
            final QueryPlan queryPlan = pstmt.optimizeQuery(selectStatement);
            // Initialize the query plan so it sets up the parallel scans
            queryPlan.iterator();
            return queryPlan;
        }
        catch (Exception exception) {
            LOG.error(String.format(
                    "Failed to get the query plan with error [%s]",
                    exception.getMessage()));
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        String tableName = PhoenixConfigurationUtil.getInputTableName(conf);

        try {
            table = new HTable(
                    new Configuration(conf), tableName.toUpperCase());
        }
        catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
        }
    }

    /**
     * Get a map from key to host of the machine containing that partition
     *
     * @return
     */
    private TreeMap<String, String[]> getPartitionLocations(JobContext context)
            throws IOException {
        if (table == null) {
            LOG.warn("No hbase table was determined, so partition locations "
                    + "cannot be determined");
            return null;
        }

        TreeMap<String, String[]> result = Maps.newTreeMap();

        // Get the name server address and the default value is null.
        nameServer = context.getConfiguration().get("hbase.nameserver.address"
                        , null);

        RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(table);

        Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
        List<TableSplit> splits;

        if (keys == null || keys.getFirst() == null
                || keys.getFirst().length == 0) {

            HRegionLocation regLoc = table.getRegionLocation(
                    HConstants.EMPTY_BYTE_ARRAY, false);

            if (null == regLoc) {
                throw new IOException("Expecting at least one region.");
            }

            splits = new ArrayList<>(1);
            long regionSize = sizeCalculator.getRegionSize(
                    regLoc.getRegionInfo().getRegionName());
            TableSplit split = new TableSplit(table.getName(),
                    HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
                    regLoc
                    .getHostnamePort().split(Addressing.HOSTNAME_PORT_SEPARATOR)[0],
                    regionSize);
            splits.add(split);
        }
        else {
            splits = new ArrayList<>(
                    keys.getFirst().length);
            for (int i = 0; i < keys.getFirst().length; i++) {
                HRegionLocation location = table.getRegionLocation(
                        keys.getFirst()[i], false);
                // The below InetSocketAddress creation does a name resolution.
                InetSocketAddress isa = new InetSocketAddress(location.getHostname(),
                        location.getPort());
                if (isa.isUnresolved()) {
                    LOG.warn("Failed resolve " + isa);
                }
                InetAddress regionAddress = isa.getAddress();
                String regionLocation;
                try {
                    regionLocation = reverseDNS(regionAddress);
                }
                catch (NamingException e) {
                    LOG.warn(
                            "Cannot resolve the host name for " + regionAddress
                                    + " because of " + e);
                    regionLocation = location.getHostname();
                }

                // These are the row keys for a complete scan
                byte[] startRow = {};
                byte[] stopRow = {};

                // determine if the given start an stop key fall into the region
                if ((startRow.length == 0 || keys.getSecond()[i].length == 0
                        || Bytes.compareTo(startRow, keys.getSecond()[i]) < 0)
                        && (stopRow.length == 0
                        || Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
                    byte[] splitStart = startRow.length == 0
                            || Bytes.compareTo(keys.getFirst()[i], startRow) >= 0
                                    ? keys.getFirst()[i] : startRow;
                    byte[] splitStop = (stopRow.length == 0
                            || Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0)
                            && keys.getSecond()[i].length > 0
                                    ? keys.getSecond()[i] : stopRow;

                    byte[] regionName = location.getRegionInfo().getRegionName();
                    long regionSize = sizeCalculator.getRegionSize(regionName);
                    TableSplit split = new TableSplit(table.getName(),
                            splitStart, splitStop, regionLocation, regionSize);
                    splits.add(split);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("getSplits: split -> " + i + " -> " + split);
                    }
                }
            }
        }

        for (TableSplit split : splits) {
            String startKey = Bytes.toString(split.getStartRow());
            String[] hosts = split.getLocations();
            result.put(startKey, hosts);
            LOG.info(startKey + " -> " + hosts[0]);
        }

        return result;
    }

  private String reverseDNS(InetAddress ipAddress) throws NamingException {
    String hostName = this.reverseDNSCacheMap.get(ipAddress);
    if (hostName == null) {
      hostName = Strings.domainNamePointerToHostName(
        DNS.reverseDns(ipAddress, this.nameServer));
      this.reverseDNSCacheMap.put(ipAddress, hostName);
    }
    return hostName;
  }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
