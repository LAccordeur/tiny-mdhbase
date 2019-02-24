/*
 * Copyright 2012 Shoji Nishimura
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package tiny.mdhbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.io.Closeables;

/**
 * Index
 * 
 * Index maintains partitioned spaces. When the number of points in a sub-space
 * exceeds a split threshold, the index halves the sub-space and allocates two
 * new buckets for the partitioned sub-spaces.
 * 
 * Schema:
 * <ul>
 * <li>row key: min key of a bucket
 * <li>column family: info
 * <ul>
 * <li>column: pl, common prefix length of points in a bucket
 * <li>column: bs, size of a bucket/number of points in a bucket
 * </ul>
 * </ul>
 * 
 * Bucket name, which is named after the common prefix naming scheme is
 * represented as a pair of binary value and its prefix length. For example.
 * [011*****] is represented as a pair of [01100000] and 3.
 * 
 * 
 * @author shoji
 * 
 */
public class Index implements Closeable {
  public static final byte[] FAMILY_INFO = "info".getBytes();

  public static final byte[] COLUMN_PREFIX_LENGTH = "pl".getBytes();

  public static final byte[] COLUMN_BUCKET_SIZE = "bs".getBytes();

  //标识该区域的是拥有直属子区域：0标识拥有，1标识未拥有，叶子节点的区域为拥有
  public static final byte[] COLUMN_SUB_REGION_IDENTIFIER = "sri".getBytes();

  private final int splitThreshold;

  private final HTable dataTable;

  private final HTable indexTable;

  private final HBaseAdmin admin;

  public Index(Configuration config, String tableName, int splitThreshold)
      throws IOException {
    this.admin = new HBaseAdmin(config);
    if (!admin.tableExists(tableName)) {
      HTableDescriptor tdesc = new HTableDescriptor(tableName);
      HColumnDescriptor cdesc = new HColumnDescriptor(Bucket.FAMILY);
      tdesc.addFamily(cdesc);
      admin.createTable(tdesc);
    }
    dataTable = new HTable(config, tableName);

    String indexName = tableName + "_index";
    if (!admin.tableExists(indexName)) {
      HTableDescriptor tdesc = new HTableDescriptor(indexName);
      HColumnDescriptor cdesc = new HColumnDescriptor(Index.FAMILY_INFO);
      tdesc.addFamily(cdesc);
      admin.createTable(tdesc);

      indexTable = new HTable(config, indexName);
      Put put = new Put(Utils.bitwiseZip(0, 0));
      put.add(FAMILY_INFO, COLUMN_PREFIX_LENGTH, Bytes.toBytes(2));
      put.add(FAMILY_INFO, COLUMN_BUCKET_SIZE, Bytes.toBytes(0L));
      put.add(FAMILY_INFO, COLUMN_SUB_REGION_IDENTIFIER, Bytes.toBytes(0));
      indexTable.put(put);
    } else {
      indexTable = new HTable(config, indexName);
    }

    this.splitThreshold = splitThreshold;
  }

  /**
   * fetches a bucket which holds the queried row.
   * 
   * @param row
   *          a queried row key
   * @return a bucket which holds the queried row.
   * @throws IOException
   */
  public Bucket fetchBucket(byte[] row) throws IOException {
    Result bucketEntry = indexTable.getRowOrBefore(row, FAMILY_INFO);
    byte[] bucketKey = bucketEntry.getRow();
    int prefixLength = Bytes.toInt(bucketEntry.getValue(FAMILY_INFO,
        COLUMN_PREFIX_LENGTH));
    Range[] ranges = toRanges(bucketKey, prefixLength);
    return createBucket(ranges);
  }

  private Range[] toRanges(byte[] bucketKey, int prefixLength) {
    byte[] suffix_ones = Utils.not(Utils.makeMask(prefixLength));
    // substitute don't cares to 0s. ex. [010*****] -> [01000000]
    int[] mins = Utils.bitwiseUnzip(bucketKey);
    // substitute don't cares to 1s. ex. [010*****] -> [01011111]
    int[] maxs = Utils.bitwiseUnzip(Utils.or(bucketKey, suffix_ones));
    Range[] ranges = new Range[2];
    ranges[0] = new Range(mins[0], maxs[0]);
    ranges[1] = new Range(mins[1], maxs[1]);
    return ranges;
  }

  /**
   * finds buckets which intersect with the query region.
   * 
   * @param rx
   * @param ry
   * @return
   * @throws IOException
   */
  public Iterable<Bucket> findBucketsInRange(Range rx, Range ry)
      throws IOException {
    byte[] probeKey = Utils.bitwiseZip(rx.min, ry.min);
    Result bucketEntry = indexTable.getRowOrBefore(probeKey, FAMILY_INFO);
    byte[] startKey = bucketEntry.getRow();
    byte[] stopKey = Bytes.incrementBytes(Utils.bitwiseZip(rx.max, ry.max), 1L);
    Scan scan = new Scan(startKey, stopKey);
    scan.addFamily(FAMILY_INFO);
    scan.setCaching(1000);
    ResultScanner results = indexTable.getScanner(scan);
    List<Bucket> hitBuckets = new LinkedList<Bucket>();
    for (Result result : results) {
      byte[] row = result.getRow();
      int pl = Bytes.toInt(result.getValue(FAMILY_INFO, COLUMN_PREFIX_LENGTH));
      Range[] rs = toRanges(row, pl);
      if (rx.intersect(rs[0]) && ry.intersect(rs[1])) {
        hitBuckets.add(createBucket(rs));
      }
    }
    return hitBuckets;
  }

  private Bucket createBucket(Range[] rs) {
    return new Bucket(dataTable, rs[0], rs[1], this);
  }

  /**
   * 
   * @param row
   * @throws IOException
   */
  void notifyInsertion(byte[] row) throws IOException {
    Result bucketEntry = indexTable.getRowOrBefore(row, FAMILY_INFO);
    byte[] bucketKey = bucketEntry.getRow();
    long size = indexTable.incrementColumnValue(bucketKey, FAMILY_INFO,
        COLUMN_BUCKET_SIZE, 1L);
    maySplit(bucketKey, size);
  }

  private void maySplit(byte[] bucketKey, long size) throws IOException {
    if (size > splitThreshold) {
      splitBucket(bucketKey);
    }
  }

  /*
   * bucket [abc*****] is partitioned into bucket [abc0****] and bucket
   * [abc1****].
   */
  private void splitBucket(byte[] splitKey) throws IOException {
      //1.获取带分裂bucket的属性信息
      Result regionEntry = indexTable.getRowOrBefore(splitKey, FAMILY_INFO);
      byte[] regionKey = regionEntry.getRow();
      int prefixLength = Bytes.toInt(regionEntry.getValue(FAMILY_INFO, COLUMN_PREFIX_LENGTH));
      int subRegionIdentifier = Bytes.toInt(regionEntry.getValue(FAMILY_INFO, COLUMN_SUB_REGION_IDENTIFIER));

      //2.统计待分裂区域中数据点的最大相似前缀
      PointDistribution pointDistribution = null;

      if (subRegionIdentifier == 0) {
          //说明这个区域内只含有其本身
          List<Result> results = scanDataRegion(dataTable, regionKey, prefixLength);
          List<Point> pointList = new ArrayList<Point>();
          for (Result result : results) {
              Point point = transformResultToPoint(result);
              pointList.add(point);
          }
          pointDistribution = Utils.calculatePointDistribution(pointList, prefixLength);

      } else {
          //说明这个区域内还还有非直属子区域，统计时需要排除这些子区域
          List<Result> results = scanIndexRegion(indexTable, regionKey, prefixLength);
          List<Range[]> regionList = new ArrayList<Range[]>();
          for (Result result : results) {
              byte[] rowKey = result.getRow();
              int pl = Bytes.toInt(result.getValue(FAMILY_INFO, COLUMN_PREFIX_LENGTH));

              Range[] subRegionRange = toRanges(rowKey, pl);
              regionList.add(subRegionRange);
          }

          List<Result> dataResults = scanDataRegion(dataTable, regionKey, prefixLength);
          List<Point> pointList = new ArrayList<Point>();
          for (Result result : dataResults) {
              Point point = transformResultToPoint(result);
              boolean checkResult = checkRange(point, regionList);
              if (!checkResult) {
                  pointList.add(point);
              }
          }

          pointDistribution = Utils.calculatePointDistribution(pointList, prefixLength);

      }

      //3.更新
      // 如果由最大相似前缀得到的子区域为父区域的直属子区域，说明父区域以及划分完毕，不必再保留父区域，更新生成的新区域并检查生成的子区域是否为自包含的
      // 否则需要保留并更新父区域的size值
      byte[] commonKey = pointDistribution.getKey();
      int commonPrefixLength = pointDistribution.getPrefixLength();
      if (commonPrefixLength > 32 * 2) {
          return; // exceeds the maximum prefix length.
      }
      byte[] newChildKeyA = commonKey;
      byte[] newChildKeyB = Bytes.incrementBytes(commonKey, 1L);

      //检查生成的子区域是否是父区域的直属子区域
      List<Put> putList = new ArrayList<Put>();
      if (prefixLength + 1 == commonPrefixLength) {
          Put put0 = new Put(newChildKeyA);
          put0.add(FAMILY_INFO, COLUMN_PREFIX_LENGTH, Bytes.toBytes(commonPrefixLength + 1));
          put0.add(FAMILY_INFO, COLUMN_BUCKET_SIZE, Bytes.toBytes(pointDistribution.getChildSizeA()));
          //put0.add(FAMILY_INFO, COLUMN_SUB_REGION_IDENTIFIER, Bytes.toBytes(0)); //是否为自包含待确定
          putList.add(put0);
      } else {
          Put put0 = new Put(regionKey);
          put0.add(FAMILY_INFO, COLUMN_PREFIX_LENGTH, Bytes.toBytes(prefixLength));
          put0.add(FAMILY_INFO, COLUMN_BUCKET_SIZE, Bytes.toBytes(0));
          put0.add(FAMILY_INFO, COLUMN_SUB_REGION_IDENTIFIER, Bytes.toBytes(1));
          putList.add(put0);
      }

      //检查子区域是否为自包含的
      List<Result> childRegionResultA = scanIndexRegion(indexTable, newChildKeyA, commonPrefixLength + 1);
      if (prefixLength + 1 == commonPrefixLength) {
          if (childRegionResultA.size() == 0) {
              putList.get(0).add(FAMILY_INFO, COLUMN_SUB_REGION_IDENTIFIER, Bytes.toBytes(0));
          } else {
              putList.get(0).add(FAMILY_INFO, COLUMN_SUB_REGION_IDENTIFIER, Bytes.toBytes(1));
          }
      } else {
          Put put1 = new Put(newChildKeyA);
          put1.add(FAMILY_INFO, COLUMN_PREFIX_LENGTH, Bytes.toBytes(commonPrefixLength + 1));
          put1.add(FAMILY_INFO, COLUMN_BUCKET_SIZE, Bytes.toBytes(pointDistribution.getChildSizeA()));
          if (childRegionResultA.size() == 0) {
              put1.add(FAMILY_INFO, COLUMN_SUB_REGION_IDENTIFIER, Bytes.toBytes(0));
          } else {
              put1.add(FAMILY_INFO, COLUMN_SUB_REGION_IDENTIFIER, Bytes.toBytes(1));
          }
          putList.add(put1);
      }

      List<Result> childRegionResultB = scanIndexRegion(indexTable, newChildKeyB, commonPrefixLength + 1);
      Put put2 = new Put(newChildKeyB);
      put2.add(FAMILY_INFO, COLUMN_PREFIX_LENGTH, Bytes.toBytes(commonPrefixLength + 1));
      put2.add(FAMILY_INFO, COLUMN_BUCKET_SIZE, Bytes.toBytes(pointDistribution.getChildSizeB()));
      if (childRegionResultB.size() == 0) {
          put2.add(FAMILY_INFO, COLUMN_SUB_REGION_IDENTIFIER, Bytes.toBytes(0));
      } else {
          put2.add(FAMILY_INFO, COLUMN_SUB_REGION_IDENTIFIER, Bytes.toBytes(1));
      }
      putList.add(put2);

      indexTable.put(putList);

  }

  private boolean checkNextBit(byte[] parentKey, byte[] childKey, int prefixLength) {
      boolean result = false;

      return result;
  }

  private boolean checkRange(Point point, List<Range[]> rangeList) {
      boolean result = false;

      for (Range[] range : rangeList) {
          Range rangeX = range[0];
          Range rangeY = range[1];

          if (rangeX.include(point.x) && rangeY.include(point.y)) {
              return true;
          }

      }

      return result;
  }

  private List<Result> scanDataRegion(HTable hTable, byte[] regionKey, int prefixLength) throws IOException {
      List<Result> resultList = new ArrayList<Result>();

      Range[] regionRange = toRanges(regionKey, prefixLength);
      Range rx = regionRange[0];
      Range ry = regionRange[1];
      byte[] startRow = Utils.bitwiseZip(rx.min, ry.min);
      byte[] stopRow = Bytes.incrementBytes(Utils.bitwiseZip(rx.max, ry.max), 1L);

      Scan scan = new Scan(startRow, stopRow);
      scan.addFamily(Bucket.FAMILY);
      scan.setCaching(1000);
      ResultScanner results = hTable.getScanner(scan);

      for (Result result : results) {
          resultList.add(result);
      }

      return resultList;
  }

    private List<Result> scanIndexRegion(HTable hTable, byte[] regionKey, int prefixLength) throws IOException {
        List<Result> resultList = new ArrayList<Result>();

        Range[] regionRange = toRanges(regionKey, prefixLength);
        Range rx = regionRange[0];
        Range ry = regionRange[1];
        byte[] startRow = Utils.bitwiseZip(rx.min, ry.min);
        byte[] stopRow = Bytes.incrementBytes(Utils.bitwiseZip(rx.max, ry.max), 1L);

        Scan scan = new Scan(startRow, stopRow);
        scan.addFamily(FAMILY_INFO);
        scan.setCaching(1000);
        ResultScanner results = hTable.getScanner(scan);

        for (Result result : results) {
            resultList.add(result);
        }

        return resultList;
    }


    private void transformResultAndAddToList(Result result, List<Point> found) {
        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bucket.FAMILY);
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            Point p = toPoint(entry.getKey(), entry.getValue());
            found.add(p);
        }


    }

    private Point transformResultToPoint(Result result) {
        byte[] rowKey = result.getRow();
        int[] coordination = Utils.bitwiseUnzip(rowKey);
        Point point = new Point(coordination[0], coordination[1]);
        return point;
    }

    private Point toPoint(byte[] qualifier, byte[] value) {
        long id = Bytes.toLong(qualifier);
        int x = Bytes.toInt(value, 0);
        int y = Bytes.toInt(value, 4);
        return new Point(id, x, y);
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    Closeables.closeQuietly(dataTable);
    Closeables.closeQuietly(indexTable);
  }
}
