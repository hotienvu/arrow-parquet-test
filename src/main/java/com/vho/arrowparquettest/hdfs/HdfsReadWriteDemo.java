package com.vho.arrowparquettest.hdfs;

import com.vho.arrowparquettest.ArrowReadWriteDemo;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

public class HdfsReadWriteDemo {
  private static final String HDFS_BASE_DIR = "/data/hdfs";
  private static final String ARROW_FILE_HDFS_PATH = "/people.arrow";
  private MiniDFSCluster hdfsCluster;

  private static final Logger LOG = LoggerFactory.getLogger(HdfsReadWriteDemo.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    HdfsReadWriteDemo app = new HdfsReadWriteDemo();
    app.initHdfs();
    app.writeToHdfs();
    Thread.sleep(1000);
    app.readFromHdfs();
    app.shutdown();
  }

  private void shutdown() {
    hdfsCluster.shutdown(false);
  }

  private void writeToHdfs() throws IOException {
    try (FileSystem fs = hdfsCluster.getFileSystem()) {
      fs.copyFromLocalFile(new Path("people.arrow"), new Path(ARROW_FILE_HDFS_PATH));

      for (FileStatus status: fs.listStatus(new Path("/"))) {
        LOG.info("[DEBUG] file = {}", status);
      }
      fs.copyToLocalFile(new Path(ARROW_FILE_HDFS_PATH), new Path("people_hdfs.arrow"));
    }
  }

  private void readFromHdfs() throws IOException {
    AllocationListener allocationListener = new AllocationListener() {
      @Override
      public void onPreAllocation(long size) {
        LOG.info("Pre allocation. size = {}", size);

      }

      @Override
      public void onAllocation(long size) {
        LOG.info("ON allocation. size = {}", size);
      }

      @Override
      public void onRelease(long size) {
        LOG.info("ON Release. size = {}", size);
      }

      @Override
      public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
        LOG.info("ON failed allocation. size = {}. Outcome = {}", size, outcome);
        return false;
      }

      @Override
      public void onChildAdded(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
        LOG.info("One child added. parent = {}, child = {}", parentAllocator, childAllocator);
      }

      @Override
      public void onChildRemoved(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
        LOG.info("One child removed. parent = {}, child = {}", parentAllocator, childAllocator);
      }
    };
    try (FileSystem fs = hdfsCluster.getFileSystem();
         BufferAllocator allocator = new RootAllocator(allocationListener, Long.MAX_VALUE);
         FSDataInputStream fis =fs.open(new Path(ARROW_FILE_HDFS_PATH))
    ) {
      FileStatus status = fs.getFileStatus(new Path(ARROW_FILE_HDFS_PATH));
      SeekableByteChannel hadoopByteChannel = new HadoopSeekableByteChannel(status, fis);
      ArrowFileReader reader = new ArrowFileReader(hadoopByteChannel, allocator) ;
      ArrowReadWriteDemo.readPersonRecords(reader);
    }
  }

  private void initHdfs() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, HDFS_BASE_DIR);
    conf.set("hadoop.security.authorization", "false");
    hdfsCluster = new MiniDFSCluster.Builder(conf)
      .format(true)
      .build();
  }
}
