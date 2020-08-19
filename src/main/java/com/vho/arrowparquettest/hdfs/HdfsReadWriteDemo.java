package com.vho.arrowparquettest.hdfs;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

public class HdfsReadWriteDemo {
  private static final String HDFS_BASE_DIR = "/data/hdfs";
  private MiniDFSCluster hdfsCluster;

  public static void main(String[] args) throws IOException {
    HdfsReadWriteDemo app = new HdfsReadWriteDemo();
    app.initHdfs();
    app.writeToHdfs();
    app.shutdown();
  }

  private void shutdown() {
    hdfsCluster.shutdown(false);
  }

  private void writeToHdfs() throws IOException {
    try {
      Thread.sleep(100000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      e.printStackTrace();
    }

    try (FileSystem fs = hdfsCluster.getFileSystem()) {
      fs.open(new Path(""));
//      fs.copyFromLocalFile("people.arrow", "");
    }
  }

  private void readFromHdfs() throws IOException {
    try (FileSystem fs = hdfsCluster.getFileSystem(); RootAllocator allocator = new RootAllocator()) {
      FSDataInputStream fis =fs.open(new Path(""));
//      fs.copyFromLocalFile("people.arrow", "");
      FileStatus status = fs.getFileStatus(new Path("people.arrow"));

      SeekableByteChannel hadoopByteChannel = new HadoopSeekableByteChannel(status, fis);
      ArrowFileReader reader = new ArrowFileReader(hadoopByteChannel, allocator) ;
    }
  }

  private void initHdfs() throws IOException {
    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, HDFS_BASE_DIR);
    hdfsCluster = new MiniDFSCluster.Builder(conf).build();
  }
}
