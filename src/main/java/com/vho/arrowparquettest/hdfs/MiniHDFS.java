package com.vho.arrowparquettest.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.IOException;

public class MiniHDFS {
  private final String baseDir;
  private MiniDFSCluster hdfs;

  public MiniHDFS(String baseDir) {
    this.baseDir = baseDir;
  }

  public void start() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir);
    conf.set("hadoop.security.authorization", "false");
    hdfs = new MiniDFSCluster.Builder(conf)
      .format(true)
      .build();
    hdfs.waitClusterUp();
  }

  public FileSystem getFileSystem() throws IOException {
    return hdfs.getFileSystem();
  }

  public Configuration getConfiguration() {
    return hdfs.getConfiguration(0);
  }
  public void shutdown() {
    hdfs.shutdown(true);
  }
}
