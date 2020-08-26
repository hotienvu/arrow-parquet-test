package com.vho.arrowparquettest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.*;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ParquetColumnReaderDemo {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetColumnReaderDemo.class);

  private static void readParquet() throws IOException {
    Configuration conf = new HdfsConfiguration();
    InputFile inputFile = HadoopInputFile.fromPath(new Path("file:///home/vho/work/github/arrow-parquet-test/people.parquet"), conf);

    ParquetReadOptions options = ParquetReadOptions.builder().build();
    try (ParquetFileReader reader = new ParquetFileReader(inputFile, options)) {
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      PageReadStore store = reader.readNextRowGroup();
      int rowGroupCount = 0;
      while (store != null) {
        rowGroupCount++;
        final ColumnDescriptor colDescriptor = schema.getColumnDescription(new String[]{"firstName"});
        PageReader pageReader = store.getPageReader(colDescriptor);

//        ColumnReadStoreImpl columnReadStoreImpl = new ColumnReadStoreImpl(store, new ParquetGroupConverter(), schema, "");
//        ColumnReader columnReader = columnReadStoreImpl.getColumnReader(colDescriptor);
//        columnReader.getTotalValueCount();
        LOG.info("reading row groups no {} with {} rows", rowGroupCount, store.getRowCount());
        DataPage page = pageReader.readPage();
        int pageCount = 0;
        while (page != null) {
          pageCount++;
          LOG.info("reading page no {} with {} elements", pageCount, page.getValueCount());
          page.getValueCount();
          page.getValueCount();
          page.accept(new DataPage.Visitor<Integer>() {
            @Override
            public Integer visit(DataPageV1 dataPageV1) {
              return null;
            }

            @Override
            public Integer visit(DataPageV2 dataPageV2) {
              return null;
            }
          });
          page = pageReader.readPage();
        }
        store = reader.readNextRowGroup();
      }

    }
  }

  public static void main(String[] args) throws IOException {
    readParquet();
  }
}
