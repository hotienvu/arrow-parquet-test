package com.vho.arrowparquettest;

import com.vho.arrowparquettest.hdfs.MiniHDFS;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetWriteDemo {
  private static final int NUM_PEOPLE = 1_000_000;
  private MiniHDFS hdfs;
  private static final Logger LOG = LoggerFactory.getLogger(ParquetWriteDemo.class);

  private Person[] generateRandom(int numPeople) {
    Person[] res = new Person[numPeople];
    for (int i = 0; i < numPeople; i++) {
      res[i] = Person.randomPerson();
    }
    return res;
  }

  private void writeToParquet(Person[] people) throws IOException {
    long start = System.currentTimeMillis();
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
      .<GenericRecord>builder(new Path("people.parquet"))
      .withSchema(Person.getAvroSchema())
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .build()) {

      GenericRecord[] records = new GenericRecord[people.length];
      for (int i=0;i<records.length;++i)
        records[i] = people[i].toGenericRecord();

      int count = 0;
      for (GenericRecord rec: records) {
        if (++count % 100_000 == 0)
          LOG.info("Done writing: {} records", count);
        writer.write(rec);
      }
    }
    LOG.info("Elapsed = {} ms", System.currentTimeMillis() - start);
  }

  private void readParquet() throws IOException {
    final Configuration conf = hdfs.getConfiguration();
    final InputFile inputFile = HadoopInputFile.fromPath(new Path("/people2.parquet"), conf);
    ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile)
      .withConf(conf)
      .build();

    GenericRecord record = reader.read();
    List<GenericRecord> res = new ArrayList<>();
    while (record != null) {
      res.add(record);
      record = reader.read();
    }
    LOG.info("Done reading {} records", res.size());
  }


  private void shutdown() {
    hdfs.shutdown();
  }

  private void init() throws IOException {
    hdfs = new MiniHDFS("/data/hdfs");
    hdfs.start();
  }

  public static void main(String[] args) throws IOException {
    ParquetWriteDemo app = new ParquetWriteDemo();
    app.init();
//    Person[] people = app.generateRandom(NUM_PEOPLE);
//    app.writeToParquet(people);
    app.hdfs.getFileSystem().copyFromLocalFile(new Path("people.parquet"), new Path("/people2.parquet"));
    app.readParquet();
    app.shutdown();
  }

}
