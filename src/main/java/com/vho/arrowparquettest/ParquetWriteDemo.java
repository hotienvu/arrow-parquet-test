package com.vho.arrowparquettest;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;

import static org.apache.parquet.avro.AvroParquetWriter.builder;

public class ParquetWriteDemo {
  private static final int NUM_PEOPLE = 1_000_000;

  public static void main(String[] args) throws IOException {
    ParquetWriteDemo app = new ParquetWriteDemo(); Person[] people = app.generateRandom(NUM_PEOPLE);
    app.writeToParquet(people);
  }

  private Person[] generateRandom(int numPeople) {
    Person[] res = new Person[numPeople];
    for (int i = 0; i < numPeople; i++) {
      res[i] = Person.randomPerson();
    }
    return res;
  }

  private void writeToParquet(Person[] people) throws IOException {
//    MessageType schema = Person.parquetSchema();
//    System.out.println(schema.toString());
//    long rowGroupSize = 1000;
//    int maxPaddingSize = 10;
//    ParquetFileWriter writer = new ParquetFileWriter(file, schema,
//      ParquetFileWriter.Mode.OVERWRITE, rowGroupSize, maxPaddingSize);
//
//    writer.start();
//    writer.startBlock(1);
//    ColumnDescriptor firstNameCol = schema.getColumnDescription(new String[]{"firstName"});
//    writer.startColumn(firstNameCol, 1, CompressionCodecName.SNAPPY);
//    writer.writeDataPage(1, );
//    writer.endColumn();
//    writer.end(Collections.emptyMap());

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
          System.out.println("Done writing: " + count);
        writer.write(rec);
      }
    }
    System.out.println("Elapsed = " + (System.currentTimeMillis() - start) + " ms.");
  }
}
