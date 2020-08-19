package com.vho.arrowparquettest;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ArrowReadWriteDemo {
  private static Logger LOG = LoggerFactory.getLogger(ArrowReadWriteDemo.class);


  private void writeToArrowFile(Person[] people, File outFile, final int numRecordsPerBatch) throws IOException {

    try (VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(Person.arrowSchema(), new RootAllocator());
         FileOutputStream os = new FileOutputStream(outFile);
         ArrowFileWriter writer = new ArrowFileWriter(schemaRoot, new DictionaryProvider.MapDictionaryProvider(), os.getChannel())) {
      LOG.info("start writing");
      writer.start();
      int batches = 0;
      while (batches * numRecordsPerBatch < people.length) {
        int from = batches * numRecordsPerBatch;
        int to = Math.min(from + numRecordsPerBatch, people.length);
        schemaRoot.allocateNew();

        LOG.info("Writing records from {} to {}", from, to - 1);
        for (int i = from; i < to; ++i) {
          writeRecord(schemaRoot, people[i], i - from);
        }

        schemaRoot.setRowCount(to - from);
        // write batch and clear schema root
        writer.writeBatch();
        schemaRoot.clear();
        LOG.info("Done writing {} records", to - from);
        batches++;
      }
      writer.end();
    }
  }

  private void writeRecord(VectorSchemaRoot schemaRoot, Person person, int idx) {
    ((VarCharVector) schemaRoot.getVector("firstName")).setSafe(idx, person.getFirstName().getBytes());
    ((VarCharVector) schemaRoot.getVector("lastName")).setSafe(idx, person.getLastName().getBytes());
    ((UInt4Vector) schemaRoot.getVector("age")).setSafe(idx, person.getAge());
    StructVector addressVector = (StructVector) schemaRoot.getVector("address");
    Address address = person.getAddress();
    ((VarCharVector) addressVector.getChild("street")).setSafe(idx, address.getStreet().getBytes());
    ((UInt4Vector) addressVector.getChild("streetNumber")).setSafe(idx, address.getStreetNumber());
    ((VarCharVector) addressVector.getChild("city")).setSafe(idx, address.getCity().getBytes());
    ((UInt4Vector) addressVector.getChild("postalCode")).setSafe(idx, address.getPostalCode());
  }

  private Person[] generateRandom(int numPeople) {
    Person[] res = new Person[numPeople];
    for (int i = 0; i < numPeople; ++i)
      res[i] = Person.randomPerson();
    return res;
  }

  private void testWriteToArrow() throws IOException {
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    final int numPeople = 1_123_456;
    LOG.info("Generating {} people", numPeople);
    Person[] people = generateRandom(numPeople);
    LOG.info("Time = {}", stopWatch);
    LOG.info("Writing to arrow file ");
    writeToArrowFile(people, new File("people.arrow"), 10_000);
    stopWatch.split();
    stopWatch.stop();
    LOG.info("Time = {}", stopWatch);
  }

  private void readFromArrowFile() throws IOException {
    try (FileInputStream os = new FileInputStream(new File("people.arrow"));
         BufferAllocator allocator = new RootAllocator();
         ArrowFileReader reader = new ArrowFileReader(new SeekableReadChannel(os.getChannel()), allocator)) {
      reader.initialize();
      readPersonRecords(reader);
    }
  }

  public static void readPersonRecords(ArrowFileReader reader) throws IOException {
    List<Person> people = new ArrayList<>();
    VectorSchemaRoot schemaRoot = reader.getVectorSchemaRoot();
    while (reader.loadNextBatch()) {
      LOG.info("reading {} records from batch", schemaRoot.getRowCount());
      VarCharVector firstNames = (VarCharVector) schemaRoot.getVector("firstName");
      VarCharVector lastNames = (VarCharVector) schemaRoot.getVector("lastName");
      UInt4Vector ages = (UInt4Vector) schemaRoot.getVector("age");
      StructVector addresses = (StructVector) schemaRoot.getVector("address");
      VarCharVector streets = (VarCharVector) addresses.getChild("street");
      UInt4Vector streetNumbers = (UInt4Vector) addresses.getChild("streetNumber");
      VarCharVector cities = (VarCharVector) addresses.getChild("city");
      UInt4Vector postalCodes = (UInt4Vector) addresses.getChild("postalCode");

      for (int i = 0; i < schemaRoot.getRowCount(); ++i) {
        Address address = new Address(new String(streets.get(i)), streetNumbers.get(i), new String(cities.get(i)),  postalCodes.get(i));
        Person person = new Person(new String(firstNames.get(i)), new String(lastNames.get(i)), ages.get(i), address);
        people.add(person);
      }
    }
    LOG.info("Done reading {} records ", people.size());
  }

  public static void main(String[] args) throws IOException {
    ArrowReadWriteDemo app = new ArrowReadWriteDemo();
    app.testWriteToArrow();
    app.readFromArrowFile();
  }

}
