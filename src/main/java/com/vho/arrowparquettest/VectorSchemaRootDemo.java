package com.vho.arrowparquettest;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class VectorSchemaRootDemo {

  private static Logger LOG = LoggerFactory.getLogger(VectorSchemaRootDemo.class);

  private static void vectorSchemaRootCreationDemo() {
    try (BufferAllocator allocator = new RootAllocator()) {
      BitVector bitVector = new BitVector("boolean", allocator);
      VarCharVector varCharVector = new VarCharVector("varchar", allocator);
      final int numRecords = 10;
      for (int i = 0; i < numRecords; i++) {
        bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
        varCharVector.setSafe(i, ("test_" + i).getBytes(StandardCharsets.UTF_8));
      }
      bitVector.setValueCount(numRecords);
      varCharVector.setValueCount(numRecords);
      List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
      List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);

      LOG.info("bit vector count = {}, varchar vector count = {}", bitVector.getValueCount(), varCharVector.getValueCount());
      LOG.info("bit vector 0 = {}", bitVector.get(0));

      try (VectorSchemaRoot vsr = new VectorSchemaRoot(fields, vectors)) {
        LOG.info("vector schema root row count = {}", vsr.getRowCount());
        for (int i = 0; i < vsr.getRowCount(); ++i) {
          int b = ((BitVector) vsr.getVector("boolean")).get(i);
          String vc = new String(((VarCharVector) vsr.getVector("varchar")).get(i));
          LOG.info("record {}th = {}, {}", i, b, vc);
        }
      }
      LOG.info("after release, bit vector count = {}, varchar vector count = {}", bitVector.getValueCount(), varCharVector.getValueCount());
    }
  }

  private static void loadUnloadRecordBatchDemo() {
    try (BufferAllocator allocator = new RootAllocator();
         BitVector bitVector = new BitVector("boolean", allocator);
         VarCharVector varCharVector = new VarCharVector("varchar", allocator)) {

      final int numRecords = 10;
      for (int i = 0; i < numRecords; i++) {
        bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
        varCharVector.setSafe(i, ("test_" + i).getBytes(StandardCharsets.UTF_8));
      }
      bitVector.setValueCount(numRecords);
      varCharVector.setValueCount(numRecords);
      List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
      List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);

      VectorSchemaRoot root1 = new VectorSchemaRoot(fields, vectors);
      VectorUnloader unloader = new VectorUnloader(root1);
      ArrowRecordBatch recordBatch = unloader.getRecordBatch();
      LOG.info("root1 size = {}", root1.getRowCount());
      LOG.info("record batch size = {}", recordBatch.getLength());

      // create a VectorSchemaRoot root2 and load the recordBatch
      VectorSchemaRoot root2 = VectorSchemaRoot.create(root1.getSchema(), allocator);
      VectorLoader loader = new VectorLoader(root2);
      loader.load(recordBatch);
      LOG.info("root2 size = {}", root2.getRowCount());
      root1.clear();
      root2.clear();
    }
  }

  public static void main(String[] args) {
    vectorSchemaRootCreationDemo();
    loadUnloadRecordBatchDemo();
  }


}
