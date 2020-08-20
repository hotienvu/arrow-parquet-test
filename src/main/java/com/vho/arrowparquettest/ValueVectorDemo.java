package com.vho.arrowparquettest;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.util.TransferPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValueVectorDemo {

  private static final Logger LOG = LoggerFactory.getLogger(ValueVectorDemo.class);

  private static void vectorLifeCycleDemo() {
    try (BufferAllocator allocator = new RootAllocator();
         IntVector intVector = new IntVector("ints", allocator)) {
      intVector.allocateNew();
      int numRecords = 10;
      for (int i = 0; i < numRecords; ++i) {
        intVector.set(i, i);
        // safe-assign a to out-of-bound index
        intVector.setSafe(100_000, i);
      }
      intVector.setValueCount(numRecords);

      LOG.info("vector size = {}", intVector.getValueCount());
      for (int i = 0; i < intVector.getValueCount(); ++i) {
        LOG.info("ints[{}] = {}", i, intVector.get(i));
      }
    }
  }

  public static void main(String[] args) {
    vectorLifeCycleDemo();
    vectorSlicingDemo();
  }

  private static void vectorSlicingDemo() {
    try (BufferAllocator allocator = new RootAllocator();
         IntVector intVector = new IntVector("ints", allocator)) {
      intVector.allocateNew();
      int numRecords = 10;
      for (int i = 0; i < numRecords; ++i)
        intVector.setSafe(i, i);
      intVector.setValueCount(numRecords);
      TransferPair transfer = intVector.getTransferPair(allocator);
      // shallow copy (zero-copy)
      transfer.splitAndTransfer(5, 5);
      IntVector sliced = (IntVector) transfer.getTo();
      LOG.info("sliced size = {} ", sliced.getValueCount());
      // changing original will also reflects copied
      intVector.set(6, 7);
      for (int i = 0; i < sliced.getValueCount(); ++i) {
        LOG.info("sliced[{}] = {}", i, sliced.get(i));
      }
      sliced.close();
    }
  }
}
