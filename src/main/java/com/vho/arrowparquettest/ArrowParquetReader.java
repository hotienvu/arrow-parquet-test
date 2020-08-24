package com.vho.arrowparquettest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;

import java.io.IOException;

public class ArrowParquetReader<T> extends ParquetReader<T> {

  /**
   * @param file a file path
   * @param <T>  the Java type of records to read from the file
   * @return an Avro reader builder
   * @deprecated will be removed in 2.0.0; use {@link #builder(InputFile)} instead.
   */
  @Deprecated
  public static <T> ArrowParquetReader.Builder<T> builder(Path file) {
    return new ArrowParquetReader.Builder<T>(file);
  }

  public static <T> ArrowParquetReader.Builder<T> builder(InputFile file) {
    return new ArrowParquetReader.Builder<T>(file);
  }

  /**
   * @param file a file path
   * @throws IOException if there is an error while reading
   * @deprecated will be removed in 2.0.0; use {@link #builder(InputFile)} instead.
   */
  @Deprecated
  public ArrowParquetReader(Path file) throws IOException {
    super(file, new AvroReadSupport<T>());
  }

  /**
   * @param file                a file path
   * @param unboundRecordFilter an unbound record filter (from the old filter API)
   * @throws IOException if there is an error while reading
   * @deprecated will be removed in 2.0.0; use {@link #builder(InputFile)} instead.
   */
  @Deprecated
  public ArrowParquetReader(Path file, UnboundRecordFilter unboundRecordFilter) throws IOException {
    super(file, new AvroReadSupport<T>(), unboundRecordFilter);
  }

  /**
   * @param conf a configuration
   * @param file a file path
   * @throws IOException if there is an error while reading
   * @deprecated will be removed in 2.0.0; use {@link #builder(InputFile)} instead.
   */
  @Deprecated
  public ArrowParquetReader(Configuration conf, Path file) throws IOException {
    super(conf, file, new AvroReadSupport<T>());
  }

  /**
   * @param conf                a configuration
   * @param file                a file path
   * @param unboundRecordFilter an unbound record filter (from the old filter API)
   * @throws IOException if there is an error while reading
   * @deprecated will be removed in 2.0.0; use {@link #builder(InputFile)} instead.
   */
  @Deprecated
  public ArrowParquetReader(Configuration conf, Path file, UnboundRecordFilter unboundRecordFilter) throws IOException {
    super(conf, file, new AvroReadSupport<T>(), unboundRecordFilter);
  }


  public static class Builder<T> extends ParquetReader.Builder<T> {

    @Deprecated
    private Builder(Path path) {
      super(path);
    }

    private Builder(InputFile file) {
      super(file);
    }

    @Override
    protected ReadSupport<T> getReadSupport() {
      return super.getReadSupport();
    }
  }
}
