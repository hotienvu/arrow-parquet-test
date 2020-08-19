package com.vho.arrowparquettest.hdfs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class HadoopSeekableByteChannel implements SeekableByteChannel {

  private final FileStatus status;
  private final FSDataInputStream inputStream;
  private boolean closed;

  public HadoopSeekableByteChannel(FileStatus status, FSDataInputStream inputStream) {
    this.status = status;
    this.inputStream = inputStream;
    closed = false;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    checkIfChannelIsClosed();
    int read = 0;

    int oldPos = dst.position();
    if (dst.hasArray()) { // if FSDataInputStream if not ByteBufferReadable
      read = inputStream.read(dst.array(), dst.position() + dst.arrayOffset(), dst.remaining());
    } else {
      read = inputStream.read(dst);
    }
    dst.position(oldPos + read);
    return read;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    checkIfChannelIsClosed();
    throw new UnsupportedOperationException();
  }

  @Override
  public long position() throws IOException {
    checkIfChannelIsClosed();
    return inputStream.getPos();
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    checkIfChannelIsClosed();

    inputStream.seek(newPosition);
    return this;
  }

  @Override
  public long size() throws IOException {
    checkIfChannelIsClosed();
    return status.getLen();
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isOpen() {
    return !closed;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    inputStream.close();
  }

  private void checkIfChannelIsClosed() throws IOException {
    if (closed) {
      throw new IOException("Channel is closed");
    }
  }
}
