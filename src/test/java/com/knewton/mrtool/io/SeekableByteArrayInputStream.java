/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 */
package com.knewton.mrtool.io;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Helper input stream which composes a {@link ByteArrayInputStream} and implements {@link Seekable}
 * and {@link PositionedReadable} for testing the {@link JsonRecordReader}
 * 
 */
public class SeekableByteArrayInputStream extends InputStream implements Seekable,
        PositionedReadable {

    private ByteArrayInputStream inputStream;
    private long pos;

    public SeekableByteArrayInputStream(byte[] recommendationBytes) {
        this.inputStream = new ByteArrayInputStream(recommendationBytes);
        pos = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seek(long pos) throws IOException {
        inputStream.skip(pos);
        this.pos = pos;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getPos() throws IOException {
        return pos;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        throw new UnsupportedOperationException("seekToNewSource is not supported.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read() throws IOException {
        int val = inputStream.read();
        if (val > 0) {
            pos++;
        }
        return val;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        long currPos = pos;
        inputStream.skip(position);
        int bytesRead = inputStream.read(buffer, offset, length);
        inputStream.skip(currPos);
        return bytesRead;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        long currPos = pos;
        inputStream.skip(position);
        inputStream.read(buffer, offset, length);
        inputStream.skip(currPos);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        long currPos = pos;
        inputStream.skip(position);
        inputStream.read(buffer);
        inputStream.skip(currPos);
    }

}
