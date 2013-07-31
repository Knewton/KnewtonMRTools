/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 */
package com.knewton.mrtool.io;

import com.knewton.mrtool.DummyJsonRecommendations;

import mockit.Mock;
import mockit.MockUp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;

public class JsonRecordReaderTest {

    private byte[] recommendationBytes;

    /**
     * Setup the input stream. Don't read from file. Just read from a memory buffer
     * 
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BufferedOutputStream buffOS = new BufferedOutputStream(baos);
        for (String jsonRec : DummyJsonRecommendations.jsonRecommendations) {
            buffOS.write(jsonRec.getBytes());
            buffOS.write("\n".getBytes());
        }
        buffOS.close();
        baos.close();
        recommendationBytes = baos.toByteArray();
    }

    /**
     * Tests if a JsonRecordReader can be initialized properly without errors.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testInitializeJsonRecordReader()
            throws IOException, InterruptedException {
        JsonRecordReader<Text> rr = new JsonRecordReader<Text>() {
            @Override
            protected Class<?> getDataClass(String jsonStr) {
                return Text.class;
            }
        };

        Configuration conf = new Configuration();
        TaskAttemptContext context =
                new TaskAttemptContext(conf, new TaskAttemptID());
        FileSplit fileSplit = new FileSplit(
                new Path("recs.2013-03-20_02_52.log"), 0,
                recommendationBytes.length, new String[0]);

        new MockUp<FileSystem>() {
            @Mock
            public FSDataInputStream open(Path f) throws IOException {
                return new FSDataInputStream(new SeekableByteArrayInputStream(
                        recommendationBytes));
            }
        };

        rr.initialize(fileSplit, context);
        assertEquals(Text.class, rr.getDataClass(null));
        rr.close();
    }

    /**
     * Tests the line reader in the record reader to see if records can be read correctly from the
     * beginning of an input stream.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testJsonRecordReader()
            throws IOException, InterruptedException {
        JsonRecordReader<Text> rr = new JsonRecordReader<Text>() {
            @Override
            protected Class<?> getDataClass(String jsonStr) {
                return Text.class;
            }
        };

        Configuration conf = new Configuration();
        TaskAttemptContext context =
                new TaskAttemptContext(conf, new TaskAttemptID());
        FileSplit fileSplit = new FileSplit(
                new Path("recs.2013-03-20_02_52.log"), 0,
                recommendationBytes.length, new String[0]);

        new MockUp<FileSystem>() {
            @Mock
            public FSDataInputStream open(Path f) throws IOException {
                return new FSDataInputStream(new SeekableByteArrayInputStream(
                        recommendationBytes));
            }
        };
        // Initialize it to get the compression codecs
        rr.initialize(fileSplit, context);
        // close the line reader and reopen it.
        rr.close();
        LineReader lineReader = rr.initLineReader(fileSplit, conf);
        Text line = new Text();
        lineReader.readLine(line);
        assertEquals(DummyJsonRecommendations.jsonRecommendations[0],
                line.toString());

        line = new Text();
        lineReader.readLine(line);
        assertEquals(DummyJsonRecommendations.jsonRecommendations[1],
                line.toString());
        lineReader.close();
    }

    /**
     * Tests the line reader in the record reader to see if records can be read correctly from a
     * random seek location in the input stream.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testJsonRecordReaderWithRandomPos()
            throws IOException, InterruptedException {
        JsonRecordReader<Text> rr = new JsonRecordReader<Text>() {
            @Override
            protected Class<?> getDataClass(String jsonStr) {
                return Text.class;
            }
        };

        Configuration conf = new Configuration();
        TaskAttemptContext context =
                new TaskAttemptContext(conf, new TaskAttemptID());
        FileSplit fileSplit = new FileSplit(
                new Path("recs.2013-03-20_02_52.log"), 10,
                recommendationBytes.length, new String[0]);

        new MockUp<FileSystem>() {
            @Mock
            public FSDataInputStream open(Path f) throws IOException {
                return new FSDataInputStream(new SeekableByteArrayInputStream(
                        recommendationBytes));
            }
        };
        // Initialize it to get the compression codecs
        rr.initialize(fileSplit, context);
        // close the line reader and reopen it.
        rr.close();
        LineReader lineReader = rr.initLineReader(fileSplit, conf);
        Text line = new Text();
        lineReader.readLine(line);
        assertEquals(DummyJsonRecommendations.jsonRecommendations[1],
                line.toString());
        line = new Text();
        lineReader.readLine(line);
        assertTrue(line.toString().isEmpty());
        lineReader.close();
    }

}
