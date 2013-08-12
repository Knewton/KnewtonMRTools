/**
 * Copyright 2013 Knewton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 * 
 */
package com.knewton.mrtool.io;

import com.knewton.mrtool.util.ObjectDecorator;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

/**
 * JSON record reader that reads files containing json objects and returns back a deserialized
 * instantiated object of type V. The json deserialization happens with gson. Note that this record
 * reader doesn't yet take advantage of splittable compressed codecs.
 * 
 * @author Giannis Neokleous
 * 
 * @param <V>
 */
// TODO: Add support for splittable compressed codecs.
public abstract class JsonRecordReader<V extends Writable>
        extends RecordReader<LongWritable, V> {

    private LongWritable key;
    private V value;
    private long start;
    private long end;
    private CompressionCodecFactory compressionCodecs;
    private LineReader in;
    private long pos;
    private Gson gson;
    private GsonBuilder gsonBuilder;
    private List<ObjectDecorator<String>> decorators;
    private Seekable seekableIn;

    public static final String APPEND_FILENAME_TO_JSON =
            "com.knewton.mapreduce.jsonrecordreader.append_filename";

    public JsonRecordReader() {
        gsonBuilder = new GsonBuilder();
        decorators = Lists.newArrayList();
        gson = gsonBuilder.create();
    }

    /**
     * Should be called before any key or value is read to setup any initialization actions. If you
     * want to inject the name of the file to the json record set the
     * {@link JsonRecordReader.APPEND_FILENAME_TO_JSON} property to true.
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        FileSplit fileSplit = (FileSplit) split;
        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        compressionCodecs = new CompressionCodecFactory(conf);

        in = initLineReader(fileSplit, conf);

        pos = start;
    }

    /**
     * Get the line reader to be used for the file. A <code>LineReader</code> can read a file line
     * by line. This separate method helps with testing too.
     * 
     * @param fileSplit
     * @param conf
     * @return
     * @throws IOException
     */
    protected LineReader initLineReader(FileSplit fileSplit, Configuration conf)
            throws IOException {
        final Path file = fileSplit.getPath();
        final CompressionCodec codec = compressionCodecs.getCodec(file);
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(fileSplit.getPath());
        seekableIn = fileIn;
        boolean skipFirstLine = false;
        LineReader lineReader;
        if (codec != null) {
            lineReader = new LineReader(codec.createInputStream(fileIn), conf);
        } else {
            // if the start is not the beginning of the file then skip the first line to get the
            // next complete json record. The previous json record will be read by the record reader
            // that got assigned the previous InputSplit.
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            lineReader = new LineReader(fileIn, conf);
        }
        if (skipFirstLine) {
            start += lineReader.readLine(new Text(), 0,
                    (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
        return lineReader;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // This is here in case nextKeyValue() gets called again after the record reader reached the
        // end of the split and doesn't have any more records to return. It avoids a null pointer
        // exception.
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);

        Text jsonText = new Text();
        int newSize = 0;
        if (getFilePosition() <= end) {
            newSize = in.readLine(jsonText);
            if (newSize > 0 && !jsonText.toString().isEmpty()) {
                for (ObjectDecorator<String> decorator : decorators) {
                    jsonText = new Text(
                            decorator.decorateObject(jsonText.toString()));
                }
                // This helps with avoiding to supress warnings for the entire method.
                @SuppressWarnings("unchecked")
                V tempValue = (V) gson.fromJson(jsonText.toString(),
                        getDataClass(jsonText.toString()));
                value = tempValue;
            }
            pos += newSize;
        }
        if (newSize == 0 || jsonText.toString().isEmpty()) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.max(0.0f,
                    Math.min(1.0f, (getFilePosition() - start) / (float) (end - start)));
        }
    }

    /**
     * Returns the position in the file taking into account compressed files.
     * 
     * @return
     * @throws IOException
     */
    private long getFilePosition() throws IOException {
        return seekableIn.getPos();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    /**
     * 
     * @return The compression factory to be used when reading files.
     */
    public CompressionCodecFactory getCompressionCodecFactory() {
        return compressionCodecs;
    }

    /**
     * Adds a decorator for this record reader. Decorators modify each record read. To define your
     * own decorator you can inherit from {@link ObjectDecorator}
     * 
     * @param decorator
     */
    public void addDecorator(ObjectDecorator<String> decorator) {
        decorators.add(decorator);
    }

    /**
     * Needs to be implemented by the subclass to return the type of object that needs to be
     * instantiated
     * 
     * @param jsonStr
     *            Useful if information about the type of the object is contained inside the JSON
     *            string.
     * @return Class to be instantiated
     */
    protected abstract Class<?> getDataClass(String jsonStr);

    /**
     * Registers a deserializer for deserializing a custom object from a Json string
     * 
     * @param type
     * @param jsonDes
     */
    public void registerDeserializer(Type type, JsonDeserializer<?> jsonDes) {
        gsonBuilder.registerTypeAdapter(type, jsonDes);
    }

}
