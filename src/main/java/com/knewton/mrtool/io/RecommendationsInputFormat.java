/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 */
package com.knewton.mrtool.io;

import com.knewton.mrtool.model.RecommendationWritable;
import com.knewton.mrtool.util.JsonFilenameDecorator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Sample Input Format for recommendations.
 * 
 * @author Giannis Neokleous
 * 
 */
public class RecommendationsInputFormat extends
        FileInputFormat<LongWritable, RecommendationWritable> {

    @Override
    public RecordReader<LongWritable, RecommendationWritable> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {

        return new JsonRecordReader<RecommendationWritable>() {

            /**
             * {@inheritDoc}
             */
            @Override
            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
                super.initialize(split, context);
                FileSplit fileSplit = (FileSplit) split;
                // decorate json record with originating filename.
                addDecorator(new JsonFilenameDecorator(fileSplit.getPath().getName()));
            }

            /**
             * {@inheritDoc}
             */
            @Override
            protected Class<?> getDataClass(String jsonStr) {
                return RecommendationWritable.class;
            }
        };
    }

}
