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

        JsonRecordReader<RecommendationWritable> rr = new JsonRecordReader<RecommendationWritable>() {
            /**
             * {@inheritDoc}
             */
            @Override
            protected Class<?> getDataClass(String jsonStr) {
                return RecommendationWritable.class;
            }
        };
        FileSplit fileSplit = (FileSplit) split;
        rr.addDecorator(new JsonFilenameDecorator(fileSplit.getPath().getName()));
        
        return rr;
    }

}
