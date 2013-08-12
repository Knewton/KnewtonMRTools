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
package com.knewton.mrtool.example;

import com.knewton.mrtool.io.RecommendationsInputFormat;
import com.knewton.mrtool.model.RecommendationWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Example job using the JsonRecordReader. This job uses the identity mapper and reducer to simply
 * read in the json records and emit their string representation.
 * 
 * @author Giannis Neokleous
 * 
 */
public class JsonMRExample {

    /**
     * 
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException,
            ClassNotFoundException {
        Job job = new Job(new Configuration());

        job.setInputFormatClass(RecommendationsInputFormat.class);
        RecommendationsInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(RecommendationWritable.class);

        job.waitForCompletion(true);
    }

}
