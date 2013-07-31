/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
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
