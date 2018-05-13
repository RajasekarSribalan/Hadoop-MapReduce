/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wordcount_filename;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Word count example
 * 
 * For more info,please refer readme.txt
 * 
 * @author raj
 *
 */

public class WordCountFileName {

	static Log log = LogFactory.getLog(WordCountFileName.class) ;
	/**
	 * Mapper class
	 * 
	 * @author raj
	 *
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			Text fileName = new Text(((FileSplit) context.getInputSplit()).getPath().getName());
			log.info("Line : " + itr);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				log.info(word + " " + fileName);
				context.write(word, fileName);
			}
		}
	}

	/**
	 * Reducer class
	 * 
	 * @author raj
	 *
	 */
	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<Text, AtomicInteger> map = new HashedMap();
			for (Text val : values) {

				if (!map.containsKey(val)) {
					map.put(val, new AtomicInteger(0));
				}
				map.get(val).incrementAndGet();
			}
			for (Map.Entry<Text, AtomicInteger> entry : map.entrySet()) {
				log.info("key : " + key);
				log.info("key : " + entry.getKey());
				log.info("key : " + entry.getValue());
				context.write(key, new Text(entry.getKey()+" "+entry.getValue().toString()));
			}

		}

	}

	/**
	 * Main class
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count with filename");
		job.setJarByClass(WordCountFileName.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
