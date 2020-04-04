package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
    	
	LinkedList<String> list;
	HashMap<String, Double> means;
	/*
	 * Setup method to achieve the list of product
	 */
	protected void setup(Context context) {
		list = new LinkedList<String>();
		means = new HashMap<String,Double>();
	}
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	    int numberOfReview = 0;
    	    int sum = 0;
            
            for (Text value : values) {
            	//Save in memory all the lines
            	list.add(value.toString());
            	
            	//Select score fields
                String[] fields = value.toString().split(",");
                int score = Integer.parseInt(fields[6]);  //There are only score != 0 filtered by mapper
                
                //Compute sum score and numberReview score for the mean
                sum = sum + score;
                numberOfReview++;               
            }
            
            //Compute mean and add to map of mean
            double mean = sum/numberOfReview;
            means.put(key.toString(), mean);
        }
    
	    protected void cleanup(Context context) {
	    	//For every line I compute che new line with normalized score
	    	for (String lines : list) {
	    		String[] fields = lines.split(",");
	    		String user = fields[2];
	    		String product =  fields[1];
	    		Double score = Integer.parseInt(fields[6]) - means.get(user);
	    		
	    		try {
					context.write(NullWritable.get(), new Text(product+" "+score));
				} catch (Exception e) {}
	    	}
	    	
	    	
	}
}
