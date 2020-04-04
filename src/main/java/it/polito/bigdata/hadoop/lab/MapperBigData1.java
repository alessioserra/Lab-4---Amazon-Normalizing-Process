package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    	    //Split in fields
    	    String[] fields = value.toString().split(",");
    	    
    	    //Select user code
    	    String user = fields[2]; 
    	    
    	    //To avoid format errors in score field, I sorround with try/catch
    	    try {
    	        int score = Integer.parseInt(fields[6]);
    	        if (score!=0)
    	            context.write(new Text(user), value);
    	    } catch (Exception e) {};
    }
}
