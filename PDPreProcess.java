import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



public class PDPreProcess {

    static Map<LongWritable,MapWritable> map;

    public static class mapper extends Mapper<Object, Text, LongWritable, MapWritable>{

        public void setup(Mapper<Object, Text, LongWritable, MapWritable>.Context context) throws IOException, InterruptedException{
            map = new HashMap<LongWritable, MapWritable>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String [] tokens = value.toString().split(" ");

            LongWritable nodeA = new LongWritable(Long.parseLong(tokens[0]));
            LongWritable nodeB = new LongWritable(Long.parseLong(tokens[1]));
            LongWritable weight = new LongWritable(Long.parseLong(tokens[2]));
            LongWritable endl = new LongWritable(-1);

            MapWritable adjList = new MapWritable();

            if(nodeA != nodeB){
                if(map.get(nodeA) == null){
                    adjList.put(nodeB, weight);
                    map.put(nodeA,adjList);
                }
                else{
                    adjList = map.get(nodeA);
                    adjList.put(nodeB, weight);
                }
            }
            else{
                if(map.get(nodeA) == null){
                    map.put(nodeA, adjList);
                }
            }
        }

        public void cleanup(Mapper<Object, Text, LongWritable, MapWritable>.Context context) throws IOException, InterruptedException{
            for (Map.Entry<LongWritable, MapWritable> entry : map.entrySet()){
                context.write(entry.getKey(), entry.getValue());
            }
        }
    }

    public static class reducer extends Reducer<LongWritable, MapWritable,LongWritable, MapWritable> {

        public void reduce(LongWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable adjList = new MapWritable();

            for (MapWritable val : values){
                for(Map.Entry<Writable, Writable> entry: val.entrySet()){
                    //2
                    if(((LongWritable)entry.getKey()).get() != -1){
                        adjList.put(entry.getKey(), entry.getValue());
                    }
                }
            }

            context.write(key, adjList);

        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job preProcessJob = Job.getInstance(conf, "preprocess");

        preProcessJob.setJarByClass(PDPreProcess.class);
        preProcessJob.setMapperClass(mapper.class);
        preProcessJob.setReducerClass(reducer.class);
        preProcessJob.setMapOutputKeyClass(LongWritable.class);
        preProcessJob.setMapOutputValueClass(MapWritable.class);
        preProcessJob.setOutputKeyClass(LongWritable.class);
        preProcessJob.setOutputValueClass(MapWritable.class);
        preProcessJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(preProcessJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(preProcessJob, new Path("/tmp/adjList"));
        FileSystem.get(preProcessJob.getConfiguration()).delete(new Path("/tmp/adjList"), true);

        preProcessJob.waitForCompletion(true);

    }




}
