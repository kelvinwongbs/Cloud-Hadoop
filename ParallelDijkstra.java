import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ParallelDijkstra {
    public static final LongWritable inf = new LongWritable(Long.MAX_VALUE);
    public static enum ConvergeIndicator { CHECK };
    
    public static class InitialiseMapper extends Mapper<LongWritable, MapWritable, LongWritable, PDNodeWritable>{
        private static LongWritable nid = new LongWritable();
        private static PDNodeWritable node = new PDNodeWritable();
        private static LongWritable src = new LongWritable();
        
        public void setup(Context context) {
            // retreive source node ID
            Configuration conf = context.getConfiguration();
            src.set(Long.parseLong(conf.get("src")));
        }
        
        public void map(LongWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
            nid = key;
            node.setNodeId(key);
            node.setDistance(key.equals(src) ? new LongWritable(0) : inf);
            node.setAdjList(value);
            node.setPrevNode(key);
            node.setFlag(new IntWritable(0));
            
            context.write(nid, node);
        }
        
    }
    
    public static class IteratedMapper extends Mapper<LongWritable, PDNodeWritable, LongWritable, PDNodeWritable>{

        public void map(LongWritable key, PDNodeWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);  // emit node structure
            
            LongWritable d = value.getDistance();
            MapWritable adjList = value.getAdjList();
            for(Map.Entry<Writable, Writable> entry: adjList.entrySet()){
                PDNodeWritable tmpnode = new PDNodeWritable();
                if (d.equals(inf)) {
                    tmpnode.setDistance(d);
                } else {
                    tmpnode.setDistance(new LongWritable(d.get() + ((LongWritable)entry.getValue()).get()));
                }
                tmpnode.setNodeId((LongWritable)entry.getKey());
                tmpnode.setPrevNode(key);
                tmpnode.setFlag(new IntWritable(1));  // to indicate non node strcuture

                context.write((LongWritable)entry.getKey(), tmpnode);  // emit distance and prev node
            }
        }
    }

    public static class IteratedReducer extends Reducer<LongWritable, PDNodeWritable, LongWritable, PDNodeWritable> {

        public void setup(Context context) {
            context.getCounter(ConvergeIndicator.CHECK).setValue(0);
        }
        
        public void reduce(LongWritable key, Iterable<PDNodeWritable> values, Context context) throws IOException, InterruptedException {      
            PDNodeWritable tmpnode = new PDNodeWritable();

            long dmin = Long.MAX_VALUE;
            long prevNode = -1;
            long dist = Long.MAX_VALUE;
            MapWritable adjList = new MapWritable();
            for (PDNodeWritable val : values){
                // find shortest distance and corresponding prev node
                if (val.getDistance().get() < dmin) {
                        dmin = val.getDistance().get();
                        prevNode = val.getPrevNode().get();
                }
                // retreive node structure
                if (val.getFlag().equals(new IntWritable(0))) {
                    adjList = new MapWritable(val.getAdjList());
                    dist = val.getDistance().get();
                } 
            }

            tmpnode.setNodeId(key);
            tmpnode.setDistance(new LongWritable(dmin));
            tmpnode.setAdjList(adjList);
            if (prevNode > 0) {
                tmpnode.setPrevNode(new LongWritable(prevNode));
            }
            tmpnode.setFlag(new IntWritable(0));

            context.write(key, tmpnode);

            if (dmin != dist) {
                context.getCounter(ConvergeIndicator.CHECK).setValue(1);
            }
        }
    }
    
    public static class DisplayMapper extends Mapper<LongWritable, PDNodeWritable, LongWritable, PDNodeWritable>{
                
        public void map(LongWritable key, PDNodeWritable value, Context context) throws IOException, InterruptedException {
            if (value.getDistance().compareTo(inf) < 0) {
                context.write(key, value);  // emit reachable node
            }
        }
        
    }
    
    public static class DisplayReducer extends Reducer<LongWritable, PDNodeWritable, Text, Text> {
        
        public void reduce(LongWritable key, Iterable<PDNodeWritable> values, Context context) throws IOException, InterruptedException {
            // convert node to text
            for (PDNodeWritable val : values){
                String dist = val.getDistance().toString();
                String prevNode = val.getPrevNode().toString();
                Text output = new Text();
                output.set(dist + ' ' + prevNode);
                Text nid = new Text();
                nid.set(key.toString());
                context.write(nid, output);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        PDPreProcess.main(args);
        
        Configuration conf = new Configuration();
        conf.set("src", args[2]);
        // initialisation
        Job InitialiseJob =  Job.getInstance(conf, "initialise");
        InitialiseJob.setNumReduceTasks(0);  // map only job
        InitialiseJob.setJarByClass(ParallelDijkstra.class);
        InitialiseJob.setMapperClass(InitialiseMapper.class);
        InitialiseJob.setMapOutputKeyClass(LongWritable.class);
        InitialiseJob.setMapOutputValueClass(PDNodeWritable.class);
        InitialiseJob.setOutputKeyClass(LongWritable.class);
        InitialiseJob.setOutputValueClass(PDNodeWritable.class);
        InitialiseJob.setInputFormatClass(SequenceFileInputFormat.class);
        InitialiseJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(InitialiseJob, new Path("/tmp/adjList"));
        FileOutputFormat.setOutputPath(InitialiseJob, new Path("/tmp/intermediate-0"));
        FileSystem.get(InitialiseJob.getConfiguration()).delete(new Path("/tmp/intermediate-0"), true);
        
        if (!InitialiseJob.waitForCompletion(true)) {
            System.exit(1);
        }
        
        // iterated mapreduce jobs
        long itrs = Long.parseLong(args[3]);
        long reachCount = 0;
        boolean check = true;

        while ((itrs > 0 && reachCount < itrs && check) || (itrs == 0 && check)) {
            Job IteratedJob = Job.getInstance(conf, String.format("iteration %d", reachCount));

            IteratedJob.setJarByClass(ParallelDijkstra.class);
            IteratedJob.setMapperClass(IteratedMapper.class);
            IteratedJob.setReducerClass(IteratedReducer.class);
            IteratedJob.setMapOutputKeyClass(LongWritable.class);
            IteratedJob.setMapOutputValueClass(PDNodeWritable.class);
            IteratedJob.setOutputKeyClass(LongWritable.class);
            IteratedJob.setOutputValueClass(PDNodeWritable.class);
            IteratedJob.setInputFormatClass(SequenceFileInputFormat.class);
            IteratedJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(IteratedJob, new Path(String.format("/tmp/intermediate-%d", reachCount)));
            FileOutputFormat.setOutputPath(IteratedJob, new Path(String.format("/tmp/intermediate-%d", reachCount + 1)));
            FileSystem.get(IteratedJob.getConfiguration()).delete(new Path(String.format("/tmp/intermediate-%d", reachCount + 1)), true);

            if (IteratedJob.waitForCompletion(true)) {
                reachCount++;
            } else {
                System.exit(1);
            }

            check = (IteratedJob.getCounters().findCounter(ParallelDijkstra.ConvergeIndicator.CHECK).getValue() != 0);
        }
        
        // display
        conf.set("mapred.textoutputformat.separator", " ");
        Job DisplayJob = Job.getInstance(conf, "display");
        DisplayJob.setJarByClass(ParallelDijkstra.class);
        DisplayJob.setMapperClass(DisplayMapper.class);
        DisplayJob.setReducerClass(DisplayReducer.class);
        DisplayJob.setMapOutputKeyClass(LongWritable.class);
        DisplayJob.setMapOutputValueClass(PDNodeWritable.class);
        DisplayJob.setOutputKeyClass(Text.class);
        DisplayJob.setOutputValueClass(Text.class);
        DisplayJob.setInputFormatClass(SequenceFileInputFormat.class);
        DisplayJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(DisplayJob, new Path(String.format("/tmp/intermediate-%d", reachCount)));
        FileOutputFormat.setOutputPath(DisplayJob, new Path(args[1]));
        
        System.exit(DisplayJob.waitForCompletion(true) ? 0 : 1);
    }
}