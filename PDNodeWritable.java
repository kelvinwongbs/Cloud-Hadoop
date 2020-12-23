import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PDNodeWritable implements Writable {
    private LongWritable nodeId;
    private LongWritable distance;
    private MapWritable adjList;
    private LongWritable prevNode;
    private IntWritable flag;

    public  PDNodeWritable(){
        nodeId = new LongWritable();
        distance = new LongWritable(Long.MAX_VALUE);
        adjList = new MapWritable();
        prevNode = new LongWritable();
        flag = new IntWritable();
    }

    public void setNodeId(LongWritable nodeId){
        this.nodeId = nodeId;
    }
    
    public void setDistance(LongWritable distance){
        this.distance = distance;
    }
    
    public void setAdjList(MapWritable adjList){
        this.adjList = adjList;
    }

    public void setPrevNode(LongWritable prevNode){
        this.prevNode = prevNode;
    }
    
    public void setFlag(IntWritable flag){
        this.flag = flag;
    }

    public LongWritable getNodeId(){
        return nodeId;
    }
    
    public LongWritable getDistance(){
        return distance;
    }
    
    public MapWritable getAdjList(){
        return adjList;
    }
    
    public LongWritable getPrevNode(){
        return prevNode;
    }
    
    public IntWritable getFlag(){
        return flag;
    }

    public void write(DataOutput dataOutput) throws IOException {
        nodeId.write(dataOutput);
        distance.write(dataOutput);
        adjList.write(dataOutput);
        prevNode.write(dataOutput);
        flag.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        nodeId.readFields(dataInput);
        distance.readFields(dataInput);
        adjList.readFields(dataInput);
        prevNode.readFields(dataInput);
        flag.readFields(dataInput);
    }
}