package uoa.di.ds.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class AggregationBolt extends BaseRichBolt {
/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
//For holding router-id and counts(times appeared so-far)
  Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
//For holding id and cpu_sum(total)
  Map<Integer, Integer> cpu_sum = new HashMap<Integer, Integer>();
  OutputCollector _collector; 
  
  private String field;
  private String operation;
  private int duration;
  
 public AggregationBolt(String field,String operation,int duration) {
	 this.field = field;
	 this.operation = operation;
	 this.duration = duration;
 }
  
  
  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
  }

  //execute is called to process tuples
  @Override
  public void execute(Tuple tuple) {
    //Get the router-id contents from the tuple
    Integer id = tuple.getInteger(4);
    //Have we counted any already?
    Integer count = counts.get(id);
    if (count == null)
      count = 0;
    //Increment the count and store it
    count++;
    counts.put(id, count);
    
    Integer curr_cpu_sum = cpu_sum.get(id);
    Integer cpu = tuple.getInteger(0);
    
    if (curr_cpu_sum == null)
    	curr_cpu_sum = cpu;
    else
    	curr_cpu_sum += cpu;
    
    cpu_sum.put(id, curr_cpu_sum);
    
    Integer cpu_average = cpu_sum.get(id)/counts.get(id);
    
    //Emit the word and the current count
    _collector.emit(new Values(id, cpu_average));
  }

  //Declare that we will emit a tuple containing two fields; word and count
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("id", "cpu_average"));
  }


}