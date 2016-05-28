package uoa.di.ds.storm.bolt;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class AggregationBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	private Timer timer;
	private static final int EMIT_TIMEFRAME = 10; // each 10 seconds
	
	//For holding router-id and counts(times appeared so-far)
	Map<Integer, Integer> counts;
	//For holding id and sum(total)
	Map<Integer, Integer> sum;
	Map<Integer, Integer> top;
	Map<Integer, Integer> bottom;
	Integer average;
	
	
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
    
    counts = Collections.synchronizedMap(new HashMap<Integer, Integer>());
    
    sum    = Collections.synchronizedMap(new HashMap<Integer, Integer>());
    top    = Collections.synchronizedMap(new HashMap<Integer, Integer>());
    bottom = Collections.synchronizedMap(new HashMap<Integer, Integer>());
    
    average = 0;

	// Round the timestamp. ie: 2016-05-26T19:21:00.000Z
    DateTime now = new DateTime().withZone(DateTimeZone.UTC);
    int roundSeconds = ((now.getSecondOfMinute() / EMIT_TIMEFRAME) * EMIT_TIMEFRAME) + EMIT_TIMEFRAME;
    DateTime startAt = now.minuteOfHour().roundFloorCopy().plusSeconds(roundSeconds);
    
	this.timer = new Timer();
	this.timer.scheduleAtFixedRate(new EmitTask(sum, top, bottom, average, _collector), startAt.toDate(), EMIT_TIMEFRAME * 1000L);
  }

  //execute is called to process tuples
  @Override
  public void execute(Tuple tuple) {
	  
	System.out.println("TUPLE:" + tuple.toString()); 
	  
	if (field.equals("cpu")) {
		if (operation.equals("AVG")) {
			
		    //Get the router-id contents from the tuple
		    Integer id = tuple.getInteger(4);
		    //Have we counted any already?
		    Integer count = counts.get(id);
		    if (count == null)
		      count = 0;
		    //Increment the count and store it
		    count++;
		    counts.put(id, count);
		    
		    Integer curr_cpu_sum = sum.get(id);
		    Integer cpu = tuple.getInteger(0);
		    
		    if (curr_cpu_sum == null)
		    	curr_cpu_sum = cpu;
		    else
		    	curr_cpu_sum += cpu;
		    
		    sum.put(id, curr_cpu_sum);
		    
		    average = sum.get(id)/counts.get(id);		    
		}
		else if (operation.equals("TOP")) {
		    //Get the router-id contents from the tuple
		    Integer id = tuple.getInteger(4);
		    Integer cpu = tuple.getInteger(0);
		    
		    Integer curr_top = top.get(id);
		    
		    if (curr_top == null)
		    	curr_top = cpu;
		    else {
		    	if (curr_top < cpu)
		    		top.put(id, cpu);               //Replace top
 		    }		 

		}
		else if (operation.equals("BOTTOM")) {
		    //Get the router-id contents from the tuple
		    Integer id = tuple.getInteger(4);
		    Integer cpu = tuple.getInteger(0);
		    
		    Integer curr_bottom = bottom.get(id);
		    
		    if (curr_bottom == null)
		    	curr_bottom = cpu;
		    else {
		    	if (curr_bottom > cpu)
		    		top.put(id, cpu);               //Replace bottom
 		    }		
		}
	}
	else if (field.equals("ram")) {
		if (operation.equals("AVG")) {
		    //Get the router-id contents from the tuple
		    Integer id = tuple.getInteger(4);
		    //Have we counted any already?
		    Integer count = counts.get(id);
		    if (count == null)
		      count = 0;
		    //Increment the count and store it
		    count++;
		    counts.put(id, count);
		    
		    Integer curr_ram_sum = sum.get(id);
		    Integer ram = tuple.getInteger(1);
		    
		    if (curr_ram_sum == null)
		    	curr_ram_sum = ram;
		    else
		    	curr_ram_sum += ram;
		    
		    sum.put(id, curr_ram_sum);
		    
		    average = sum.get(id)/counts.get(id);		    
		}
		else if (operation.equals("TOP")) {
		    //Get the router-id contents from the tuple
		    Integer id = tuple.getInteger(4);
		    Integer ram = tuple.getInteger(1);
		    
		    Integer curr_top = top.get(id);
		    
		    if (curr_top == null)
		    	curr_top = ram;
		    else {
		    	if (curr_top < ram)
		    		top.put(id, ram);               //Replace top
 		    }	
		}
		else if (operation.equals("BOTTOM")) {
		    //Get the router-id contents from the tuple
		    Integer id = tuple.getInteger(4);
		    Integer ram = tuple.getInteger(0);
		    
		    Integer curr_bottom = bottom.get(id);
		    
		    if (curr_bottom == null)
		    	curr_bottom = ram;
		    else {
		    	if (curr_bottom > ram)
		    		top.put(id, ram);               //Replace bottom
 		    }		
		}
	}
	else if (field.equals("activeSessions")) {
		if (operation.equals("AVG")) {
		    //Get the router-id contents from the tuple
		    Integer id = tuple.getInteger(4);
		    //Have we counted any already?
		    Integer count = counts.get(id);
		    if (count == null)		    	
		      count = 0;
		    //Increment the count and store it
		    count++;
		    counts.put(id, count);
		    
		    Integer curr_as_sum = sum.get(id);
		    Integer activeSessions = tuple.getInteger(2);
		    
		    if (curr_as_sum == null)
		    	curr_as_sum = activeSessions;
		    else
		    	curr_as_sum += activeSessions;
		    
		    sum.put(id, curr_as_sum);
		    
		    average = sum.get(id)/counts.get(id);		    			
		}
		else if (operation.equals("TOP")) {
		    //Get the router-id contents from the tuple
		    Integer id = tuple.getInteger(4);
		    Integer activeSessions = tuple.getInteger(2);
		    
		    Integer curr_top = top.get(id);
		    
		    if (curr_top == null)
		    	curr_top = activeSessions;
		    else {
		    	if (curr_top < activeSessions)
		    		top.put(id, activeSessions);               //Replace top
 		    }	
		}
		else if (operation.equals("BOTTOM")) {
		    //Get the router-id contents from the tuple
		    Integer id = tuple.getInteger(4);
		    Integer activeSessions = tuple.getInteger(2);
		    
		    Integer curr_bottom = bottom.get(id);
		    
		    if (curr_bottom == null)
		    	curr_bottom = activeSessions;
		    else {
		    	if (curr_bottom > activeSessions)
		    		top.put(id, activeSessions);               //Replace bottom
 		    }		
		}
		else if (operation.equals("SUM")) {
		    //Get the router-id contents from the tuple
		    Integer id = tuple.getInteger(4);
		    
		    Integer curr_as_sum = sum.get(id);
		    Integer activeSessions = tuple.getInteger(2);
		    
		    if (curr_as_sum == null)
		    	curr_as_sum = activeSessions;
		    else
		    	curr_as_sum += activeSessions;
		    
		    sum.put(id, curr_as_sum);
		}
	}
	else if (field.equals("upTime")) {
		if (operation.equals("TOP")) {
		    //Get the router-id contents from the tuple
		    Integer id = tuple.getInteger(4);
		    Integer upTime = tuple.getInteger(1);
		    
		    Integer curr_top = top.get(id);
		    
		    if (curr_top == null)
		    	curr_top = upTime;
		    else {
		    	if (curr_top < upTime)
		    		top.put(id, upTime);               //Replace top
 		    }	
		}
	}
	else if (field.equals("temperature")) {
		if (operation.equals("AVG")) {
		    //Get the router-id contents from the tuple
		    Integer id = tuple.getInteger(4);
		    //Have we counted any already?
		    Integer count = counts.get(id);
		    if (count == null)		    	
		      count = 0;
		    //Increment the count and store it
		    count++;
		    counts.put(id, count);
		    
		    Integer curr_temp_sum = sum.get(id);
		    Integer temperature = tuple.getInteger(7);
		    
		    if (curr_temp_sum == null)
		    	curr_temp_sum = temperature;
		    else
		    	curr_temp_sum += temperature;
		    
		    sum.put(id, curr_temp_sum);
		    
		    average = sum.get(id)/counts.get(id);			
		}
		else if (operation.equals("TOP")) {
		    //Get the router-id contents from the tuple
		    Integer id = tuple.getInteger(4);
		    Integer temperature = tuple.getInteger(7);
		    
		    Integer curr_top = top.get(id);
		    
		    if (curr_top == null)
		    	curr_top = temperature;
		    else {
		    	if (curr_top < temperature)
		    		top.put(id, temperature);               //Replace top
 		    }	
		}
		else if (operation.equals("BOTTOM")) {
		    //Get the router-id contents from the tuple
		    Integer id = tuple.getInteger(4);
		    Integer temperature = tuple.getInteger(7);
		    
		    Integer curr_bottom = bottom.get(id);
		    
		    if (curr_bottom == null)
		    	curr_bottom = temperature;
		    else {
		    	if (curr_bottom > temperature)
		    		top.put(id, temperature);               //Replace bottom
 		    }	
		}
	}
	else {
		//What?
	}
    _collector.ack(tuple);
    //Emit the word and the current count
    //_collector.emit(new Values(id, cpu_average));
  }

  //Declare that we will emit a tuple containing two fields; word and count
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("id", "cpu_average"));
  }


  @Override
  public void cleanup() {
	  this.timer.cancel();
	  this.timer.purge();
  }

  class EmitTask extends TimerTask {
	  
	  private final Map<Integer, Integer> sum;
	  private final Map<Integer, Integer> top;
	  private final Map<Integer, Integer> bottom;
	  private final Integer average;

	  
	  private final OutputCollector collector;
	  
	  public EmitTask(Map<Integer, Integer> sum, Map<Integer, Integer> top, Map<Integer, Integer> bottom, Integer average, OutputCollector collector) {
		  	this.sum = sum;
		  	this.top = top;
		  	this.bottom = bottom;
		  	this.average = average;
		  	this.collector = collector;
	  }
	  
	  @Override
	  public void run() {
		  // create snapshot
//		  Map<String, Integer> snapshot;
//		  synchronized (this.counts) {
//			  snapshot = new HashMap<String, Integer>(this.counts);
//			  this.counts.clear();
//		  }
//		  
//		  long currentTime = System.currentTimeMillis();
//		  Set<String> keys = snapshot.keySet();
//		  for (String word : keys) {
//			  Integer count = snapshot.get(word);
//			  this.collector.emit(new Values(currentTime, word, count));
//		  }
	  }
  }
}