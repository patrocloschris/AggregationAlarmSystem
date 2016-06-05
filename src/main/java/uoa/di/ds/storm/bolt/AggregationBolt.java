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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uoa.di.ds.storm.spout.RandomEventGeneratorSpout;
import uoa.di.ds.storm.utils.Cons;

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class AggregationBolt extends BaseRichBolt {

	private static final Logger LOG = LoggerFactory.getLogger(RandomEventGeneratorSpout.class);
	private static final long serialVersionUID = 1L;
	
	private Timer timer;
	private Integer EMIT_TIMEFRAME = 10; // each 10 seconds (default)
	private String boltName;
	//For holding router-id and counts(times appeared so-far)
	Map<Integer, Integer> counts;
	//For holding id and sum(total)
	Map<Integer, Integer> sum;
	Map<Integer, Integer> top;
	Map<Integer, Integer> bottom;
	Map<Integer, String> name;
	Map<Integer, String> site;
	Integer average;
	
	
	OutputCollector _collector; 
  
	private String field;
	private String operation;
	private Integer duration;
  
	
 public AggregationBolt(String field,String operation, Integer duration) {
	 this.field = field;
	 this.operation = operation;
	 this.duration = duration;
	 this.boltName=field.concat("_").concat(operation);
	 LOG.info("Creating Bolt with field=[{}], operation=[{}], duration=[{}]",field,operation,duration);
 }
  
  
  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
    
    counts = Collections.synchronizedMap(new HashMap<Integer, Integer>());
    
    sum    = Collections.synchronizedMap(new HashMap<Integer, Integer>());
    top    = Collections.synchronizedMap(new HashMap<Integer, Integer>());
    bottom = Collections.synchronizedMap(new HashMap<Integer, Integer>());
    
    name = Collections.synchronizedMap(new HashMap<Integer, String>());
    site = Collections.synchronizedMap(new HashMap<Integer, String>());
    
    average = 0;

    EMIT_TIMEFRAME = duration;
	// Round the timestamp. ie: 2016-05-26T19:21:00.000Z
    DateTime now = new DateTime().withZone(DateTimeZone.UTC);
    int roundSeconds = ((now.getSecondOfMinute() / EMIT_TIMEFRAME) * EMIT_TIMEFRAME) + EMIT_TIMEFRAME;
    DateTime startAt = now.minuteOfHour().roundFloorCopy().plusSeconds(roundSeconds);
    
	this.timer = new Timer();
	this.timer.scheduleAtFixedRate(new EmitTask(this.boltName,sum, top, bottom, average, name, site, field, operation, duration, _collector), startAt.toDate(), EMIT_TIMEFRAME * 1000L);
	 LOG.info("Preapating Bolt=[{}]",this.boltName);
  }

  //execute is called to process tuples
  @Override
  public void execute(Tuple tuple) {
	  
	LOG.info("BOLT=[{}]:Recieved tuple=>[{}]",this.boltName, tuple.toString()); 
	  
	if (field.equals("cpu")) {
		if (operation.equals("AVG")) {
			
		    //Get the router-id contents from the tuple
			Integer id = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
			
			String name_t = name.get(id);
			if (name_t == null) 
				name.put(id, tuple.getStringByField(Cons.TUPLE_VAR_NAME));
			
			String site_t = site.get(id);
			if (site_t == null) 
				site.put(id, tuple.getStringByField(Cons.TUPLE_VAR_SITE));

			
			
		    //Have we counted any already?
		    Integer count = counts.get(id);
		    if (count == null)
		      count = 0;
		    //Increment the count and store it
		    count++;
		    counts.put(id, count);
		    
		    Integer curr_cpu_sum = sum.get(id);
		    Integer cpu = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
		    
		    if (curr_cpu_sum == null)
		    	curr_cpu_sum = cpu;
		    else
		    	curr_cpu_sum += cpu;
		    
		    sum.put(id, curr_cpu_sum);
		    
		    average = sum.get(id)/counts.get(id);		    
		}
		else if (operation.equals("TOP")) {
		    //Get the router-id contents from the tuple
			Integer id = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
			
			String name_t = name.get(id);
			if (name_t == null) 
				name.put(id, tuple.getStringByField(Cons.TUPLE_VAR_NAME));
			
			String site_t = site.get(id);
			if (site_t == null) 
				site.put(id, tuple.getStringByField(Cons.TUPLE_VAR_SITE));
			
		    Integer cpu = tuple.getIntegerByField(Cons.TUPLE_VAR_CPU);
		    
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
			Integer id = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
			
			String name_t = name.get(id);
			if (name_t == null) 
				name.put(id, tuple.getStringByField(Cons.TUPLE_VAR_NAME));
			
			String site_t = site.get(id);
			if (site_t == null) 
				site.put(id, tuple.getStringByField(Cons.TUPLE_VAR_SITE));
			
		    Integer cpu = tuple.getIntegerByField(Cons.TUPLE_VAR_CPU);
		    
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
			Integer id = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
			
			String name_t = name.get(id);
			if (name_t == null) 
				name.put(id, tuple.getStringByField(Cons.TUPLE_VAR_NAME));
			
			String site_t = site.get(id);
			if (site_t == null) 
				site.put(id, tuple.getStringByField(Cons.TUPLE_VAR_SITE));
		    //Have we counted any already?
		    Integer count = counts.get(id);
		    if (count == null)
		      count = 0;
		    //Increment the count and store it
		    count++;
		    counts.put(id, count);
		    
		    Integer curr_ram_sum = sum.get(id);
		    Integer ram = tuple.getIntegerByField(Cons.TUPLE_VAR_RAM);
		    
		    if (curr_ram_sum == null)
		    	curr_ram_sum = ram;
		    else
		    	curr_ram_sum += ram;
		    
		    sum.put(id, curr_ram_sum);
		    
		    average = sum.get(id)/counts.get(id);		    
		}
		else if (operation.equals("TOP")) {
		    //Get the router-id contents from the tuple
			Integer id = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
			
			String name_t = name.get(id);
			if (name_t == null) 
				name.put(id, tuple.getStringByField(Cons.TUPLE_VAR_NAME));
			
			String site_t = site.get(id);
			if (site_t == null) 
				site.put(id, tuple.getStringByField(Cons.TUPLE_VAR_SITE));
		    Integer ram = tuple.getIntegerByField(Cons.TUPLE_VAR_RAM);
		    
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
			Integer id = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
			
			String name_t = name.get(id);
			if (name_t == null) 
				name.put(id, tuple.getStringByField(Cons.TUPLE_VAR_NAME));
			
			String site_t = site.get(id);
			if (site_t == null) 
				site.put(id, tuple.getStringByField(Cons.TUPLE_VAR_SITE));
		    Integer ram = tuple.getIntegerByField(Cons.TUPLE_VAR_RAM);
		    
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
			Integer id = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
			
			String name_t = name.get(id);
			if (name_t == null) 
				name.put(id, tuple.getStringByField(Cons.TUPLE_VAR_NAME));
			
			String site_t = site.get(id);
			if (site_t == null) 
				site.put(id, tuple.getStringByField(Cons.TUPLE_VAR_SITE));
		    //Have we counted any already?
		    Integer count = counts.get(id);
		    if (count == null)		    	
		      count = 0;
		    //Increment the count and store it
		    count++;
		    counts.put(id, count);
		    
		    Integer curr_as_sum = sum.get(id);
		    Integer activeSessions = tuple.getIntegerByField(Cons.TUPLE_VAR_ACTIVESESSIONS);
		    
		    if (curr_as_sum == null)
		    	curr_as_sum = activeSessions;
		    else
		    	curr_as_sum += activeSessions;
		    
		    sum.put(id, curr_as_sum);
		    
		    average = sum.get(id)/counts.get(id);		    			
		}
		else if (operation.equals("TOP")) {
		    //Get the router-id contents from the tuple
			Integer id = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
			
			String name_t = name.get(id);
			if (name_t == null) 
				name.put(id, tuple.getStringByField(Cons.TUPLE_VAR_NAME));
			
			String site_t = site.get(id);
			if (site_t == null) 
				site.put(id, tuple.getStringByField(Cons.TUPLE_VAR_SITE));
		    Integer activeSessions = tuple.getIntegerByField(Cons.TUPLE_VAR_ACTIVESESSIONS);
		    
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
			Integer id = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
			
			String name_t = name.get(id);
			if (name_t == null) 
				name.put(id, tuple.getStringByField(Cons.TUPLE_VAR_NAME));
			
			String site_t = site.get(id);
			if (site_t == null) 
				site.put(id, tuple.getStringByField(Cons.TUPLE_VAR_SITE));
		    Integer activeSessions = tuple.getIntegerByField(Cons.TUPLE_VAR_ACTIVESESSIONS);
		    
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
			Integer id = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
			
			String name_t = name.get(id);
			if (name_t == null) 
				name.put(id, tuple.getStringByField(Cons.TUPLE_VAR_NAME));
			
			String site_t = site.get(id);
			if (site_t == null) 
				site.put(id, tuple.getStringByField(Cons.TUPLE_VAR_SITE));
		    
		    Integer curr_as_sum = sum.get(id);
		    Integer activeSessions = tuple.getIntegerByField(Cons.TUPLE_VAR_ACTIVESESSIONS);
		    
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
			Integer id = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
			
			String name_t = name.get(id);
			if (name_t == null) 
				name.put(id, tuple.getStringByField(Cons.TUPLE_VAR_NAME));
			
			String site_t = site.get(id);
			if (site_t == null) 
				site.put(id, tuple.getStringByField(Cons.TUPLE_VAR_SITE));
			
		    Integer upTime = tuple.getIntegerByField(Cons.TUPLE_VAR_UPTIME);
		    
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
			Integer id = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
			
			String name_t = name.get(id);
			if (name_t == null) 
				name.put(id, tuple.getStringByField(Cons.TUPLE_VAR_NAME));
			
			String site_t = site.get(id);
			if (site_t == null) 
				site.put(id, tuple.getStringByField(Cons.TUPLE_VAR_SITE));
		    //Have we counted any already?
		    Integer count = counts.get(id);
		    if (count == null)		    	
		      count = 0;
		    //Increment the count and store it
		    count++;
		    counts.put(id, count);
		    
		    Integer curr_temp_sum = sum.get(id);
		    Integer temperature = tuple.getIntegerByField(Cons.TUPLE_VAR_TEMPERATURE);
		    
		    if (curr_temp_sum == null)
		    	curr_temp_sum = temperature;
		    else
		    	curr_temp_sum += temperature;
		    
		    sum.put(id, curr_temp_sum);
		    
		    average = sum.get(id)/counts.get(id);			
		}
		else if (operation.equals("TOP")) {
		    //Get the router-id contents from the tuple
			Integer id = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
			
			String name_t = name.get(id);
			if (name_t == null) 
				name.put(id, tuple.getStringByField(Cons.TUPLE_VAR_NAME));
			
			String site_t = site.get(id);
			if (site_t == null) 
				site.put(id, tuple.getStringByField(Cons.TUPLE_VAR_SITE));
			
		    Integer temperature = tuple.getIntegerByField(Cons.TUPLE_VAR_TEMPERATURE);
		    
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
			Integer id = tuple.getIntegerByField(Cons.TUPLE_VAR_ID);
			
			String name_t = name.get(id);
			if (name_t == null) 
				name.put(id, tuple.getStringByField(Cons.TUPLE_VAR_NAME));
			
			String site_t = site.get(id);
			if (site_t == null) 
				site.put(id, tuple.getStringByField(Cons.TUPLE_VAR_SITE));
			
		    Integer temperature = tuple.getIntegerByField(Cons.TUPLE_VAR_TEMPERATURE);
		    
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
    declarer.declare(new Fields(Cons.TUPLE_VAR_ID, Cons.TUPLE_VAR_NAME, Cons.TUPLE_VAR_SITE, Cons.TUPLE_VAR_EVENTTIME, Cons.TUPLE_VAR_VALUE, 
    		Cons.TUPLE_VAR_DURATION, Cons.TUPLE_VAR_FIELD, Cons.TUPLE_VAR_OPER));        
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
	  private Integer average;
	  
	  private final Map<Integer, String> name;
	  private final Map<Integer, String> site;
	  
	  private final String field;
	  private final String operation;
	  private final Integer duration;
	  private final String boltName;
	  
	  private final OutputCollector collector;
	  
	  public EmitTask(String boltName,Map<Integer, Integer> sum, Map<Integer, Integer> top, Map<Integer, Integer> bottom, Integer average,  Map<Integer, String> name, Map<Integer, String> site, String field, String operation, Integer duration, OutputCollector collector) {
		  
		  	this.sum = sum;
		  	this.top = top;
		  	this.bottom = bottom;
		  	this.average = average;
		  	
		  	this.name = name;
		  	this.site = site;
		 
		  	
		  	this.field = field;
		  	this.operation = operation;
		  	this.duration = duration;
		  	
		  	this.collector = collector;
		  	this.boltName = boltName;
	  }
	  
	  @Override
	  public void run() {
		LOG.info("BOLT=[{}]: Time to emit all values",this.boltName); 
			  

		  //Do we need sync here?
		  if (this.field.equals("cpu") || this.field.equals("ram") || this.field.equals("temperature")) {
			  
			  if (this.operation.equals("AVG")) {
				  Integer average_;								
				  
				  synchronized (this.average) {
					  average_ = this.average;
					  this.average = 0;
				  }					  
					  
				  long currentTime = System.currentTimeMillis();
				  Set<Integer> keys = this.name.keySet();
				  for (Integer id : keys) {
					  String name_  = this.name.get(id);
					  String site_  = this.site.get(id);
					  this.collector.emit(new Values(id, name_, site_, currentTime, average_, this.duration, this.field, this.operation));
				  }

			  }
			  else if (this.operation.equals("TOP")) {
				  Map<Integer, Integer> top_snapshot;
				  
				  synchronized (this.top) {
					  top_snapshot = new HashMap<Integer, Integer>(this.top);
					  this.top.clear();
				  }
				  
				  long currentTime = System.currentTimeMillis();
				  Set<Integer> keys = top_snapshot.keySet();
				  for (Integer id : keys) {
					  Integer aggr_value_top = top_snapshot.get(id);
					  String name_  = this.name.get(id);
					  String site_  = this.site.get(id);
					  this.collector.emit(new Values(id, name_, site_, currentTime, aggr_value_top, this.duration, this.field, this.operation));
				  }
			  }
			  else if (this.operation.equals("BOTTOM")) {
				  Map<Integer, Integer> bottom_snapshot;
				  
				  synchronized (this.bottom) {
					  bottom_snapshot = new HashMap<Integer, Integer>(this.bottom);
					  this.bottom.clear();
				  }
				  
				  long currentTime = System.currentTimeMillis();
				  Set<Integer> keys = bottom_snapshot.keySet();
				  for (Integer id : keys) {
					  Integer aggr_value_bottom = bottom_snapshot.get(id);
					  String name_  = this.name.get(id);
					  String site_  = this.site.get(id);
					  this.collector.emit(new Values(id, name_, site_, currentTime, aggr_value_bottom, this.duration, this.field, this.operation));
				  }
			  }
			  
		  }
		  else if (this.field.equals("activeSessions")) {
			  if (this.operation.equals("AVG")) {
				  Integer average_;								
				  
				  synchronized (this.average) {
					  average_ = this.average;
					  this.average = 0;
				  }					  
					  
				  long currentTime = System.currentTimeMillis();
				  Set<Integer> keys = this.name.keySet();
				  for (Integer id : keys) {
					  String name_  = this.name.get(id);
					  String site_  = this.site.get(id);
					  this.collector.emit(new Values(id, name_, site_, currentTime, average_, this.duration, this.field, this.operation));
				  }

			  }
			  else if (this.operation.equals("TOP")) {
				  Map<Integer, Integer> top_snapshot;
				  
				  synchronized (this.top) {
					  top_snapshot = new HashMap<Integer, Integer>(this.top);
					  this.top.clear();
				  }
				  
				  long currentTime = System.currentTimeMillis();
				  Set<Integer> keys = top_snapshot.keySet();
				  for (Integer id : keys) {
					  Integer aggr_value_top = top_snapshot.get(id);
					  String name_  = this.name.get(id);
					  String site_  = this.site.get(id);
					  this.collector.emit(new Values(id, name_, site_, currentTime, aggr_value_top, this.duration, this.field, this.operation));
				  }
			  }
			  else if (this.operation.equals("BOTTOM")) {
				  Map<Integer, Integer> bottom_snapshot;
				  
				  synchronized (this.bottom) {
					  bottom_snapshot = new HashMap<Integer, Integer>(this.bottom);
					  this.bottom.clear();
				  }
				  
				  long currentTime = System.currentTimeMillis();
				  Set<Integer> keys = bottom_snapshot.keySet();
				  for (Integer id : keys) {
					  Integer aggr_value_bottom = bottom_snapshot.get(id);
					  String name_  = this.name.get(id);
					  String site_  = this.site.get(id);
					  this.collector.emit(new Values(id, name_, site_, currentTime, aggr_value_bottom, this.duration, this.field, this.operation));
				  }
			  }
			  else if (this.operation.equals("SUM")) {
				  Map<Integer, Integer> sum_snapshot;
				  
				  synchronized (this.sum) {
					  sum_snapshot = new HashMap<Integer, Integer>(this.sum);
					  this.sum.clear();
				  }
				  
				  long currentTime = System.currentTimeMillis();
				  Set<Integer> keys = sum_snapshot.keySet();
				  for (Integer id : keys) {
					  Integer aggr_value_sum = sum_snapshot.get(id);
					  String name_  = this.name.get(id);
					  String site_  = this.site.get(id);
					  this.collector.emit(new Values(id, name_, site_, currentTime, aggr_value_sum, this.duration, this.field, this.operation));
				  }
			  }

		  }
		  else if (this.field.equals("upTime")) {
			  
			  if (this.operation.equals("TOP")) {
				  Map<Integer, Integer> top_snapshot;
				  
				  synchronized (this.top) {
					  top_snapshot = new HashMap<Integer, Integer>(this.top);
					  this.top.clear();
				  }
				  
				  long currentTime = System.currentTimeMillis();
				  Set<Integer> keys = top_snapshot.keySet();
				  for (Integer id : keys) {
					  Integer aggr_value_top = top_snapshot.get(id);
					  String name_  = this.name.get(id);
					  String site_  = this.site.get(id);
					  this.collector.emit(new Values(id, name_, site_, currentTime, aggr_value_top, this.duration, this.field, this.operation));
				  }
			  }
			  
		  }
	  }
  }
}