package uoa.di.ds.storm.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uoa.di.ds.db.ConnectionManager;
import uoa.di.ds.storm.utils.Cons;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class AlarmsBolt extends BaseRichBolt{

	private static final long serialVersionUID = 1L;
	
	private static final Logger LOG = LoggerFactory.getLogger(AlarmsBolt.class);
	
    private Map conf= null;
    private TopologyContext context = null;
    private OutputCollector collector = null;
    private String cassandraHostname = null; 
    private String clusterName=null;
	private Session session;
	private String keyspace;
	private ArrayList<String> rulesList;
	private String metaTable;
	private Map<String , HashSet<Integer>> active_notifs;

//	public static final String CASSANDRA_A_KEYSPACE = "cassandra.alarms.keyspace";
//	public static final String CASSANDRA_AM_TABLE = "cassandra.alarms.alarms_meta";       // Is that correct?

    public AlarmsBolt(String keyspace,String metaTable) {
    	LOG.info("Creating Bolt for Keyspace=[{}]",keyspace);
    	this.keyspace = keyspace;
    	this.metaTable = metaTable;
    	this.rulesList = new ArrayList<String>();
    }
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
	      this.conf = stormConf;
	      this.context = context;
	      this.collector = collector;
	      this.active_notifs = new HashMap<>();
	      LOG.info("Preparing Bolt...");	
	      
    	  ConnectionManager.init(cassandraHostname, clusterName);
    	  session = ConnectionManager.getInstance().getCluster().connect(keyspace);
    	  LOG.info("Preparing alarms bolt....Connection for DB was=[{}]",session.getLoggedKeyspace());
    	  
    	  //Read Rules from metadata table
  		  ResultSet results = session.execute("SELECT * FROM " + metaTable);
    	  for (Row row : results) {

    		//e.g. rule: field = "cpu" / rule=" >" / value= "90"
			String field = row.getString("field") == null ? "field" : row.getString("field");
			String operation = row.getString("operation") == null ? "operation" : row.getString("operation");
			String rule = row.getString("rule") == null ? "rule" : row.getString("rule");
			int value = row.getInt("value") == 0 ? 10 : row.getInt("value");
			
			String alarm_rule_tuple = field+","+operation+","+rule+","+Integer.toString(value);
			LOG.info("Adding rule=[{}]",alarm_rule_tuple);
			rulesList.add(alarm_rule_tuple);										//Each rule in the arraylist has the form: field,operation,rule,value
		}
	}
	
	public void withHostName(String name){
		this.cassandraHostname = name;
	}

	public void withClusterName(String clusterName){
		this.clusterName = clusterName;
	}
	
	@Override
	public void execute(Tuple input) {
		
		for (int i=0; i<=rulesList.size(); i++) {
			String alarm_rule = rulesList.get(i);	
			
			String[] tokens = alarm_rule.split(",");
			int j=0;
			
			String field 	 = tokens[0];
			String operation = tokens[1];
			String rule  	 = tokens[2];
			String value 	 = tokens[3];
			
			
			if (field.equals(input.getStringByField(Cons.TUPLE_VAR_FIELD))) { // Field match
				if (operation.equals(input.getStringByField(Cons.TUPLE_VAR_OPER))) {
					
					if (rule.equals(">")) {
						if (input.getIntegerByField(Cons.TUPLE_VAR_VALUE) > Integer.parseInt(value)) {
							Integer notif = generateNotifId(field, operation, rule);
							String add_text = new String(field+ " " + operation+"("+Integer.toString(input.getIntegerByField(Cons.TUPLE_VAR_VALUE))+")" + " exceeded allowed limit of "+ value);
							generateActiveAlarm(input.getStringByField(Cons.TUPLE_VAR_NAME), notif, add_text, input.getLongByField(Cons.TUPLE_VAR_EVENTTIME));
						}
						else {
							Integer notif = generateNotifId(field, operation, rule);
							String add_text = new String(field+ " " + operation+"("+Integer.toString(input.getIntegerByField(Cons.TUPLE_VAR_VALUE))+")" + " is now below limit of "+ value);
							generateClearAlarm(input.getStringByField(Cons.TUPLE_VAR_NAME), notif, add_text, input.getLongByField(Cons.TUPLE_VAR_EVENTTIME));
					
						}
					}
					else if (rule.equals("<")) {
						if (input.getIntegerByField(Cons.TUPLE_VAR_VALUE) < Integer.parseInt(value)) {
							Integer notif = generateNotifId(field, operation, rule);
							String add_text = new String(field+ " " + operation+"("+Integer.toString(input.getIntegerByField(Cons.TUPLE_VAR_VALUE))+")" + " descended below allowed limit of "+ value);
							generateActiveAlarm(input.getStringByField(Cons.TUPLE_VAR_NAME), notif, add_text, input.getLongByField(Cons.TUPLE_VAR_EVENTTIME));
						}
						else {
							Integer notif = generateNotifId(field, operation, rule);
							String add_text = new String(field+ " " + operation+"("+Integer.toString(input.getIntegerByField(Cons.TUPLE_VAR_VALUE))+")" + " is now above limit of "+ value);
							generateClearAlarm(input.getStringByField(Cons.TUPLE_VAR_NAME), notif, add_text, input.getLongByField(Cons.TUPLE_VAR_EVENTTIME));
						}
					}
				/*	else if (rule.equals(">=")) {
						if (input.getIntegerByField(Cons.TUPLE_VAR_VALUE) >= Integer.parseInt(value))
							generateActiveAlarm();
						else
							generateClearAlarm();
					}
					else if (rule.equals("<=")) {
						if (input.getIntegerByField(Cons.TUPLE_VAR_VALUE) <= Integer.parseInt(value))
							generateActiveAlarm();
						else
							generateClearAlarm();
					}						
					else if (rule.equals("==")) {
						if (input.getIntegerByField(Cons.TUPLE_VAR_VALUE) == Integer.parseInt(value))
							generateActiveAlarm();
						else
							generateClearAlarm();
					} */
						
							   		
						
					
				}
			}
		}
		
	    collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields(Cons.TUPLE_VAR_MO, Cons.TUPLE_VAR_NOTIF, Cons.TUPLE_VAR_ADDTEXT, Cons.TUPLE_VAR_EVENTTIME, Cons.TUPLE_VAR_STATE));        
	}
	
	private Integer generateNotifId(String field, String operation, String rule) {
		
		return (field+operation+rule).hashCode();
	}
	
	private void generateActiveAlarm(String mo, Integer notif_id, String add_text, Long eventtime) {
		
		HashSet<Integer> s = active_notifs.get(mo);
		
		if (s == null) {
			HashSet<Integer> notifs = new HashSet<Integer>();
			notifs.add(notif_id);
			LOG.info("Active First Alarm for mo=[{}] with notificationID=[{}] and text=[{}]",mo,notif_id,add_text);
			this.collector.emit(new Values(mo, notif_id, add_text, eventtime, "active"));
			active_notifs.put(mo, notifs);
		}
		else {
			if (!s.contains(notif_id)) {
				s.add(notif_id);
				LOG.info("Active Alarm for mo=[{}] with notificationID=[{}] and text=[{}]",mo,notif_id,add_text);
				this.collector.emit(new Values(mo, notif_id, add_text, eventtime, "active"));
			}
			//If set of active notifs for this mo contains the specified id do nothing!
		}
	}
	
	private void generateClearAlarm(String mo, Integer notif_id, String add_text, Long eventtime) {
		HashSet<Integer> s = active_notifs.get(mo);
		
		if (s != null) {
			if (s.contains(notif_id)) {
				LOG.info("Clear Alarm for mo=[{}] with notificationID=[{}] and text=[{}]",mo,notif_id,add_text);
				this.collector.emit(new Values(mo, notif_id, add_text, eventtime,"clear"));
				s.remove(notif_id);  //Remove active
			}
		}
	}

}
