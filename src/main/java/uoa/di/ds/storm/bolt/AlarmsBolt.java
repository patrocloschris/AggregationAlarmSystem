package uoa.di.ds.storm.bolt;

import java.util.ArrayList;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
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

//	public static final String CASSANDRA_A_KEYSPACE = "cassandra.alarms.keyspace";
//	public static final String CASSANDRA_AM_TABLE = "cassandra.alarms.alarms_meta";       // Is that correct?

    public AlarmsBolt(String keyspace) {
    	this.keyspace = keyspace;
    	this.rulesList = new ArrayList<String>();
    }
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
	      this.conf = stormConf;
	      this.context = context;
	      this.collector = collector;
    	  ConnectionManager.init(cassandraHostname, clusterName);
    	  session = ConnectionManager.getInstance().getCluster().connect(keyspace);
    	  LOG.info("Preparing alarms bolt....Connection for DB was=[{}]",session.getLoggedKeyspace());
    	  
    	  //Read Rules from metadata table
  		  ResultSet results = session.execute("SELECT * FROM " + Cons.CASSANDRA_AM_TABLE);
    	  for (Row row : results) {

    		//e.g. rule: field = "cpu" / rule=" >" / value= "90"
			String field = row.getString("field") == null ? "field" : row.getString("field");
			String rule = row.getString("rule") == null ? "rule" : row.getString("rule");
			int value = row.getInt("value") == 0 ? 10 : row.getInt("value");
			
			String alarm_rule_tuple = field+","+rule+","+Integer.toString(value);
			rulesList.add(alarm_rule_tuple);										//Each rule in the arraylist has the form: field,rule,value
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
			
			String field =  tokens[0];
			String rule  =  tokens[1];
			String value =  tokens[2];
			
			
			if (field.equals(input.getStringByField(Cons.TUPLE_VAR_FIELD))) { // Field match
		
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields(Cons.TUPLE_VAR_MO, Cons.TUPLE_VAR_NOTIF, Cons.TUPLE_VAR_ADDTEXT, Cons.TUPLE_VAR_STATE));        
	}

}
