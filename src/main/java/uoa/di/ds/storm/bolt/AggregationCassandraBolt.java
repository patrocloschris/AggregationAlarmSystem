package uoa.di.ds.storm.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import uoa.di.ds.db.ConnectionManager;
import uoa.di.ds.storm.utils.Cons;

public class AggregationCassandraBolt extends BaseRichBolt{

	private static final long serialVersionUID = 1L;
	
	private String field;
    private Map conf= null;
    private TopologyContext context = null;
    private OutputCollector collector = null;
    private String cassandraHostname = null; 
    private boolean batchMode = false;
    private int batchSize = 10;
	private ArrayList<Tuple> tupleList ;
	private Session session;
	private String keyspace;
	
    public AggregationCassandraBolt(String field,String keyspace) {
    	this.tupleList = new ArrayList<Tuple>();
    	this.field = field;
    	this.keyspace = keyspace;
    }
    
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
	      this.conf = stormConf;
	      this.context = context;
	      this.collector = collector;
    	  ConnectionManager.init(cassandraHostname);
    	  session = ConnectionManager.getInstance().getCluster().connect(keyspace);
	}

	@Override
	public void execute(Tuple input) {
		if(batchMode){
			tupleList.add(input);
			if(batchSize == tupleList.size()){
				insertBatchToCassandra("TODO", tupleList);
				tupleList.clear();
			}
		}else{
			insertToCassandra("TODO",input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	public void withHostName(String name){
		this.cassandraHostname = name;
	}

	public void withBatchMode(boolean mode){
		this.batchMode = mode;
	}

	public void withBatchSize(int size){
		this.batchSize = size;
	}
	
	private void insertToCassandra(String table,Tuple tuple){
		Statement statement = QueryBuilder.insertInto(table)
		        .value("id",tuple.getIntegerByField(Cons.TUPLE_VAR_ID))
		        .value("name", tuple.getStringByField(Cons.TUPLE_VAR_NAME))
		        .value("site", tuple.getStringByField(Cons.TUPLE_VAR_SITE))
		        .value("eventTime", tuple.getLongByField(Cons.TUPLE_VAR_EVENTTIME))
		        .value("value", tuple.getFloatByField(Cons.TUPLE_VAR_VALUE))
		        .value("duration", tuple.getIntegerByField(Cons.TUPLE_VAR_DURATION));
		session.execute(statement);		
	}

	private void insertBatchToCassandra(String table,List<Tuple> tuples){
		
		BatchStatement batch = new BatchStatement();

		for(Tuple tuple: tuples){
			Statement statement = QueryBuilder.insertInto(table)
			        .value("id",tuple.getIntegerByField(Cons.TUPLE_VAR_ID))
			        .value("name", tuple.getStringByField(Cons.TUPLE_VAR_NAME))
			        .value("site", tuple.getStringByField(Cons.TUPLE_VAR_SITE))
			        .value("eventTime", tuple.getLongByField(Cons.TUPLE_VAR_EVENTTIME))
			        .value("value", tuple.getFloatByField(Cons.TUPLE_VAR_VALUE))
			        .value("duration", tuple.getIntegerByField(Cons.TUPLE_VAR_DURATION));
			batch.add(statement);
		}
		session.equals(batch);
	}
}
