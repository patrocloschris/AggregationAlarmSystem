package uoa.di.ds.storm.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.esotericsoftware.minlog.Log;

import uoa.di.ds.db.ConnectionManager;
import uoa.di.ds.storm.utils.Cons;

public class AggregationCassandraBolt extends BaseRichBolt{

	private static final Logger LOG = LoggerFactory.getLogger(AggregationCassandraBolt.class);

	private static final long serialVersionUID = 1L;
	
    private Map _conf= null;
    private TopologyContext _context = null;
    private OutputCollector _collector = null;
    private String cassandraHostname = null;
    private String clusterName=null;
    private boolean batchMode = false;
    private int batchSize = 10;
	private ArrayList<Tuple> tupleList ;
	private Session session;
	private String keyspace;
	
    public AggregationCassandraBolt(String keyspace) {
    	this.tupleList = new ArrayList<Tuple>();
    	this.keyspace = keyspace;
    	LOG.info("Creating bolt for Keyspace=[{}]",keyspace);
    }
    
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
	      this._conf = stormConf;
	      this._context = context;
	      this._collector = collector;
    	  ConnectionManager.init(cassandraHostname,clusterName);
    	  session = ConnectionManager.getInstance().getCluster().connect(keyspace);
    	  LOG.info("Preparing bolt....Connection on DB=[{}]",session.getLoggedKeyspace());
	}

	@Override
	public void execute(Tuple input) {
		if(batchMode){
			tupleList.add(input);
			if(batchSize == tupleList.size()){
				insertBatchToCassandra( tupleList);
				tupleList.clear();
			}
		} else {
			insertToCassandra(input);
		}
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	public void withHostName(String name){
		this.cassandraHostname = name;
	}

	public void withClusterName(String clusterName){
		this.clusterName = clusterName;
	}

	public void withBatchMode(boolean mode){
		this.batchMode = mode;
	}

	public void withBatchSize(int size){
		this.batchSize = size;
	}
	
	private void insertToCassandra(Tuple tuple){
		
		Statement statement = QueryBuilder.insertInto(constructTableName(tuple.getStringByField(Cons.TUPLE_VAR_FIELD),
				tuple.getStringByField(Cons.TUPLE_VAR_OPER)))
		        .value("id",tuple.getIntegerByField(Cons.TUPLE_VAR_ID))
		        .value("name", tuple.getStringByField(Cons.TUPLE_VAR_NAME))
		        .value("site", tuple.getStringByField(Cons.TUPLE_VAR_SITE))
		        .value("eventTime", tuple.getLongByField(Cons.TUPLE_VAR_EVENTTIME))
		        .value("value", tuple.getValueByField(Cons.TUPLE_VAR_VALUE))
		        .value("duration", tuple.getIntegerByField(Cons.TUPLE_VAR_DURATION));
		session.execute(statement);
		LOG.info("Insert into DB tuple=[{}]",tuple);
	}

	private void insertBatchToCassandra(List<Tuple> tuples){
		
		BatchStatement batch = new BatchStatement();

		for(Tuple tuple: tuples){
			Statement statement = QueryBuilder.insertInto(constructTableName(tuple.getStringByField(Cons.TUPLE_VAR_FIELD),
					tuple.getStringByField(Cons.TUPLE_VAR_OPER)))
			        .value("id",tuple.getIntegerByField(Cons.TUPLE_VAR_ID))
			        .value("name", tuple.getStringByField(Cons.TUPLE_VAR_NAME))
			        .value("site", tuple.getStringByField(Cons.TUPLE_VAR_SITE))
			        .value("eventTime", tuple.getLongByField(Cons.TUPLE_VAR_EVENTTIME))
			        .value("value", tuple.getFloatByField(Cons.TUPLE_VAR_VALUE))
			        .value("duration", tuple.getIntegerByField(Cons.TUPLE_VAR_DURATION));
			batch.add(statement);
		}
		session.execute(batch);
		LOG.info("Insert into DB multi tuples");
	}
	
	private String constructTableName(String field,String operation){
		return (field.toLowerCase()).concat("_").concat((operation.toLowerCase())).concat("_statistics");
	}
}
