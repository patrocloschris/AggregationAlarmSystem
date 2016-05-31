package uoa.di.ds.storm.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class AggregationCassandraBolt extends BaseRichBolt{

	private static final long serialVersionUID = 1L;
    private Map conf= null;
    private TopologyContext context = null;
    private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
	      this.conf = stormConf;
	      this.context = context;
	      this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
