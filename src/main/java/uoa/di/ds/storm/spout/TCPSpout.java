package uoa.di.ds.storm.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;

public class TCPSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	
	private Map config;
	private TopologyContext context;
	private SpoutOutputCollector collector;

	@Override
	public void nextTuple() {
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		this.config = arg0;
		this.context = arg1;
		this.collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}


}
