package uoa.di.ds.storm.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uoa.di.ds.storm.utils.network.SocketClient;

public class TCPSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
    protected static final Logger LOG = LoggerFactory.getLogger(TCPSpout.class);
	
	private Map _config;
	private TopologyContext _context;
	private SpoutOutputCollector _collector;
	private SocketClient client;
	
	public TCPSpout(String hostName, int port) {
		client = new SocketClient(hostName, port);
	}
	
	@Override
	public void nextTuple() {
		String record = client.getNextResponse();
		_collector.emit(new Values(record));
		LOG.trace("Emitting Record=[{}]",record);
	}

	@Override
	public void open(Map topologyConfig, TopologyContext topologyContext, SpoutOutputCollector collector) {
		this._config = topologyConfig;
		this._context = topologyContext;
		this._collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(uoa.di.ds.storm.utils.Cons.TUPLE_VAR_MSG));
	}
	
}
