package uoa.di.ds.example.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class PrintBolt extends BaseBasicBolt {


	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		System.out.println("Print recieve tuple: "+tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
	}
}