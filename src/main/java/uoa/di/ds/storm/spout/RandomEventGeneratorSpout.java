package uoa.di.ds.storm.spout;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import uoa.di.ds.storm.utils.Cons;

public class RandomEventGeneratorSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private int rate;
	private String[] sites = {"Athens", "Salonica", "Crete"};
	private Map _config;
	private TopologyContext _context;
	private SpoutOutputCollector _collector;


	public RandomEventGeneratorSpout(int rate) {
		this.rate = rate;
	}
	
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this._config = conf;
		this._context = context;
		this._collector = collector;
	}

	@Override
	public void nextTuple() {
        for (int j=0; j<=rate;j++) {
        	Integer cpu = ThreadLocalRandom.current().nextInt(0, 100 + 1);  //%cpu
        	Integer ram = ThreadLocalRandom.current().nextInt(100, 4096);  //Ram in MB
        	Integer activeSessions = ThreadLocalRandom.current().nextInt(1, 10 + 1);  //Active sessions
        	Integer upTime = ThreadLocalRandom.current().nextInt(1, 1728000 + 1);  //Seconds uptime
        	Integer id = ThreadLocalRandom.current().nextInt(1, 200 + 1);  //200 IDs
        	String name = new String("router_"+Integer.toString(id));
        	String site = sites[id % 3];
        	Integer temperature = ThreadLocalRandom.current().nextInt(25, 50 + 1);  // temperature
    		_collector.emit(new Values(cpu, ram, activeSessions, upTime, id, name, site, temperature));
        }
        try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(
				new Fields(Cons.TUPLE_VAR_CPU, Cons.TUPLE_VAR_RAM, Cons.TUPLE_VAR_ACTIVESESSIONS, Cons.TUPLE_VAR_UPTIME,
						Cons.TUPLE_VAR_ID, Cons.TUPLE_VAR_NAME, Cons.TUPLE_VAR_SITE, Cons.TUPLE_VAR_TEMPERATURE));
	}

}
