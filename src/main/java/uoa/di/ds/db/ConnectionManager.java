package uoa.di.ds.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;

public class ConnectionManager {

	private static ConnectionManager instance = null;
	private static Object lock;
	private Cluster cluster = null;

	private ConnectionManager(String host) {
		Builder clusterBulder = Cluster.builder();
		String possibleHosts[] = host.split(",");
		for(String h  : possibleHosts){
			clusterBulder.addContactPoint(h);
		}
		cluster = clusterBulder.build();
	}

	public static ConnectionManager getInstance() {
		return instance;
	}

	public static void init(String host) {
		synchronized (lock) {
			if (instance == null) {
				instance = new ConnectionManager(host);
			}
		}
	}

	public Cluster getCluster() {
		return cluster;
	}

}
