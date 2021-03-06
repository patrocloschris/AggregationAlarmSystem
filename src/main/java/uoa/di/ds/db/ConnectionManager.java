package uoa.di.ds.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;

public class ConnectionManager {

	private static ConnectionManager instance = null;
	private static Object lock = new Object();
	private Cluster cluster = null;

	private ConnectionManager(String host,String clusterName) {
		Builder clusterBulder = Cluster.builder();
		String possibleHosts[] = host.split(",");
		for(String h  : possibleHosts){
			clusterBulder.addContactPoint(h);
		}
		if(clusterName!=null && clusterName.length()>0){
			clusterBulder.withClusterName(clusterName);
		}
		cluster = clusterBulder.build();
	}

	public static ConnectionManager getInstance() {
		return instance;
	}

	public static void init(String host,String clusterName) {
		synchronized (lock) {
			if (instance == null) {
				instance = new ConnectionManager(host,clusterName);
			}
		}
	}

	public Cluster getCluster() {
		return cluster;
	}

}
