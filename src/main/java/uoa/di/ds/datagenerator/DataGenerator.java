package uoa.di.ds.datagenerator;

import java.io.File;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This simple app produces tuples referring to router statistics
 * and appends them to a csv file with the following syntax:
 *
 * @author Emmanouil Angelogiannopoulos (eangelog@di.uoa.gr)
 */

public class DataGenerator {
	
	int _numOutputFiles;
	String[] sites = {"Athens", "Salonica", "Crete"};
	
	public static void main(String[] args) throws Exception {
	
		DataGenerator dg = new DataGenerator();
		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive		
		//dg._numOutputFiles = ThreadLocalRandom.current().nextInt(1, 100 + 1);
		dg._numOutputFiles = 10;
		
		for (int i=0; i<=dg._numOutputFiles;i++) {
	        PrintWriter pw = new PrintWriter(new File("files//stat"+Integer.toString(i)+".csv"));
	        StringBuilder sb = new StringBuilder();
	        
	        int num_rows = ThreadLocalRandom.current().nextInt(100, 1000 + 1);
	        sb.append("CPU (%)");
	        sb.append(',');
	        sb.append("RAM (GB)");
	        sb.append(',');
	        sb.append("activeSessions");
	        sb.append(',');
	        sb.append("upTime (seconds)");
	        sb.append(',');
	        sb.append("ID");
	        sb.append(',');
	        sb.append("Name");
	        sb.append(',');
	        sb.append("Site");
	        sb.append(',');
	        sb.append("Temperature (Celsius)");
	        sb.append('\n');
	        for (int j=0; j<=num_rows;j++) {
	        	Integer cpu = ThreadLocalRandom.current().nextInt(0, 100 + 1);  //%cpu
	        	Integer ram = ThreadLocalRandom.current().nextInt(100, 4096);  //Ram in MB
	        	Integer activeSessions = ThreadLocalRandom.current().nextInt(1, 10 + 1);  //Active sessions
	        	Integer upTime = ThreadLocalRandom.current().nextInt(1, 1728000 + 1);  //Seconds uptime
	        	Integer id = ThreadLocalRandom.current().nextInt(1, 200 + 1);  //200 IDs
	        	String name = new String("router_"+Integer.toString(id));
	        	String site = dg.sites[id % 3];
	        	Integer temperature = ThreadLocalRandom.current().nextInt(25, 50 + 1);  // temperature
	        	
	        	Tuple<Integer,Integer,Integer,Integer,Integer,String,String,Integer> tup = new Tuple<Integer,Integer,Integer,Integer,Integer,String,String,Integer>
	        																						(cpu, ram, activeSessions, upTime, id, name, site, temperature);
	        	
	        	sb.append(tup.toCSVString());
	        }
	        pw.write(sb.toString());
	        pw.close();
		}

	   
	}
}
