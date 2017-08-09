
package main.java.com.nmlab.pangu.BasicStatistics.Bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class expbolt4jentpcap implements IBasicBolt{

	private BasicOutputCollector outputCollector;
	public long pktlen = 0;
	public long countPacket = 0;
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("countPacket"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		outputCollector = collector;
		outputCollector.setContext(tuple);  
		try {
			countPacket++;
			pktlen = (Long) tuple.getValueByField("len");
			System.out.printf("PacketCount:%d\n",countPacket);
			System.out.printf("PacketLength:"+pktlen);
            
        } catch(FailedException e) {
 
	    	System.out.println("fail to deal with packet");
	    }  

	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
