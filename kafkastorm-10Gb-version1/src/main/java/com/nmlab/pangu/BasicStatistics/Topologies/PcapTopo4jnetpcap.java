
package main.java.com.nmlab.pangu.BasicStatistics.Topologies;
import java.util.HashMap;

import main.java.com.nmlab.pangu.BasicStatistics.Spouts.*;
import main.java.com.nmlab.pangu.BasicStatistics.Bolts.*;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import backtype.storm.StormSubmitter;
import main.java.com.nmlab.pangu.BasicStatistics.Helpers.KafkaProperties;


public class PcapTopo4jnetpcap {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		 Config conf = new Config();
		 if (args == null || args.length == 0) {
			 conf.put("storm.zookeeper.port", 12181);			 
			 builder.setSpout("PcapSpout4jnetpcap4", new PcapSpout4jnetpcap(null,-1,null,"F://My Software//pcap//oneday_No.7_201707210600.pcap",null,-1,"topic15"), 1);
			 builder.setBolt("PortDistributionBolt", new PortDistributionBolt(),1).allGrouping("PcapSpout4jnetpcap4","http");
			 conf.setNumWorkers(1);
			 LocalCluster cluster = new LocalCluster();
			 cluster.submitTopology("PcapTopo4jnetpcap", conf, builder.createTopology()); 			
		     Utils.sleep(1000000);
			 cluster.killTopology("PcapTopo4jnetpcap");
			 cluster.shutdown();
		 }
		 else{
			 builder.setSpout("PcapSpout4jnetpcap1", new PcapSpout4jnetpcap(null,-1,null,"F://My Software//pcap//oneday_No.7_201707210100.pcap",null,-1,"topic2"), 1);
			 builder.setSpout("PcapSpout4jnetpcap2", new PcapSpout4jnetpcap(null,-1,null,"F://My Software//pcap//oneday_No.7_201707210100.pcap",null,-1,"topic5"), 1);
			 builder.setSpout("PcapSpout4jnetpcap3", new PcapSpout4jnetpcap(null,-1,null,"F://My Software//pcap//oneday_No.7_201707210100.pcap",null,-1,"topic17"), 1);
			 builder.setSpout("PcapSpout4jnetpcap4", new PcapSpout4jnetpcap(null,-1,null,"F://My Software//pcap//oneday_No.7_201707210100.pcap",null,-1,"topic15"), 1);
			 builder.setBolt("PortDistributionBolt1", new PortDistributionBolt(),1).allGrouping("PcapSpout4jnetpcap1","http");
			 builder.setBolt("PortDistributionBolt2", new PortDistributionBolt(),1).allGrouping("PcapSpout4jnetpcap2","http");
			 builder.setBolt("PortDistributionBolt3", new PortDistributionBolt(),1).allGrouping("PcapSpout4jnetpcap3","http");
			 builder.setBolt("PortDistributionBolt4", new PortDistributionBolt(),1).allGrouping("PcapSpout4jnetpcap4","http");
			 
			 
        	 try{
        		 StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        	 }catch (InvalidTopologyException e ){
        		 e.printStackTrace();
        	 } catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}        	 
		 }	           
	}
}
