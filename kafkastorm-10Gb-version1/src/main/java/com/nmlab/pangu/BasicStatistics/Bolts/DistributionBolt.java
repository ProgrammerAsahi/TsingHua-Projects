package main.java.com.nmlab.pangu.BasicStatistics.Bolts;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import main.java.com.nmlab.pangu.BasicStatistics.Helpers.Flow;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DistributionBolt implements IBasicBolt{

	private BasicOutputCollector outputCollector;
	public long time = 0;
	static long slots = 0;
	public ArrayList<Flow> flow;
	public FileWriter fw ;
	public int flowIndex=-1;
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("timestamp","throughput","countPacket"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		this.flow = new ArrayList<Flow>();
		flow.clear();
		try {
			  fw = new FileWriter("/home/pangu/distribution");
		} catch (FileNotFoundException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
		// TODO Auto-generated method stub

	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		outputCollector = collector;
		outputCollector.setContext(tuple);  
		flowIndex=-1;
		Flow temp = new Flow();
		temp.srcIP = (String) tuple.getValueByField("src");
		temp.dstIP = (String) tuple.getValueByField("dst");
		//fw.write(flow.size()+"\r\n");
		System.out.println("src:"+temp.srcIP);
		System.out.println("dst:"+temp.dstIP);
		System.out.println("flow.size():"+flow.size());
		for(int i=0;i<flow.size();i++)
		{
			if(flow.get(i).srcIP.equals(temp.srcIP) && flow.get(i).dstIP.equals(temp.dstIP)){
			flowIndex=i;
			break;	
			}
		}
	    //int flowIndex = FindFlow(this.flow,temp.srcIP,temp.dstIP);
		try {
			if(time == 0) 
				time = (Long) tuple.getValueByField("sec");
			if((Long) tuple.getValueByField("sec") - time >= 1)
			{
				if(flowIndex==-1){
					 temp.length =  (Integer)tuple.getValueByField("len");
					 temp.count = 1;
					 flow.add(temp);
				}
				else {
					flow.get(flowIndex).count ++;
					flow.get(flowIndex).length += (Integer) tuple.getValueByField("len");
				}
				for(int i=0;i<flow.size();i++)
				{
					try {
						fw.write(tuple.getValueByField("sec")+" "+(flow.get(i).count)+" "+flow.get(i).length+" "+flow.get(i).srcIP+" "+flow.get(i).dstIP+"\r\n");						
					} catch (IOException e) {
						// TODO 自动生成的 catch 块
						e.printStackTrace();
					}
				}
				flow.clear();
				time = (Long) tuple.getValueByField("sec");
			}
			else {
				if(flowIndex==-1){
					 temp.length = (Integer) tuple.getValueByField("len");
					 temp.count = 1;
					 flow.add(temp);
				}
				else {
					flow.get(flowIndex).count ++;
					flow.get(flowIndex).length += (Long) tuple.getValueByField("len");
				}
			}
        } catch(FailedException e) {
	    	System.out.println("fail to deal with packet");
	    }  
	}
	
	/*int FindFlow(ArrayList<Flow> flow,String src,String dst)
	{
		int i;
		for(i=0;i<flow.size();i++)
		{
			if(flow.get(i).srcIP.equals(src) && flow.get(i).dstIP.equals(dst)) 
				return i;
		}
		return -1;
	}*/
	
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
