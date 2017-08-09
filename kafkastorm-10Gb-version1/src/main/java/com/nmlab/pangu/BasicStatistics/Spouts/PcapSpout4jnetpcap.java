
package main.java.com.nmlab.pangu.BasicStatistics.Spouts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.java.com.nmlab.pangu.BasicStatistics.Helpers.CreateValue;

import org.jnetpcap.Pcap;
import org.jnetpcap.PcapHeader;
import org.jnetpcap.PcapIf;
import org.jnetpcap.nio.JMemory;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.PcapPacket;
import org.jnetpcap.packet.PcapPacketHandler;
import org.jnetpcap.protocol.application.Html;
import org.jnetpcap.protocol.network.Ip4;
import org.jnetpcap.protocol.network.Ip6;
import org.jnetpcap.protocol.tcpip.Http;
import org.jnetpcap.protocol.tcpip.Tcp;
import org.jnetpcap.protocol.tcpip.Udp;
import org.jsoup.Jsoup;
import org.jnetpcap.nio.JBuffer;
import org.jnetpcap.packet.format.FormatUtils;
import org.jnetpcap.packet.JRegistry;
import org.jsoup.nodes.*;
import org.jsoup.select.*;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import main.java.com.nmlab.pangu.BasicStatistics.Helpers.KafkaProperties;

import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class PcapSpout4jnetpcap implements IRichSpout {

	private SpoutOutputCollector outputCollector;
	PcapIf device;
	Pcap pcap; 
	StringBuilder errbuf = new StringBuilder(); // For any error msgs
	private CreateValue create;
	
	
	//start capture
	private String deviceName = null;
	private int count = -1;
	private String filter = null;
	private String srcFilename =null ;
	private String dstFilename = null;
	private int sampLen = 64*1024;
	public int countPacket = 0;
	private int flags = Pcap.MODE_PROMISCUOUS; // capture all packets
	private int timeout = 10 ; // 10 seconds in millis		

	private ConsumerConnector consumer;	
    private String topic=null;
    private Map<String, Integer> topicCountMap;
    private Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap ;
    private KafkaStream<byte[], byte[]> stream ;
    private ConsumerIterator<byte[], byte[]> it;
    
    
    private static ConsumerConfig createConsumerConfig()
    {
        Properties props = new Properties();
        props.put("zookeeper.connect", KafkaProperties.zkConnect);
        props.put("group.id", "test-consumer-group");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }			
	//test
	public Ip4 ip4;
	public Ip6 ip6;
	public Http http;
	public Html html;
	public PcapHeader hdr;
	public JBuffer buf;
	public String src=null;
	public String dst=null;
	int id;
	long time = 0;
	 long slots = 0;
	 long throughput = 0;
	PcapPacket packet;
    public PcapSpout4jnetpcap(){};       
    public PcapSpout4jnetpcap(String deviceName, int count, String filter, String srcFilename, String dstFilename, int sampLen,String topic){
    	this.deviceName = deviceName;
    	this.count = count; 
    	this.filter = filter;
    	this.srcFilename = srcFilename;
    	this.dstFilename = dstFilename;
    	if(sampLen<0)
    		this.sampLen = 64*1024;
    	this.sampLen = sampLen;
    	this.topic=topic;
    }
    public PcapSpout4jnetpcap(String deviceName, String count, String filter, String srcFilename, String dstFilename, String sampLen,String topic){
    	this.deviceName = deviceName;
    	this.count = Integer.parseInt(count); 
    	this.filter = filter;
    	this.srcFilename = srcFilename;
    	this.dstFilename = dstFilename;
    	int slen=0;
    	if(Integer.parseInt(sampLen)<0)
            slen = 64*1024;// Capture all packets, no trucation
    	this.sampLen = slen;    	
    	this.topic=topic;
    }

    PcapIf getDevice(){
    	List<PcapIf> alldevs = new ArrayList<PcapIf>(); // Will be filled with NICs
    	int r = Pcap.findAllDevs(alldevs, errbuf);
		if (r == Pcap.NOT_OK || alldevs.isEmpty()) {
			System.err.printf("Can't read list of devices, error is %s", errbuf.toString());
			return null;
		}
		int i = 0,chooseid=0;
		for (PcapIf device : alldevs) {
			String description =(device.getDescription() != null) ? device.getDescription(): "No description available";
			if(deviceName!=null && deviceName!="" && device.getName().equals(deviceName))
				chooseid=i;
			System.out.printf("#%d: %s [%s]\n", i++, device.getName(), description);
		}
		return alldevs.get(chooseid);
    }
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector spoutOutputCollector) {
		// TODO Auto-generated method stub
		this.outputCollector = spoutOutputCollector;		
        try {         	
        /*	  this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                    createConsumerConfig());
        	this.topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(topic, new Integer(1));
            this.consumerMap = consumer.createMessageStreams(topicCountMap);
            this.stream = consumerMap.get(topic).get(0);
            this.it = stream.iterator(); */
        	
        	//open device
        	if(srcFilename!=null){
        		pcap=Pcap.openOffline(srcFilename, errbuf);
        		if (pcap == null) {
        			System.err.printf("Error while opening srcfile  for capture: "+ errbuf.toString());
        			return;
        		}
        		http = new Http();
        		html = new Html();
        		hdr = new PcapHeader(JMemory.POINTER);  
        		buf = new JBuffer(JMemory.POINTER);
        		id = JRegistry.mapDLTToId(pcap.datalink());
        	}
        	else
        	{
        		this.sampLen=64*1024;
        		device = getDevice();        		
        		pcap =Pcap.openLive(device.getName(), this.sampLen, this.flags, this.timeout, errbuf);
        		if (pcap == null) {
        			System.err.printf("Error while opening device for capture: "+ errbuf.toString());
        			return;
        		}
            	http = new Http();
            	html = new Html();
            	hdr = new PcapHeader(JMemory.POINTER);  
                buf = new JBuffer(JMemory.POINTER);
                id = JRegistry.mapDLTToId(pcap.datalink());
        	}        	
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	public void nextTuple() {
		// TODO Auto-generated method stub
		try{
			if(srcFilename!=null)
			{
				PcapPacketHandler<String> packethandler = new PcapPacketHandler<String>(){
					public void nextPacket(PcapPacket packet, String user){
						String js_code="";
						String html_code="";
						ip4 = new Ip4();
						ip6 = new Ip6();
						String destination="";
						String source="";
						js_code="";
						if(packet.hasHeader(http)&&packet.hasHeader(html))
						{
							if(packet.hasHeader(ip4))
							{
								destination = FormatUtils.asString(packet.getHeader(ip4).destination());
								source = FormatUtils.asString(packet.getHeader(ip4).source());
							}
							else if(packet.hasHeader(ip6))
							{
								destination=FormatUtils.asString(ip6.destination());
								source= FormatUtils.asString(ip6.source());
							}
							//此处为js代码提取部分
							JBuffer jbuffer = packet;
							byte[] data_byte=jbuffer.getByteArray(0, packet.size());						
							html_code=new String(data_byte);
							Document doc =Jsoup.parse(html_code,"UTF-8");
							Elements js_elements=doc.getElementsByTag("script");
							if(js_elements.toString().length()>0)
							{
								js_code=js_code+"source:"+source+"\r\n";
								js_code=js_code+"destination:"+destination+"\r\n";
								System.out.println("source:"+source);
								System.out.println("destination:"+destination);
								for(Element js_element:js_elements)
								{
									js_code=js_code+js_element.toString()+"\r\n";
									System.out.println(js_element.toString());
								}
							}
							outputCollector.emit("http",new Values(packet.getCaptureHeader().seconds(),js_code));
						}
					}
				};
				while (pcap.nextEx(hdr, buf) == Pcap.NEXT_EX_OK){
					pcap.loop(1,packethandler,"asahi");
				}
			}
			/*else
			{
				while(this.it.hasNext())
				{
					byte[] dstpacket=it.next().message();
					packet = new PcapPacket(dstpacket);
					if(packet.hasHeader(http)&&packet.hasHeader(html))
					{
						//此处为js代码提取部分
						JBuffer jbuffer = packet;
						byte[] data_byte=jbuffer.getByteArray(0, packet.size());						
						String html_code=new String(data_byte);
						Document doc =Jsoup.parse(html_code);
						Elements js_elements=doc.getElementsByTag("script");
						for(Element i:js_elements)
						{
							String tag=i.tagName();
							if(tag.equals("script"))
								js_code=js_code+"<script>"+i.text()+"</script>"+"\r\n";
						}
						this.outputCollector.emit("http",new Values(packet.getCaptureHeader().seconds(),js_code));
					}
				}
			}*/
		} catch(Exception e) {
			    	System.out.println("fail to deal with packet");
			    }
	}
	
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		// TODO Auto-generated method stub
		outputFieldsDeclarer.declareStream("http",new Fields("sec","js_code"));//后面bolt都依据这个方法来定义字段
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	
	
    	public void ack(Object arg0) {
    		// TODO Auto-generated method stub
    		
    	}

    	public void activate() {
    		// TODO Auto-generated method stub
    		
    	}

    	public void close() {
    		// TODO Auto-generated method stub
    		pcap.close();
    	}

    	public void deactivate() {
    		// TODO Auto-generated method stub
    		
    	}

    	public void fail(Object arg0) {
    		// TODO Auto-generated method stub
    		
    	}

}
