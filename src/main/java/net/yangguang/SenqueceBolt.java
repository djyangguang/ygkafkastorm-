package net;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
//import redis.clients.jedis.JedisPool;
/**
 * 就是对kafka出来的数据转换成字符串，接下来我们想办法来处理strom清洗之后的数据，我们为了简单就把输出保存到一个文件中，Bolt逻辑SenqueceBolt类的代码如下
 * 就是把输出的消息放到文件kafkastorm.out中
 * @author dj
 *BaseRichBolt
 */
public class SenqueceBolt extends BaseRichBolt  {
	private OutputCollector _collector;
	JedisCluster jedisCluster;
	//logInfoHandler loginfohandler;
	//JedisPool pool;
    public void execute(Tuple arg0, BasicOutputCollector arg1) {
        String word = (String) arg0.getValue(0);
        String out = "output:" + word;
        System.out.println(out);
        String orderInfo = arg0.getString(0);
		ordersBean order = logInfoHandler.getOrdersBean(orderInfo);
        System.out.println("=++++"+order.getTotalPrice()+order.getMerchantName());
        	//Jedis jedis = pool.getResource();
      		//jedis.zincrby("orderAna:topSalesByMerchant", order.getTotalPrice(), order.getMerchantName());
      	//  RedisApi.lpush("yg", new String[] {order.getTotalPrice()+"", order.getMerchantName()+""});
        //写文件
        try {
            DataOutputStream out_file = new DataOutputStream(new FileOutputStream("D://"+this+".txt"));
            out_file.writeUTF(out);
            out_file.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        arg1.emit(new Values(out));
    }

    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("message"));
    }
//	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
//		this._collector = collector;
//		logInfoHandler log= new logInfoHandler();
//		this.pool = new JedisPool(new JedisPoolConfig(), "ymhHadoop",6379,2 * 60000,"12345");
//		
//		
//	}

	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		JedisPoolConfig poolConfig = new JedisPoolConfig();
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        HostAndPort hostAndPort = new HostAndPort("192.168.62.128", 7000);
        HostAndPort hostAndPort1 = new HostAndPort("192.168.62.128", 7001);
        HostAndPort hostAndPort2 = new HostAndPort("192.168.62.128", 7002);
        HostAndPort hostAndPort3 = new HostAndPort("192.168.62.131", 7003);
        HostAndPort hostAndPort4 = new HostAndPort("192.168.62.131", 7004);
        HostAndPort hostAndPort5 = new HostAndPort("192.168.62.131", 7005);
        nodes.add(hostAndPort);
        nodes.add(hostAndPort1);
        nodes.add(hostAndPort2);
        nodes.add(hostAndPort3);
        nodes.add(hostAndPort4);
        nodes.add(hostAndPort5);
         jedisCluster = new JedisCluster(nodes, poolConfig);
		String word = (String) arg0.getValue(0);
        String out = "output:" + word;
        System.out.println(out);
        String orderInfo = arg0.getString(0);
		ordersBean order = logInfoHandler.getOrdersBean(orderInfo);
        System.out.println("=++++"+order.getTotalPrice()+order.getMerchantName());
        jedisCluster.zincrby("orderAna:topSalesByMerchant", order.getTotalPrice(), order.getMerchantName());
		System.out.println(jedisCluster.zrange("orderAna:topSalesByMerchant", 0, -1));
		
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		JedisPoolConfig poolConfig = new JedisPoolConfig();
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        HostAndPort hostAndPort = new HostAndPort("192.168.62.128", 7000);
        HostAndPort hostAndPort1 = new HostAndPort("192.168.62.128", 7001);
        HostAndPort hostAndPort2 = new HostAndPort("192.168.62.128", 7002);
        HostAndPort hostAndPort3 = new HostAndPort("192.168.62.131", 7003);
        HostAndPort hostAndPort4 = new HostAndPort("192.168.62.131", 7004);
        HostAndPort hostAndPort5 = new HostAndPort("192.168.62.131", 7005);
        nodes.add(hostAndPort);
        nodes.add(hostAndPort1);
        nodes.add(hostAndPort2);
        nodes.add(hostAndPort3);
        nodes.add(hostAndPort4);
        nodes.add(hostAndPort5);
         jedisCluster = new JedisCluster(nodes, poolConfig);//JedisCluster中默认分装好了连接池.
		
	}
}