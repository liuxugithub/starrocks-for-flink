package com.starrocks.connector.flink.plugins;

import com.alibaba.fastjson.JSON;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.shaded.netty4.io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
/**
 * flushBytes: flush to starrocks total rows
 * flushBytes:  flush to starrocks total bytes
 * totalReceived:  received from mysql total rows
 * insertReceivedCount:  received insert  event from mysql total rows
 * updateReceivedCount:  received update event from mysql total rows
 * deleteReceivedCount:  received delete event from mysql total rows
 * replaceReceivedCount:  received replace event from mysql total rows
 * alterReceivedCount:  received alter event from mysql total rows
 */
public class RedisPlugin {
    private static final Logger log = LoggerFactory.getLogger(RedisPlugin.class);
    public static final String KEY_Prefix="flink:cache:database_sync::";
    private static List<String> hosts;
    private static String pwd;
    private JedisCluster cluster;
    private static RedisPlugin redisCluster;
    public static final void init(StarRocksSinkOptions readableConfig){
        log.info("redis init :{}", JSON.toJSONString(readableConfig));
        if(!StringUtil.isNullOrEmpty(readableConfig.getRedisHosts())){
            hosts= Arrays.stream(readableConfig.getRedisHosts().split(",")).collect(Collectors.toList());
            pwd=readableConfig.getRedisPassWord();
            redisCluster=new RedisPlugin(hosts,pwd);
        }
    }
    public static final RedisPlugin getInstance(){
        if(redisCluster==null){
            synchronized (RedisPlugin.class){
                if(redisCluster==null&&hosts!=null){
                    redisCluster=new RedisPlugin(hosts,pwd);
                }
            }
        }
        log.info("redisCluster:{},hosts:{}",redisCluster,hosts);
        return redisCluster;
    }
    public void close(){
        try {
            if(this.cluster!=null){
                this.cluster.close();
            }
        } catch (IOException e) {
            log.error("redis close error:",e);
        }finally {
            this.cluster=null;
        }
    }
    private RedisPlugin(List<String> hosts,String pwd) {
        log.info("hosts:{},pwd:{}",hosts,pwd);
        if(hosts!=null && hosts.size()>0){
            Set<HostAndPort> nodes = new HashSet<>();
            for(String host:hosts){
                String[] ipAndPort =  host.split(":");
                nodes.add(new HostAndPort(ipAndPort[0], Integer.valueOf(ipAndPort[1])));
            }
            JedisPoolConfig jedisPoolConfig =  new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(20);
            jedisPoolConfig.setTestOnBorrow(true);
            jedisPoolConfig.setTestOnReturn(true);
            jedisPoolConfig.setMaxWaitMillis(30 * 1000);
            jedisPoolConfig.setTimeBetweenEvictionRunsMillis(10 * 1000);
            jedisPoolConfig.setMinEvictableIdleTimeMillis(30 * 1000);
            jedisPoolConfig.setMaxIdle(2);
            jedisPoolConfig.setMinIdle(1);
            this.cluster = new JedisCluster(nodes,10000,20000,
                    5, StringUtil.isNullOrEmpty(this.pwd)?null:pwd,jedisPoolConfig);
        }
    }
    public void increment(String tableId,String filedName,long size){
        this.cluster.hincrBy(KEY_Prefix+tableId,filedName,size);
    }
    public  final void collectMetrics(String tableId, Long flushRows,Long flushBytes){
        if(flushBytes!=null) {
            this.increment(tableId,"flushBytes",flushBytes);
        }
        if(flushRows!=null) {
            this.increment(tableId,"flushRows",flushRows);
        }
    }
    public  final void  receivedCountIncrement(String tableId){
        this.increment(tableId,"totalReceived",1);
    }
    public  final void  receivedInsertCountIncrement(String tableId){
        this.increment(tableId,"insertReceivedCount",1);
    }
    public  final void  receivedUpdateCountIncrement(String tableId){
        this.increment(tableId,"updateReceivedCount",1);
    }
    public  final void  receivedReplaceCountIncrement(String tableId){
        this.increment(tableId,"replaceReceivedCount",1);
    }
    public  final void  receivedAlterCountIncrement(String tableId){
        this.increment(tableId,"alterReceivedCount",1);
    }
    public  void  receivedDeleteCountIncrement(String tableId){
        this.increment(tableId,"deleteReceivedCount",1);
    }
}
