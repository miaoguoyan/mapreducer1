package com.baizhi.demo10;

import org.apache.hadoop.conf.Configuration;

public class RedisConfiguration {
    private Configuration conf;
    public RedisConfiguration(Configuration conf) {
        this.conf = conf;
    }
    public static final String REDIS_HOST="redis.host";
    public static final String REDIS_PORT="redis.port";
    public static final String REDIS_DESCRIPT_KEY="redis.descriptKey";

    public static void configRedis(Configuration conf,String host,int port,String descriptKey){
        conf.set(REDIS_HOST,host);
        conf.setInt(REDIS_PORT,port);
        conf.set(REDIS_DESCRIPT_KEY,descriptKey);
    }

    public String getHost(){
        return conf.get(REDIS_HOST);
    }
    public int getPort(){
        return conf.getInt(REDIS_PORT,6379);
    }
    public String getDescriptKey(){
        return conf.get(REDIS_DESCRIPT_KEY);
    }


}
