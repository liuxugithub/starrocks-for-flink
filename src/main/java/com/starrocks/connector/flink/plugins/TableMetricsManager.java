package com.starrocks.connector.flink.plugins;

public class TableMetricsManager {
    public static final String genTableId(String dataBase,String tableName){
        return dataBase+"-"+tableName;
    }
}
