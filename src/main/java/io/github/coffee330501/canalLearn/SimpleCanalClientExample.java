package io.github.coffee330501.canalLearn;


import com.alibaba.fastjson2.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;

public class SimpleCanalClientExample {


    public static void main(String args[]) throws InterruptedException, InvalidProtocolBufferException {
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("127.0.0.1", 11111), "example", "", "");

        while (true) {
            connector.connect();
            // 订阅数据库
            connector.subscribe("tnqd.*");
            // 拿100条数据
            Message message = connector.get(100);

            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.isEmpty()) {
                System.out.println("无数据...");
                Thread.sleep(1000);
            } else {
                for (CanalEntry.Entry entry : entries) {
                    // 获取表名
                    String tableName = entry.getHeader().getTableName();
                    // 获取类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    // 获取序列化后的数据
                    ByteString storeValue = entry.getStoreValue();

                    if (Objects.equals(entryType, CanalEntry.EntryType.ROWDATA)) {
                        // 反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        // 事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // 数据集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        for (CanalEntry.RowData rowData : rowDatasList) {
                            JSONObject beforeData = new JSONObject();
                            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                            for (CanalEntry.Column column : beforeColumnsList) {
                                beforeData.put(column.getName(), column.getValue());
                            }

                            JSONObject afterData = new JSONObject();
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                            for (CanalEntry.Column column : afterColumnsList) {
                                afterData.put(column.getName(), column.getValue());
                            }

                            System.out.println("Table: " + tableName + ",EventType: " + eventType + ",Before: " + beforeData + ",After: " + afterData);
                        }
                    } else {
                        System.out.println("当前操作类型为: " + entryType);
                    }
                }
            }
        }
    }
}