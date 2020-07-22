package com.lcx.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 李松柏
 * @createTime 2020/7/21 11:03
 * @description hbaseAPI操作
 */
public class HbaseDemo01 {

    //获取configuration对象
    private static final Configuration conf;

    static {
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.88.110");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    //判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {
        //在HBase中管理、访问表需要先创建HBaseAdmin对象
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        //HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.tableExists(tableName);
    }

    //创建表
    public static void createTable(String tableName, String... columnFamliy) throws IOException {
        //HBaseAdmin 过时了
        //HBaseAdmin admin = new HBaseAdmin(conf);
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在！！！");
            System.exit(0);
        } else {
            //创建表属性对象，表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列簇
            for (String cf : columnFamliy) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功!");
        }
    }

    //删除表
    public static void dropTable(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if (isTableExist(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！！！");
        } else {
            System.out.println("表" + tableName + "不存在！！！");
            System.exit(0);
        }
    }

    //向表中插入数据
    public static void addRowData(String tableName, String rowKey, String columfamily, String colum, String value) throws IOException {
        if (isTableExist(tableName)) {
            //创建HTable对象
            HTable hTable = new HTable(conf, tableName);
            //向表中插入数据
            Put put = new Put(Bytes.toBytes(rowKey));
            //向put对象中组装数据
            put.add(Bytes.toBytes(columfamily), Bytes.toBytes(colum), Bytes.toBytes(value));
            hTable.put(put);
            hTable.close();
            System.out.println("插入数据成功");
        } else {
            System.out.println("表" + tableName + "不存在！！！");
            System.exit(0);
        }
    }

    //删除多行数据
    public static void deleteMultRow(String tableName, String... rows) throws IOException {
        if (isTableExist(tableName)) {
            HTable hTable = new HTable(conf, tableName);
            List<Delete> deleteList = new ArrayList<Delete>();
            for (String row : rows) {
                Delete delete = new Delete(Bytes.toBytes(row));
                deleteList.add(delete);
            }
            hTable.delete(deleteList);
            hTable.close();
            System.out.println("删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在！！！");
            System.exit(0);
        }
    }

    //获取某一行数据
    public static void getRow(String tableName, String rowKey) throws IOException {
        if (isTableExist(tableName)) {
            HTable hTable = new HTable(conf, tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            //显示所有版本
            get.setMaxVersions();
            //显示指定时间戳的版本
            //get.setTimeStamp();
            Result result = hTable.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.print("行键："+Bytes.toString(result.getRow())+"\t");
                System.out.print("列簇："+Bytes.toString(CellUtil.cloneFamily(cell))+"\t");
                System.out.print("列："+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t");
                System.out.print("值："+Bytes.toString(CellUtil.cloneValue(cell))+"\t");
                System.out.println("时间戳："+cell.getTimestamp());
            }
        }else {
            System.out.println("表" + tableName + "不存在！");
            System.exit(0);
        }
    }

    //获取表中某一行指定"列簇：列"的数据
    public static void getRowQualifier(String tableName,String rowKey,String columfamily,String qualifier) throws IOException {
        if(isTableExist(tableName)){
            HTable hTable = new HTable(conf, tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setMaxVersions();
            get.addColumn(Bytes.toBytes(columfamily),Bytes.toBytes(qualifier));
            Result result = hTable.get(get);
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.print("行键："+Bytes.toString(result.getRow())+"\t");
                System.out.print("列簇："+Bytes.toString(CellUtil.cloneFamily(cell))+"\t");
                System.out.print("列："+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t");
                System.out.print("值："+Bytes.toString(CellUtil.cloneValue(cell))+"\t");
                System.out.println("时间戳："+cell.getTimestamp());
            }
        }else {
            System.out.println("表" + tableName + "不存在！");
            System.exit(0);
        }
    }


    //获取表中所有数据
    public static void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable得到resultcanner实现类的对象
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //得到rowkey
                System.out.print("行键：" + Bytes.toString(CellUtil.cloneRow(cell)) + "\t");
                //得到列簇
                System.out.print("列簇：" + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t");
                //列
                System.out.print("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t");
                //值
                System.out.println("值：" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    //main
    public static void main(String[] args) throws IOException {
        //判断表是否存在 然后打印表中的数据
//        String tableName = "student";
//        if(isTableExist(tableName)){
//            System.out.println("student表存在，内容如下：");
//            getAllRows(tableName);
//        }else
//        System.out.println("student表不存在!");

        //创建student01表
//        String[] columnFamliy = {"cf1","cf2"};
//        createTable("student01",columnFamliy);
//

        //向表student01中插入数据
//        addRowData("student01","10011","cf1","name","zhangsan");
//        addRowData("student01","10011","cf1","age","21");
//        addRowData("student01","10011","cf1","sex","男");
//        //打印表中的数据
//        getAllRows("student01");

        //删除多行数据
        //删除student01表中 rowkey值为10010和10011的数据
//        String[] rows = {"10010","10011"};
//        deleteMultRow("student01",rows);
        //获取一行数据
        //获取表student01中 rowkey值为10011的数据
        //getRow("student01","10011");
        //获取表student01中 rowkey值为1001的数据 显示所有版本
        //getRow("student","1001");

        //获取表中某一行指定"列簇：列"的数据
        //获取student表中”cf1：name“的值
        getRowQualifier("student","1001","cf1","name");
    }
}
