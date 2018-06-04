package common;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by LuoYJ on 2018/6/1.
 * 此代码为练习使用
 * colFamily == columns
 */
public class HBaseHelp {
    private static final Logger logger = LoggerFactory.getLogger(HBaseHelp.class);

    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;
//    private static HBaseAdmin admin;

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "192.168.226.131");

    }

    public static void createConnect(){

        try {
            connection = ConnectionFactory.createConnection(configuration);
//            admin = new HBaseAdmin(configuration);  //过时？
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 关闭连接
    public static void close() {
        try {
            if (null != admin)
                admin.close();
            if (null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //创建表--列族 以数组传入
    public boolean createTable(String tableName,String[] columns){
        createConnect();
        try {
            if (admin.tableExists(TableName.valueOf(tableName))){
                System.out.println("table is exists!");
                return false;
            }else{
                //创建table对象，管理列族，获取属性等
//                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);     //过时？
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

                for (String column :
                        columns) {
                    //获取列族名字等
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(column);
                    tableDescriptor.addFamily(hColumnDescriptor);
                }
                logger.info("-------createTable-----");
                admin.createTable(tableDescriptor);
            }
        } catch (IOException e) {
            logger.info("-----异常:"+e.getMessage());
            e.printStackTrace();
            return false;

        }
        close();
        logger.info("创建表成功！");

        return true;

    }

    //删除表
    public boolean deleteTable(String tableName){
        createConnect();
        TableName name = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(name)){
                logger.info("-------deleteTable-----");
                //删除表前，需要disable(禁用表)
                admin.disableTable(name);
                admin.deleteTable(name);
            }
            else {
                System.out.println("table not exists!");
                return false;
            }

        } catch (IOException e) {
            logger.info("-----异常:"+e.getMessage());
            e.printStackTrace();
            return false;
        }
        close();
        return true;
    }

    //查看已有表
    public void listTable(){
        createConnect();
        try {
            TableName[] tableNames = admin.listTableNames();
            for (TableName name :
                    tableNames) {
                System.out.println(name);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        close();
    }

    /**
     * @param tableName 表名
     * @param rowkey
     * @param colFamily 列族
     * @param qualifier key
     * @param value     value
     */
    //插入数据
    public boolean putDate(String tableName,String rowkey,String colFamily,String qualifier,String value){
        createConnect();
        try {
//            HTable hTable = new HTable(configuration, tableName);  //过期？
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowkey.getBytes());

//            put.add(colFamily.getBytes(),qualifier.getBytes(),value.getBytes());
            put.addColumn(colFamily.getBytes(),qualifier.getBytes(),value.getBytes());
            table.put(put);
            table.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        close();
        return true;
    }

    //修改数据---相同即修改

    //修改表结构---增加列族
    public boolean addColumn(String tableName,String[] columns){
        createConnect();
        try {
            HColumnDescriptor hColumnDescriptor = null;
            for (String column:
                    columns) {
                hColumnDescriptor = new HColumnDescriptor(column);
            }
//            admin.modifyColumn(TableName.valueOf(tableName),hColumnDescriptor);  //重新修改
            admin.addColumn(TableName.valueOf(tableName),hColumnDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        close();
        return true;
    }

    //删除数据
    public boolean deleteDate(String tableName,String rowkey,String colFamily,String qualifier){
        createConnect();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(rowkey.getBytes());  //删除指定rowkey

            if (StringUtils.isNotBlank(colFamily)){

                //删除指定列
                if (StringUtils.isNotBlank(qualifier)){
                    delete.addColumn(colFamily.getBytes(),qualifier.getBytes());
                }else{
                    //删除指定列族
                    delete.addFamily(colFamily.getBytes());
                }
            }

            //删除指定 时间戳

            table.delete(delete);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        close();
        return true;
    }

    //根据rowkey查询数据  中文乱码问题
    public void getDate(String tableName,String rowkey,String colFamily,String qualifier){
        createConnect();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowkey.getBytes());

            if (StringUtils.isNotBlank(colFamily)){

                //查询指定列
                if (StringUtils.isNotBlank(qualifier)){
                    get.addColumn(colFamily.getBytes(),qualifier.getBytes());
                }else{
                    //查询指定列族
                    get.addFamily(colFamily.getBytes());
                }
            }

            Result result = table.get(get);
            showCell(result);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**批量查询数据  scan  范围查询时，包头不包尾
     * @param tableName  必填
     * @param columns    列族 （可查询不同列族）
     * @param qualifier  这个可以考虑使用get
     *
     * @param startRow   开始行 rowkey
     * @param stopRow    结束行
     */
    public void scanDate(String tableName,String startRow,String stopRow,String[] columns,String qualifier){
        createConnect();
        try {
            //全表扫描
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();

            //按rowkey范围查询，可以有头无尾   方式1
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(stopRow.getBytes());

            //按 列族查询  方式2
            for (String column :
                    columns) {
                scan.addFamily(column.getBytes());
//                scan.addColumn(column.getBytes(),qualifier.getBytes());  也可以查询同一列族，多列的情况
            }

            ResultScanner scanner = table.getScanner(scan);

            for (Result result : scanner) {
                showCell(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // 格式化输出
    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("key:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
            System.out.println("------------------------");
        }
    }


}
