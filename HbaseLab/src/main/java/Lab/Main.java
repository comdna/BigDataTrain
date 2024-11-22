package Lab;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.hbase.*; 
import org.apache.hadoop.hbase.client.*; 
import org.apache.hadoop.hbase.util.Bytes; 
import java.io.IOException;
import java.util.NavigableMap;


public class Main {
    public static Configuration configuration; 
    public static Connection connection; 
    public static Admin admin; 
    public static void main(String[] args)throws IOException{
        /*
         *创建表的测试 
        */
        createTable("SC", new String[]{"STUDENT","COURSE"});

        /**
         *测试列的扫描 
        */
        // System.out.println("student列簇扫描:");
        // scanColumn("SC", "student");
        // System.out.println("course列簇扫描:");
        // scanColumn("SC", "course");
        // System.out.println("course:score扫描");
        // scanColumn("SC", "course:score");

        /*
         *删除行测试 
        */
        // deleteRow("SC", "g002");
        // deleteRow("SC", "g003");

        /*
         *修改表信息 
        */
        //modifyData("SC", "g002", "course:score", "96");

        /*
            插入成绩信息的代码
        */
        // final String[] cols = new String[]{"student:No","course:No","course:score"};
        // String[] vals = new String[]{"2024211001","202402","66"};
        // addRecord("SC","g001",cols,vals);
        // vals = new String[]{"2024211002","202402","94"};
        // addRecord("SC","g002",cols,vals);
        // vals = new String[]{"2024211003","202403","88"};
        // addRecord("SC","g003",cols,vals);

        /*
        插入课程基础信息的代码
        */
        // final String[] cols = new String[]{"course:No","course:name","course:credit"};
        // String[] vals = new String[]{"202402","Computer Network","3"};
        // addRecord("SC","c001",cols,vals);
        // vals = new String[]{"202403","BigData","2"};
        // addRecord("SC","c002",cols,vals);
        // vals = new String[]{"202404","BasketBall","1"};
        // addRecord("SC","c003",cols,vals);

        /*
        插入学生基础信息的代码
        */
        // String[] cols = new String[]{"student:number","student:name","student:sex","student:age"};
        // String[] vals = new String[]{"2025211001","Hadoop","male","20"};
        // addRecord("SC","001",cols,vals);
        // vals = new String[]{"2025211002","HBase","male","21"};
        // addRecord("SC","002",cols,vals);
        // vals = new String[]{"2025211003","Spark","female","24"};
        // addRecord("SC","003",cols,vals);
    }
    private static void init(){ 
        configuration = HBaseConfiguration.create(); 
        configuration.set("hbase.rootdir","file:///root/data/hbase/data"); 
        try{
            connection = ConnectionFactory.createConnection(configuration); 
            admin = connection.getAdmin(); 
        }catch (IOException e){ 
            e.printStackTrace(); 
        } 
    }
    //关闭连接
    private static void close(){ 
        try{ 
            if(admin != null){ 
                admin.close(); 
            } 
            if(null != connection){ 
                connection.close(); 
            } 
        }catch (IOException e){ 
            e.printStackTrace(); 
        } 
    }
    
    @SuppressWarnings("deprecation")
    public static void createTable(String myTableName,String[] colFamily) throws IOException { 
        init(); 
        TableName tableName = TableName.valueOf(myTableName); 
        if(admin.tableExists(tableName)){ 
            System.out.println("table exists!Deleting it"); 
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table deleted successfully.");
        }

        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName); 
        for(String str: colFamily){ 
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str); 
            hTableDescriptor.addFamily(hColumnDescriptor); 
        } 
        admin.createTable(hTableDescriptor);     
        close(); 
    }
    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException
    {
        if(fields.length!=values.length){
            System.out.println("插入"+tableName+"：列数和数据数不一致! 插入失败");
            return;
        }
        init(); 
        Table table = connection.getTable(TableName.valueOf(tableName)); 
        Put put = new Put(Bytes.toBytes(row));
        String columnFamily = new String();
        String columnQualifier = new String();
        for(int i=0;i<fields.length;i++){
            String str = fields[i];
            if(!str.contains(String.valueOf(':'))){
                System.out.println("column不符合输入要求");
            }
            else{
                String[] splits=str.split(":", 2);
                columnFamily = splits[0];
                columnQualifier = splits[1];
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes(values[i])); 
            }
        }
        table.put(put); 
        table.close(); 
        close(); 
    }
    
    public static void scanColumn(String tableName, String column) throws IOException {
        init(); // 初始化连接
        Table table = connection.getTable(TableName.valueOf(tableName)); // 获取表实例
        Scan scan = new Scan();

        // 判断参数是列族还是具体列
        if (column.contains(":")) {
            // 参数是具体列，添加列族和列限定符
            String[] parts = column.split(":");
            String columnFamily = parts[0];
            String qualifier = parts[1];
            scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
        } else {
            // 参数是列族，添加列族
            scan.addFamily(Bytes.toBytes(column));
        }

        // 扫描表
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            StringBuilder rowResult = new StringBuilder();
            String rowKey = Bytes.toString(result.getRow());
            rowResult.append(rowKey).append("  ");

            if (column.contains(":")) {
                String[] parts = column.split(":");
                String columnFamily = parts[0];
                String qualifier = parts[1];

                byte[] value = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                rowResult.append(qualifier).append(":").append(value == null ? "null" : Bytes.toString(value));
            } else {
                NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(column));
                if (familyMap != null) {
                    for (byte[] qualifier : familyMap.keySet()) {
                        byte[] value = familyMap.get(qualifier);
                        rowResult.append(Bytes.toString(qualifier)).append(":")
                                .append(value == null ? "null" : Bytes.toString(value)).append("  ");
                    }
                }
            }
            System.out.println(rowResult.toString().trim());
        }
        table.close();
        close();
    }
    public static void modifyData(String tableName, String row, String column, String newData) throws IOException
    {
        init(); // 初始化连接
        Table table = connection.getTable(TableName.valueOf(tableName)); // 获取表实例
        //column->Family Column
        String columnFamily = new String();
        String columnQualifier = new String();
        if(!column.contains(String.valueOf(':'))){
            System.out.println("column不符合输入要求");
        }
        else{
            String[] splits=column.split(":", 2);
            columnFamily = splits[0];
            columnQualifier = splits[1];
        }

        // 创建 Put 对象，用于指定要修改的行
        Put put = new Put(Bytes.toBytes(row));
        
        // 在指定列族和列中设置新的数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes(newData));
        
        // 执行插入/更新操作
        table.put(put);
        System.out.println("Row '" + row + "', Column '" + columnFamily + ":" + column + "' updated with new data: '" + newData + "'.");
        table.close();
        close();
    }

    //删除某行
    public static void deleteRow(String tableName, String row) throws IOException
    {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(row));
        // 执行删除操作
        table.delete(delete);
        System.out.println("Row '" + row + "' deleted successfully from table '" + tableName + "'.");
        table.close();
        close();
    }
}

