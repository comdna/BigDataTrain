package Lab;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.hbase.*; 
import org.apache.hadoop.hbase.client.*; 
import org.apache.hadoop.hbase.util.Bytes; 
import java.io.IOException;


public class Main {
    public static Configuration configuration; 
    public static Connection connection; 
    public static Admin admin; 
    public static void main(String[] args)throws IOException{
        createTable("SC", new String[]{"Student","Course"});


    }
    public static void init(){ 
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
    public static void close(){ 
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
    public static void scanColumn(String tableName, String column) throws IOException
    {

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

