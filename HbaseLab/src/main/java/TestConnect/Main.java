package TestConnect;
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
        createTable("table1", new String[]{"f1"});
        insertData("table1", "1", "f1", "f11", "200");
        getData("table1", "1","f1", "f11");
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
            System.out.println("table exists!"); 
        }
        else { 
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName); 
            for(String str: colFamily){ 
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str); 
                hTableDescriptor.addFamily(hColumnDescriptor); 
            } 
            admin.createTable(hTableDescriptor); 
        } 
        close(); 
    }
    public static void insertData(String tableName, String rowkey, String colFamily, String col, String val) throws IOException { 
        init(); 
        Table table = connection.getTable(TableName.valueOf(tableName)); 
        Put put = new Put(Bytes.toBytes(rowkey)); 
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val)); 
        table.put(put); 
        table.close(); 
        close(); 
    }
    public static void getData(String tableName,String rowkey,String colFamily,String col)throws IOException{ 
        init(); 
        Table table = connection.getTable(TableName.valueOf(tableName)); 
        Get get = new Get(Bytes.toBytes(rowkey)); 
        get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col)); 
        //获取的result数据是结果集，还需要格式化输出想要的数据才行 
        Result result = table.get(get); 
        System.out.println(new
            String(
                result.getValue(colFamily.getBytes(),col==null?null:col.getBytes())
            )
        );
        table.close(); 
        close(); 
    } 
}
