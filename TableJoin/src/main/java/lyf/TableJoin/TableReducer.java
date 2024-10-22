package lyf.TableJoin;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TableReducer extends Reducer<Text,TableBean,TableBean,NullWritable>{

    private TableBean outV = new TableBean();
    @Override
    protected void reduce(Text text, Iterable<TableBean> values,
            Reducer<Text, TableBean, TableBean,NullWritable>.Context context) throws IOException, InterruptedException 
    {
        ArrayList<TableBean> orders = new ArrayList<>();
        ArrayList<TableBean> pds = new ArrayList<>();
        for(TableBean tableBean:values)
        {
            TableBean tempTableBean = new TableBean();
            if(tableBean.getFlag().equals("order"))
            {
                try {
                    BeanUtils.copyProperties(tempTableBean, tableBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orders.add(tempTableBean);
            }
            else if(tableBean.getFlag().equals("pd"))
            {
                try {
                    BeanUtils.copyProperties(tempTableBean, tableBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                pds.add(tempTableBean);
            }
        }
        for(TableBean order:orders)
        {
            for(TableBean pd:pds)
            {
                outV.setId(order.getId());
                outV.setPid(text.toString());
                outV.setPname(pd.getPname());
                outV.setAmount(order.getAmount());
                context.write(outV,NullWritable.get());
            }
        }
    }
    
}
