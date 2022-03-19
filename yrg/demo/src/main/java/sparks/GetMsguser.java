package sparks;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static sparks.Connecthbase.initHbase;


public class GetMsguser {
    public static String getMsguser(String msg)throws IOException{
        List<String> arr=new ArrayList<String>();
        if(msg.contains("@")){
            arr.add("cf,email,"+msg);
        }
        else{
            arr.add("cf,mobile,"+msg);
        }
        String data = getData("user",arr);
        return data;
    }

    public static String getData(String tableName ,List<String> arr) throws IOException{
        Table table = initHbase().getTable(TableName.valueOf(tableName));
        FilterList filterList = new FilterList();
        Scan scan = new Scan();
        for(String str:arr) {
            String[] s = str.split(",");
            SingleColumnValueFilter sc = new SingleColumnValueFilter(Bytes.toBytes(s[0]),
                    Bytes.toBytes(s[1]),
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes(s[2]));
            sc.setFilterIfMissing(true);
            filterList.addFilter(sc);
        }
        scan.setFilter(filterList);
        ResultScanner rs = table.getScanner(scan);
        HashMap<String, String> hashmap = new HashMap<String, String>();
        for (Result r : rs) {
            String row = "id";
            Long rowv = Bytes.toLong(r.getRow());
            hashmap.put(row, rowv.toString());
            Cell[] cells = r.rawCells();
            for (Cell cell : cells) {
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                hashmap.put(key, value);
            }
        }
        JSONObject jsonObject = new JSONObject(hashmap);
        return jsonObject.toString();
    }

}


