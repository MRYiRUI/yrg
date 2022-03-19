package sparks;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static sparks.Connecthbase.initHbase;

public class Getutitle {
    public static void main(String[] args) {

    }
    public static String getutitle(String msg)throws IOException {
        List<String> arr=new ArrayList<String>();
        arr.add("cf,memberId,"+msg);
        String data = getData("usertitle",arr);
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
