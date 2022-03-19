package sparks;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static sparks.Connecthbase.initHbase;

public class Getprovrf {
    public static void main(String[] args) {

    }
    public static String getprovrf ( )throws IOException {
        String data = getData("provincerf");
        return data;

    }
    public static String getData(String tableName) throws IOException{
        Table table = initHbase().getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        HashMap<String, String> hashmap = new HashMap<String, String>();
        ArrayList list = new ArrayList();
        for (Result r : rs) {
            Cell[] cells = r.rawCells();
            for (Cell cell : cells) {
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                hashmap.put(key, value);
            }
            JSONObject jsonObject = new JSONObject(hashmap);
            list.add(jsonObject.toString());
        }
        return list.toString();
    }
}
