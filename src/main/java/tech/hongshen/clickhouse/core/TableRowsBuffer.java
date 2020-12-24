package tech.hongshen.clickhouse.core;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hongshen
 * @date 2020/12/24
 */
public class TableRowsBuffer {

    private final String table;
    private final List<String> rows;

    public TableRowsBuffer(String table) {
        this.table = table;
        this.rows = new ArrayList<>();
    }

    public void add(String row) {
        rows.add(row);
    }

    public int bufferSize() {
        return rows.size();
    }

    public List<String> getRows() {
        return rows;
    }

    public String getTable() {
        return table;
    }
}
