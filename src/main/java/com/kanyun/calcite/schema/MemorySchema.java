package com.kanyun.calcite.schema;

import com.kanyun.calcite.column.MemoryColumn;
import com.kanyun.calcite.table.MemoryTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemorySchema extends AbstractSchema {

    private Map<String, Table> tableMap;
    private List<MemoryColumn> meta;
    private List<List<Object>> source;

    public MemorySchema(List<MemoryColumn> meta, List<List<Object>> source){
        this.meta = meta;
        this.source = source;
    }

    @Override
    public Map<String, Table> getTableMap(){
        if(tableMap == null){
            tableMap = new HashMap<>();
            tableMap.put("memory", new MemoryTable(meta, source));
        }
        return tableMap;
    }
}