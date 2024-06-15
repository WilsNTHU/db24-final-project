package org.vanilladb.bench.server.param.sift;

import java.util.Set;
import java.util.ArrayList;

import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureHelper;

public class SiftBenchParamHelper implements StoredProcedureHelper {
    private final String table = "sift";
    private final String embField = "i_emb";
    private VectorConstant query;
    private int numDimension;
    private Integer[] items;
    private int numNeighbors = 20; // Number of top-k

    private int insertCount = 0;
    private ArrayList<Object[]> insertHistory  = new ArrayList<>();

    @Override
    public void prepareParameters(Object... pars) {
        numDimension = (Integer) pars[0];
        float[] rawVector = new float[numDimension];
        for (int i = 0; i < numDimension; i++) {
            rawVector[i] = (float) pars[i+1];
        }
        query = new VectorConstant(rawVector);
        items = new Integer[numNeighbors];

        if (pars.length > numDimension+1){
            insertCount = (Integer) pars[numDimension+1];
            int ptr = numDimension + 2;
            for (int i = 0; i < insertCount; i++){
                Object[] insertParam = new Object[numDimension + 1];
                for (int j = 0; j < numDimension + 1; j++) {
                    insertParam[j] = pars[ptr + j + 1];
                }
                insertHistory.add(insertParam);
                ptr = ptr + numDimension + 2;
            }
        }
    }

    @Override
    public Schema getResultSetSchema() {
        Schema sch = new Schema();
        sch.addField("rc", Type.INTEGER);
        for (int i = 0; i < numNeighbors; i++) {
            sch.addField("id_" + i, Type.INTEGER);
        }
        return sch;
    }

    @Override
    public SpResultRecord newResultSetRecord() {
        SpResultRecord rec = new SpResultRecord();
        rec.setVal("rc", new IntegerConstant(numNeighbors));

        for (int i = 0; i < numNeighbors; i++) {
            rec.setVal("id_" + i, new IntegerConstant(items[i]));
        }
        return rec;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    public void setNearestNeighbors(Set<Integer> nearestNeighbors) {
        items = nearestNeighbors.toArray(items);
    }

    public String getTableName() {
        return table;
    }

    public String getEmbeddingField() {
        return embField;
    }

    public VectorConstant getQuery() {
        return query;
    }

    public int getK() {
        return numNeighbors;
    }

    public int getInsertCount(){
        return insertCount;
    }

    public ArrayList<Object[]> getInsertHistory() {
        return insertHistory;
    }
}
