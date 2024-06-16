package org.vanilladb.bench.server.procedure.sift;

import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;

import org.vanilladb.bench.benchmarks.sift.SiftBenchConstants;
import org.vanilladb.bench.server.param.sift.SiftBenchParamHelper;
import org.vanilladb.bench.server.procedure.StoredProcedureUtils;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class SiftCalculateRecallProc extends StoredProcedure<SiftBenchParamHelper> {

    public SiftCalculateRecallProc() {
        super(new SiftBenchParamHelper());
    }

    @Override
    protected void executeSql() {
        SiftBenchParamHelper paramHelper = getHelper();
        VectorConstant query = paramHelper.getQuery();
        Transaction tx = getTransaction();

        int numDimension = SiftBenchConstants.NUM_DIMENSION;
        ArrayList<Object[]> insertHistory = paramHelper.getInsertHistory(); //[emb, id]

        String deleteSql = String.format("DELETE FROM %s WHERE i_id >= %d", paramHelper.getTableName(), SiftBenchConstants.NUM_ITEMS);
        try {
            StoredProcedureUtils.executeUpdate(deleteSql, tx);
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    
        for (Object[] insertItem : insertHistory){
            int id = (Integer) insertItem[insertItem.length - 1];
            float[] rawVector = new float[numDimension];
            for (int i = 0; i < numDimension; i++) {
                rawVector[i] = (Float) insertItem[i];
            }
            String sql = String.format("INSERT INTO %s (i_id, %s) VALUES (%d, %s)", paramHelper.getTableName(), paramHelper.getEmbeddingField(), id, new VectorConstant(rawVector).toString());
            // System.out.println(sql);
            StoredProcedureUtils.executeUpdate(sql, tx);
        }

        // Execute true nearest neighbor search
        Scan trueNeighborScan = StoredProcedureUtils.executeCalculateRecall(query, paramHelper.getTableName(), paramHelper.getEmbeddingField(), paramHelper.getK(), tx);
        
        trueNeighborScan.beforeFirst();
        
        Set<Integer> nearestNeighbors = new HashSet<>();

        int count = 0;
        while (trueNeighborScan.next()) {
            nearestNeighbors.add((Integer) trueNeighborScan.getVal("i_id").asJavaVal());
            count++;
        }

        trueNeighborScan.close();

        if (count == 0)
            throw new RuntimeException("Nearest neighbor query execution failed for " + query.toString());
        
        paramHelper.setNearestNeighbors(nearestNeighbors);
    }
}
