package org.vanilladb.bench.benchmarks.sift.rte;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.vanilladb.bench.benchmarks.sift.SiftBenchConstants;
import org.vanilladb.bench.benchmarks.sift.SiftTransactionType;
// import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.util.RandomValueGenerator;
import org.vanilladb.core.sql.VectorConstant;

public class SiftInsertParamGen extends SiftParamGen {
    private static RandomValueGenerator randomGenerator = new RandomValueGenerator();
    private static AtomicInteger curId = new AtomicInteger(SiftBenchConstants.NUM_ITEMS);

    private VectorConstant query;

    @Override
    public SiftTransactionType getTxnType() {
        return SiftTransactionType.INSERT;
    }

    private VectorConstant getSingleVector(int line) {
        return (VectorConstant) queryList.get(line);
    }

    @Override
    public Object[] generateParameter() {
        ArrayList<Object> paramList = new ArrayList<>();

        // =====================
		// Generating Parameters
		// =====================
        paramList.add(SiftBenchConstants.NUM_DIMENSION);

        // Generate a query vector
        query = getSingleVector(randomGenerator.number(SiftBenchConstants.NUM_ITEMS, 1000000 - 1));
        
        for (int i = 0; i < SiftBenchConstants.NUM_DIMENSION; i++) {
            paramList.add(query.get(i));
        }
        paramList.add(curId.getAndIncrement());

        return paramList.toArray();
    }
    
}
