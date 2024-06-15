package org.vanilladb.bench.benchmarks.sift.rte;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.vanilladb.bench.TxnResultSet;
import org.vanilladb.bench.VanillaBenchParameters;
import org.vanilladb.bench.benchmarks.sift.SiftTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.sp.VanillaDbSpResultSet;
import org.vanilladb.bench.rte.TransactionExecutor;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.rte.jdbc.JdbcExecutor;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;

public class SiftTxExecutor extends TransactionExecutor<SiftTransactionType> {

    private Map<VectorConstant, Set<Integer>> resultMap;
    private Map<VectorConstant, Integer> insertMap;
    private CopyOnWriteArrayList<Object[]> insertHistory;
    private boolean isWarmingUp = false;

    public SiftTxExecutor(TxParamGenerator<SiftTransactionType> pg, Map<VectorConstant, Set<Integer>> resultMap, Map<VectorConstant, Integer> insertMap, CopyOnWriteArrayList<Object[]> insertHistory) {
        this.pg = pg;
        this.resultMap = resultMap;
        this.insertMap = insertMap;
        this.insertHistory = insertHistory;
    }

    public void setWarmingUp(boolean isWarmingUp) {
        this.isWarmingUp = isWarmingUp;
    }

    @Override
    public TxnResultSet execute(SutConnection conn) {
        try {
            // generate parameters
            Object[] params = pg.generateParameter();
            VectorConstant query = ((SiftParamGen) pg).getQuery();

            long txnRT = System.nanoTime();

            
            

            VanillaDbSpResultSet result = (VanillaDbSpResultSet) executeTxn(conn, params);
            
            if (pg.getTxnType() == SiftTransactionType.ANN && !isWarmingUp){
                insertMap.put(query, insertHistory.size());
            }

            if (pg.getTxnType() == SiftTransactionType.INSERT && !isWarmingUp){
                // params: [dim, new_emb, id]
                insertHistory.add(params);
            }

            // measure response time
            long txnEndTime = System.nanoTime();
            txnRT = txnEndTime - txnRT;

            // display output
            if (VanillaBenchParameters.SHOW_TXN_RESPONSE_ON_CONSOLE)
                System.out.println(pg.getTxnType() + " " + result.outputMsg());

            Set<Integer> approximateNeighbors = new HashSet<>();
            
            Schema sch = result.getSchema();
            Record rec = result.getRecords()[0];
            
            for (String fld : sch.fields()) {
                if (fld.equals("rc"))
                    continue;
                approximateNeighbors.add((Integer) rec.getVal(fld).asJavaVal());
            }

            if (pg.getTxnType() == SiftTransactionType.ANN && !isWarmingUp){
                resultMap.put(query, approximateNeighbors);
            }
            
            return new TxnResultSet(pg.getTxnType(), txnRT, txnEndTime, result.isCommitted(), result.outputMsg());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    protected JdbcExecutor<SiftTransactionType> getJdbcExecutor() {
        throw new UnsupportedOperationException("no JDBC implementation for SIFT");
    }
    
}
