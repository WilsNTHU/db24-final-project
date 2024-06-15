package org.vanilladb.core.query.algebra.vector;

import java.util.Set;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

public class NearestNeighborPlan implements Plan {
    private Plan child;
    
    private DistanceFn distFn;
    private Set<String> projectFields;
    private int limit;

    public NearestNeighborPlan(Plan p, DistanceFn distFn, int limit, Set<String> projectFields, Transaction tx) {
        this.distFn = distFn;
        this.limit = limit;
        this.projectFields = projectFields;
        this.child = p;
    }

    @Override
    public Scan open() {
        Scan s = child.open();
        return new NearestNeighborScan(s, distFn, limit, projectFields);
    }

    @Override
    public long blocksAccessed() {
        return child.blocksAccessed();
    }

    @Override
    public Schema schema() {
        return child.schema();
    }

    @Override
    public Histogram histogram() {
        return child.histogram();
    }

    @Override
    public long recordsOutput() {
        return child.recordsOutput();
    }
}       
