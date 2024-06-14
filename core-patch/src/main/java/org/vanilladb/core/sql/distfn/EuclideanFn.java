package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

// Optimization: SIMD operation
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class EuclideanFn extends DistanceFn {

    static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;
    static final int SPECIES_LENGTH = SPECIES.length();

    public EuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {
        float[] queryArray = query.asJavaVal();
        float[] paramArray = vec.asJavaVal();
        double sum = 0;
        int i = 0;
        int bound = SPECIES.loopBound(vec.dimension());
        FloatVector queryVector, paramVector, diffVector;
        // Perform SIMD on operands as many as possible
        for (; i < bound; i += SPECIES_LENGTH) {
            queryVector = FloatVector.fromArray(SPECIES, queryArray, i);
            paramVector = FloatVector.fromArray(SPECIES, paramArray, i);
            diffVector = queryVector.sub(paramVector);
            sum += diffVector.mul(diffVector).reduceLanes(VectorOperators.ADD);
        }
        // Perform tail operands sequentially
        for(; i < vec.dimension(); ++i) {
            double diff = queryArray[i] - paramArray[i];
            sum = Math.fma(diff, diff, sum);
        }
        return Math.sqrt(sum);
    }
    
    public static void main(String[] args) {
        VectorConstant v1 = new VectorConstant(new float[] {1, 2, 3, 4, 5});
        VectorConstant v2 = new VectorConstant(new float[] {1, 2, 3, 4, 5});

        EuclideanFn euc = new EuclideanFn("dummy");
        euc.setQueryVector(v1);

        System.out.println(euc.distance(v2));
    }
}
