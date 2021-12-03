package ecnu.db.generator;

import com.google.ortools.Loader;
import com.google.ortools.sat.*;

/** Minimal CP-SAT example to showcase calling the solver. */
public class JoinStatusCompute {
    public static void main(String[] args) throws Exception {
        Loader.loadNativeLibraries();
        // Create the model.
        CpModel model = new CpModel();

        // Create the variables.
        int numVals = 20;
        IntVar[] arr = new IntVar[12];
        for (int i = 0; i < 12; i++) {
            arr[i] = model.newIntVar(0, numVals, String.valueOf(i));
        }

        // Create the constraints.
        model.addEquality(LinearExpr.sum(new IntVar[]{arr[0],arr[1],arr[2]}),5);
        model.addEquality(LinearExpr.sum(new IntVar[]{arr[3],arr[4],arr[5]}),1);

        model.addEquality(LinearExpr.sum(new IntVar[]{arr[0],arr[1]}),4);
        model.addEquality(LinearExpr.sum(new IntVar[]{arr[0],arr[3]}),3);

        model.addEquality(LinearExpr.sum(new IntVar[]{arr[6],arr[7]}),3);
        model.addEquality(LinearExpr.sum(new IntVar[]{arr[6],arr[9]}),2);

        for (int i = 0; i < 6; i++) {
            model.addLessOrEqual(arr[i+6],arr[i]);
            IntVar b = model.newBoolVar("b"+ i);
            model.addEquality(arr[i+6],0).onlyEnforceIf(b);
            model.addEquality(arr[i],0).onlyEnforceIf(b);
            model.addGreaterThan(arr[i+6],0).onlyEnforceIf(b.not());
            model.addGreaterThan(arr[i],0).onlyEnforceIf(b.not());
        }

        model.addLessOrEqual(LinearExpr.sum(new IntVar[]{arr[6],arr[9]}),2);
        model.addLessOrEqual(LinearExpr.sum(new IntVar[]{arr[7],arr[10]}),2);
        model.addLessOrEqual(LinearExpr.sum(new IntVar[]{arr[8],arr[11]}),1);

        // Create a solver and solve the model.
        CpSolver solver = new CpSolver();
        solver.getParameters().setEnumerateAllSolutions(true);
        CpSolverStatus status = solver.solve(model);

        if (status == CpSolverStatus.OPTIMAL || status == CpSolverStatus.FEASIBLE) {
            int i = 0;
            for (IntVar intVar : arr) {
                System.out.print(solver.value(intVar)+" ");
                if(i++ == 5){
                    System.out.println();
                }
            }
            System.out.println();
            System.out.println(solver.wallTime());
            System.out.println(solver.userTime());
        } else {
            System.out.println("No solution found.");
        }
        System.out.println(model.validate());
    }
}