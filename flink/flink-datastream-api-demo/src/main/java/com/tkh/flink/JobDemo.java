package com.tkh.flink;

import com.tkh.flink.jobs.PlayJob;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.table.api.PlannerConfig;

import static com.tkh.flink.Environment.*;

public class JobDemo {
    public static void main(String[] args) throws Exception{
        env.setParallelism(2);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        tEnv.getConfig().getConfiguration().setBoolean("table.dml-sync", true);

        PlayJob.preExecution();

        exeSet.execute();
//        env.execute();
    }
}
