package com.flink.example.ml;

import com.flink.example.ml.util.LinearRegressionData;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Collection;

/**
 * @author vector
 * @date: 2019/7/9 0009 16:20
 */
public class LinearRegression {
    public static void main(String[] args) {
        final  ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final int iterations = params.getInt("iterations", 10);

        env.getConfig().setGlobalJobParameters(params);

        DataSet<Data> data;
        if (params.has("input")) {
            data = env.readCsvFile(params.get("input"))
                    .fieldDelimiter(" ")
                    .includeFields(true, true)
                    .pojoType(Data.class);
        }else{
            System.out.println("Executing LinearRegression example with default input data set.");
            System.out.println("Use --input to specify file input.");
            data = LinearRegressionData.getDefaultDataDataSet(env);
        }


        DataSet<Data> parameters = LinearRegressionData.getDefaultDataDataSet(env);

        IterativeDataSet<Data> loop = parameters.iterate(iterations);

        data.map(new RichMapFunction<Data, Tuple2<Params, Integer>>() {

            private Collection<Params> parameters;

            private Params parameter;

            private int count = 1;

            @Override
            public void open(Configuration parameters) throws Exception {
                this.parameters = getRuntimeContext().getBroadcastVariable("parameters");
            }

            @Override
            public Tuple2<Params, Integer> map(Data data) {
                for (Params p : parameters) {
                    this.parameter = p;
                }
                double theta0 = parameter.theta0 - 0.01 * ((parameter.theta0 + (parameter.theta1 * data.x)) - data.y);
                double theta1 = parameter.theta1 - 0.01 * (((parameter.theta0 + (parameter.theta1 * data.x)) - data.y) * data.x);

                return new Tuple2<>(new Params(theta0, theta1), count);
            }
        }).withBroadcastSet(loop, "parameters")
                .reduce(new ReduceFunction<Tuple2<Params, Integer>>() {
                    @Override
                    public Tuple2<Params, Integer> reduce(Tuple2<Params, Integer> val1, Tuple2<Params, Integer> val2) throws Exception {
                        double newTheta0 = val1._1.theta0 + val2._1.theta0;
                        double newTheta1 = val1._1.theta1 + val2._1.theta1;
                        Params result = new Params(newTheta0, newTheta1);
                        return new Tuple2<Params, Integer>(result, val1._1 + val2._1);

                    }
                })

    }

    public static class Data implements Serializable{
        public double x, y;

        public Data() {}

        public Data(double x, double y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public String toString() {
            return "(" + x + "|" + y + ")";
        }

    }

    public static class Params implements Serializable {

        private double theta0, theta1;

        public Params() {}

        public Params(double x0, double x1) {
            this.theta0 = x0;
            this.theta1 = x1;
        }

        @Override
        public String toString() {
            return theta0 + " " + theta1;
        }

        public double getTheta0() {
            return theta0;
        }

        public double getTheta1() {
            return theta1;
        }

        public void setTheta0(double theta0) {
            this.theta0 = theta0;
        }

        public void setTheta1(double theta1) {
            this.theta1 = theta1;
        }

        public Params div(Integer a) {
            this.theta0 = theta0 / a;
            this.theta1 = theta1 / a;
            return this;
        }

    }

}
