import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;


public class DemoMain  {


    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);


        DataSet<String> input=env.fromElements("张三 12 男"
                , "李四 13 男"
                , "王五 14 男"
                , "赵柳 15 男"
                ,"前期 16 男"
                ,"liuba 16 男"
                , "孙九 9 男"
                ,"正式 2 男");
        input.print();

        DataSet<PlayerData> topInput=input.map(new MapFunction<String, PlayerData>() {
            @Override
            public PlayerData map(String s) throws Exception {
                String[] split=s.split(" ");

                return new PlayerData(String.valueOf(split[0]),
                        String.valueOf(split[1]),
                        String.valueOf(split[2]));
            }
        });
        Table topScope=tableEnv.fromDataSet(topInput);
        tableEnv.registerTable("table1",topScope);


        Table queryResult =tableEnv.sqlQuery("select sex2 as player, count(age) as num from table1 group by sex2 ");

        tableEnv
                .toDataSet(queryResult,Result.class)
                .print();

    }

    //修饰词 必须为 public static
    public static class PlayerData{
        public String name;
        public String age;
        public String sex2;

        public PlayerData() {
        }

        public PlayerData(String name, String age, String sex2) {
            this.name = name;
            this.age = age;
            this.sex2 = sex2;
        }

        @Override
        public String toString() {
            return "PlayerData{" +
                    "name='" + name + '\'' +
                    ", age='" + age + '\'' +
                    ", sex2='" + sex2 + '\'' +
                    '}';
        }
    }
    public static class Result {
        // 特征名和sql中的一一对应
        public String player;
        public Long num;

        public Result() {
            super();
        }
        public Result(String player, Long num) {
            this.player = player;
            this.num = num;
        }
        @Override
        public String toString() {
            return player + ":" + num;
        }
    }

}


































