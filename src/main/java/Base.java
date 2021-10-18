import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 定义所有task作业的父类，在父类中实现公共的代码
 * 加载配置文件内容到ParameterTool对象中
 * 1）flink流处理环境的初始化
 * 2）flink接入mongdb数据源消费数据
 */
public abstract class Base {
    private static Logger logger = LoggerFactory.getLogger(Base.class);


    //定义parameterTool工具类
    public static ParameterTool parameterTool;
    public static String appName;

    /**
     * 定义静态代码块，加载配置文件数据到ParameterTool对象中
     */
    static {
        try {
            parameterTool = ParameterTool.fromPropertiesFile(Base.class.getClassLoader().getResourceAsStream("conf.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //todo 初始化flink流式处理的开发环境
    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


    /**
     * TODO flink任务的初始化方法
     *
     * @return
     */
    public static StreamExecutionEnvironment getEnv(String className) {


        //为了后续进行测试方便，将并行度设置为1，在生产环境一定不要设置代码级别的并行度，可以设置client级别的并行度
        env.setParallelism(1);

        //todo 开启checkpoint
        //  设置每隔30s周期性开启checkpoint
        env.enableCheckpointing(30 * 1000);
        //  设置检查点的model、exactly-once、保证数据一次性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //  设置两次checkpoint的时间间隔，避免两次间隔太近导致频繁的checkpoint，而出现业务处理能力下降
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(20 * 1000);
        //  设置checkpoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(600 * 1000);
        //  设置checkpoint的最大尝试次数，同一个时间有几个checkpoint在运行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //  设置checkpoint取消的时候，是否保留checkpoint，checkpoint默认会在job取消的时候删除checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //  设置执行job过程中，保存检查点错误时，job不失败
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);

        //todo  如果开启了checkpoint，默认不停的重启，没有开启checkpoint，无重启策略
        //env.setRestartStrategy(RestartStrategies.noRestart());

        appName = className;

        //返回env对象
        return env;
    }
}
