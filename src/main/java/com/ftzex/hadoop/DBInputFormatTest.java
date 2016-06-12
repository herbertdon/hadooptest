package com.ftzex.hadoop;

import com.ftzex.utils.JobHelper;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by dondavid on 16/6/12.
 */
public class DBInputFormatTest {
    public static void main(String[] args) throws Exception {
        runJob(args);
    }

    public static void runJob(String[] args) throws IOException {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: DBInputFormatTest <mysql-connector> <in> <out>");
            System.exit(2);
        }

        String mysqlJar = args[0];
        String output = args[1];

        JobHelper.addJarForJob(conf,mysqlJar);

        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver","jdbc:mysql://localhost/hadooptest" +
            "?user=root&password=passw0rd");

        /**JobConf和Job类有什么区别,怎么都有setJarByClass/setInputFormat等类似方法?
         * ANS: JobConf是旧版API,当时是对于Configuration的Job配置扩展,但是在新版API中并不区分,配置都通过Configuration配置
         *      而Job对象就是做作业控制的,而非JobClient类.这里我们看到这个例子全是用旧版的API去做的,包括DBConfiguration都是
         *      旧版的API,并且用了JobClient来做作业控制.不清楚是否在新版API中已经没有DBConfiguration?还有,旧版新版API能否支
         *      持混合使用?
         *      ANS: 最好别混合使用,都统一使用一套API.现在尝试使用新版API来改写.
         **/
        Job job = Job.getInstance(conf,"DBInputFormatTest");
        job.setJarByClass(DBInputFormatTest.class);

        Path outputPath = new Path(output);
        outputPath.getFileSystem(conf).delete(outputPath,true);

        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(AvroOu);

    }
}
