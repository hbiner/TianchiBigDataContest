package lm_lr ;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;


public class lrDriver {

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();

		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("user_id:string , brand_id:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("n_click:bigint,n_buy:bigint,"
														 + "n_collect:bigint,n_basket:bigint,click_label:bigint") );

		// TODO: specify input and output tables
		InputUtils
				.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(),
				job);

		job.setMapperClass(lrMapper.class);
		job.setReducerClass(lrReducer.class);

		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}
