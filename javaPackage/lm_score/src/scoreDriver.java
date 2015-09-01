import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;


public class scoreDriver {

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();

		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("user_id:string,fre_click:double,"
			                                           	+ "fre_buy:double,actions:bigint,count_buy_brand_kinds:bigint"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("brand_id:string,n_click:bigint,n_buy:bigint,n_collect:bigint,n_basket:bigint"));
		
		// TODO: specify input and output tables
		InputUtils
				.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(),
				job);

		job.setMapperClass(scoreMapper.class);
		job.setReducerClass(scoreReducer.class);

		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
		
	}
}
