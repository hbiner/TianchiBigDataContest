import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;


public class scaleDriver {

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();

		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("user_id:string,brand_id:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("last_datetime:string,var:double"));

		// TODO: specify input and output tables
		InputUtils
				.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(),
				job);

		job.setMapperClass(scaleMapper.class);
		job.setReducerClass(scaleReducer.class);
		job.set("date_slice",args[2]) ;
		job.set("var_name", args[3]);
		job.set("min", args[4]);
		job.set("max", args[5]);
		//job.setSplitSize(16);; //set the num of mapper
		
		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}
