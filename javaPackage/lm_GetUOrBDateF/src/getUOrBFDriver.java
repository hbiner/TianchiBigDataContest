import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;


public class getUOrBFDriver {

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();

		// TODO: specify map output types
		if (args[2].compareTo("user_id")== 0 ){
		job.setMapOutputKeySchema(SchemaUtils.fromString("user_id:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("brand_id:string,type:string,visit_datetime:string"));
		}
		else if (args[2].compareTo("brand_id")== 0 ){
			job.setMapOutputKeySchema(SchemaUtils.fromString("brand_id:string"));
			job.setMapOutputValueSchema(SchemaUtils.fromString("user_id:string,type:string,visit_datetime:string"));
		}

		// TODO: specify input and output tables
		InputUtils
				.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(),
				job);

		job.setMapperClass(getUOrBFMapper.class);
		job.setReducerClass(getUOrBFReducer.class);
		job.setSplitSize(8);
		job.set("item", args[2]);
		job.set("startDate", args[3]);
		job.set("endDate", args[4]);
		job.setSplitSize(8);
		
		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}
