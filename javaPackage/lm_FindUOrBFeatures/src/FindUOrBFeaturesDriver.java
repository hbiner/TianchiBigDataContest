import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;


public class FindUOrBFeaturesDriver {

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();

		
		if ( args[2].compareTo("brand") == 0 ){
		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("brand_id:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("user_id:string,clicked:double,bought:double,collected:double,basketed:double,actions:bigint"));
		}
		else if ( args[2].compareTo("user") == 0  ){
			// TODO: specify map output types
			job.setMapOutputKeySchema(SchemaUtils.fromString("user_id:string"));
			job.setMapOutputValueSchema(SchemaUtils.fromString("brand_id:string,clicked:double,bought:double,collected:double,basketed:double,actions:double"));
			}
		// TODO: specify input and output tables
		InputUtils
				.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(),
				job);

		job.setMapperClass(FindUOrBFeaturesMapper.class);
		job.setReducerClass(FindUOrBFeaturesReducer.class);
		job.setSplitSize(2);; //set the num of mapper
		job.setMemoryForJVM(2048);
		job.setMemoryForReduceTask(4096);
		// set the Item
		job.set("Item",args[2] );
		
		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}
