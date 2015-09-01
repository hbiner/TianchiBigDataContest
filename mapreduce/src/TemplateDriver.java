import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.ReducerBase;

import java.io.IOException;
import java.util.Iterator;

public class TemplateDriver {
	public class Mapper extends MapperBase {

		@Override
		public void setup(TaskContext context) throws IOException {
		}

		@Override
		public void map(long recordNum, Record record, TaskContext context)
				throws IOException {
		}

	}
	public class Reducer extends ReducerBase {

		@Override
		public void setup(TaskContext context) throws IOException {
		}

		@Override
		public void reduce(Record key, Iterator<Record> values, TaskContext context)
				throws IOException {

			while (values.hasNext()) {
				values.next();
				// TODO process value
			}
		}

	}

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();

		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

		// TODO: specify input and output tables
		InputUtils
				.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(),
				job);

		// TODO: specify a mapper
		job.setMapperClass(Mapper.class);
		// TODO: specify a reducer
		job.setReducerClass(Reducer.class);

		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}
}
