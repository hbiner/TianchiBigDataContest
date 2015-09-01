package lm_test;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.TaskContext;

public class testMapper extends MapperBase {

	@Override
	public void setup(TaskContext context) throws IOException {
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context)
			throws IOException {
	}

}
