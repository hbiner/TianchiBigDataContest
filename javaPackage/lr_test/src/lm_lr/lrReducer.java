package lm_lr ;

import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;

import Jama.*;
import LassoRegression.*;


public class lrReducer extends ReducerBase {
    Record output;
	
	@Override
	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {
		ArrayList<double[]>  _rowSetForConstructMatrix = new ArrayList<double []>();
		double [] _row ;
		
		while (values.hasNext()) {
			// TODO process value
			Record val =values.next();
			_row = new double[val.getColumnCount() + 1] ; // add the X0
			_row[0] = 1.0;
			_row[2] = val.getBigint("n_click") ;
			_row[3] = val.getBigint("n_buy");
			_row[4] = val.getBigint("n_collect");
			_row[5] = val.getBigint("n_basket");
			_rowSetForConstructMatrix.add(_row);
			
		}
		// construc the matrix
		//int countRow  = _rowSetForConstructMatrix.size() ;
		double [][]M = new double [ _rowSetForConstructMatrix.size() ][ 4 ] ;
		for ( int i = 0 ; i < M.length ; i++){
			M[i] = _rowSetForConstructMatrix.get(i) ;
		}
		Matrix featureAndLabelMatrix = new Matrix (M);
		// SGD
		Lasso DoIt = new Lasso(featureAndLabelMatrix) ;
		DoIt.SGD(400, -0.1126);
		//
		output.set(0 , key.getString("user_id") );
		output.set(1 , key.getString("brand_id"));
		// W
		 output.set(2 , DoIt.getMatOutputW().get(0, 0) ); 
		 output.set(3 , DoIt.getMatOutputW().get(1, 0) ); 
		 output.set(4 , DoIt.getMatOutputW().get(2, 0) ); 
		 output.set(5 , DoIt.getMatOutputW().get(3, 0) ); 
		 output.set(6 , DoIt.getMatOutputW().get(4, 0) ); 
		 
		 // Ein
		 DoIt.calculEin();
		 output.set(7,DoIt.getfEin());
		  
		 context.write(output);
		 	
	}

}
