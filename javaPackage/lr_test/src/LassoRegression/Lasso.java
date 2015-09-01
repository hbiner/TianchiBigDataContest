package LassoRegression;
import Jama.Matrix;


/**
 * 
 * @author nkryBqY8
 *
 */
public class Lasso {

	private Matrix matOutputW ;
	 private double fEin ;
	 private int nDimension ;
	 private int nIteratorN ;
	 private int nDataN ;
	
	 private Matrix  matFeatureLabel ; 
	 public Matrix getMatFeatureLabel() {
		return matFeatureLabel;
	}

	public void setMatFeatureLabel(Matrix matFeatureLabel) {
		this.matFeatureLabel = matFeatureLabel;
	}

	public Matrix getMatOutputW() {
		return matOutputW;
	}


	public double getfEin() {
		return fEin;
	}


	public int getnDimension() {
		return nDimension;
	}


	public int getnIteratorN() {
		return nIteratorN;
	}


	public int getnDataN() {
		return nDataN;
	}



    /**
     * construct
     * @param A		Matrix for training
     */
      public Lasso (Matrix A){
	    	this.matFeatureLabel = A;
	    	this.nDataN = A .getRowDimension();
	    	this.nDimension = A .getColumnDimension() -1 ; //minus the label column
	        this.nIteratorN = 0 ;
	    	matOutputW = new Matrix( this.nDimension,1,0) ;
	    	 	
      }
    
   /**
    * theta of s = 1/(1 + EXP(s) )
    * @param s	score
    * @return 	1/(1 + EXP(s) )
    */
    public double theta(double s ){
    	return (double) 1.0/(1 +  Math.exp(s) );
    }

/**
 * stochastic gradient descent
 * @param iter  
 * @param eta	learning rate
 * @return
 */
    public void SGD(int iter ,double eta ){
    	double _delta; 
    	int [] _colIndexs  = new int[this.nDimension ];
    	double _y ;
    	// initialize
    	for ( int i  = 0; i <_colIndexs.length ; i++){
    		_colIndexs[i] = i;
    	}
    	
    	if( iter > this.nDataN) iter = this.nDataN ; // if iter is bigger than the number of data 
    	
    	for ( int i = 0 ; i<iter ; i ++ )
    	{
    		// get the label
    		_y = this.matFeatureLabel.get(i, this.nDimension ) ;
    				
     		// theta(-y*W*X)	  
    	   _delta = theta (
    			   -1*_y*this.matOutputW.times( 
    			   this.matFeatureLabel.getMatrix(i,i, _colIndexs)).get(0, 0)
    			   );
    		
    		this.matOutputW = this.matOutputW .plus(
    				this.matFeatureLabel.getMatrix(i,i, _colIndexs).times( _y * eta * _delta ).transpose() 
    				);
    	}	
    	
    	
    }

    public void calculEin(){
    	int [] _colIndexs  = new int[this.nDimension ];
    	double _y ;
    	double _theta ;
    	double _error = 0. ;
    	// initialize
    	for ( int i  = 0; i <_colIndexs.length ; i++){
    		_colIndexs[i] = i;
    	}
        	
    	for ( int i = 0 ; i<this.nDataN ; i ++ )
    	{
    		// label
    		_y = this.matFeatureLabel.get(i, this.nDimension) ;
    				
     		// theta(-y*W*X)	  
    		_theta = theta (
    			   _y*this.matOutputW.times( 
    			   this.matFeatureLabel.getMatrix(i,i, _colIndexs)).get(0, 0)
    			   );
    		
    	    _error  = _error - Math.log(_theta) ;	
    	}
    	
     this.fEin = _error / this.nDataN ;
    }
      
}
