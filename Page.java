//import java.io.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.*;
import java.lang.*;




public class Page implements java.io.Serializable {

    Vector<Hashtable<String, Object> > tuples;
    int page_number;
    Object min_value;
    Object max_value;
    int row_count;
    int max_number_of_rows;//change later to be determined by config file
    boolean isFull=false;

    public Page(int page_number, Object min_value, Object max_value, Vector<Hashtable<String, Object>> tuples, int row_count)
    throws DBAppException{
        this.page_number = page_number;
        this.min_value = min_value;
        this.max_value = max_value;
        this.tuples = tuples;
        this.row_count = row_count;
       try{
           Properties props= new Properties();
           InputStream input= new FileInputStream("./src/main/resources/DBApp.config");
         //  InputStream input= getClass().getClassLoader().getResourceAsStream("DBApp.config");
           props.load(input);
           max_number_of_rows= Integer.parseInt(props.getProperty("MaximumRowsCountinTablePage"));
       } catch (FileNotFoundException e) {
           throw new DBAppException("Config file not found");
       } catch (IOException e) {
           throw new DBAppException("Failure loading config file");
       }


    }

    public boolean compareToMax(Object o) throws DBAppException{

        //oolean in_page= false;

        if (o instanceof java.lang.String) {
            ((String) o).toLowerCase();
            if (this.max_value.toString().compareTo(o.toString())>0) {
                return true;
            }
            else if(this.max_value.toString().compareTo(o.toString())<0){
                return false;
            }
            else {
                throw new DBAppException("Duplicate value for clustering key");
            }
        }
        else if (o instanceof java.lang.Integer){
            if ((int)this.max_value > (int)o){
                return true;
            }
            else if((int)this.max_value <(int) o){
                return false;
            }
            else {
                throw new DBAppException("Duplicate value for clustering key");
            }
        }

        else if (o instanceof java.lang.Double){
            if(Double.compare((Double)this.max_value, (Double) o)>0){
                return true;
            }
            else if(Double.compare((Double)this.max_value, (Double) o)<0){
                return false;
            }
            else {
                throw new DBAppException("Duplicate value for clustering key");
            }
        }

        else {
            if (((Date)this.max_value).compareTo((Date) o)>0){
                return true;
            }
            else if (((Date)this.max_value).compareTo((Date) o)<0){
                return false;
            }
            else {
                throw new DBAppException("Duplicate value for clustering key");
            }
        }
       // return in_page;
    }

    public boolean greaterThanOrEqual(Object o){
        boolean greater=true;
        if (o instanceof java.lang.String) {

            ((String) o).toLowerCase();
            if(this.max_value.toString().compareTo(o.toString())<0){
                greater=false;
            }
        }
        else if (o instanceof java.lang.Integer){

            if((int)this.max_value <(int) o){
                greater=false;
            }
        }

        else if (o instanceof java.lang.Double){

            if(Double.compare((Double)this.max_value, (Double) o)<0){
                greater= false;
            }
        }

        else {
            if (((Date)this.max_value).compareTo((Date) o)<0){
                greater=false;
            }
        }
        return greater;
    }

    public boolean compareToMin(Object o) throws DBAppException{

        boolean new_min= false;

        if (o instanceof java.lang.String) {
            if (this.min_value.toString().compareTo(o.toString())>0) {
                new_min=true;
            }
            else if(this.min_value.toString().compareTo(o.toString())<0){
                return false;
            }
            else {
                throw new DBAppException("Duplicate value for clustering key");
            }
        }
        else if (o instanceof java.lang.Integer){
            if ((int)this.min_value > (int)o){
                new_min=true;
            }
            else if((int)this.min_value <(int) o){
                return false;
            }
            else{
                throw new DBAppException("Duplicate value for clustering key");
            }
        }

        else if (o instanceof java.lang.Double){
            if(Double.compare((Double)this.min_value, (Double) o)>0){
                new_min=true;
            }
            else if(Double.compare((Double)this.min_value, (Double) o)<0){
                return false;
            }
            else {
                throw new DBAppException("Duplicate value for clustering key");
            }
        }

        else {
            if (((Date)this.min_value).compareTo((Date) o)>0){
                new_min=true;
            }
            else if (((Date)this.min_value).compareTo((Date) o)<0){
                return false;
            }
            else {
                throw new DBAppException("Duplicate value for clustering key");
            }
        }
        return new_min;
    }
}
