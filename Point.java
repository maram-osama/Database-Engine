
import java.util.Vector;
public class Point implements java.io.Serializable{

    Object x;
    Object y;
    Object z;

    Object pkValue;

    int pageNumber;

    Vector<Point> duplicates;



    public Point(Object x, Object y, Object z, int pageNumber, Object pkValue,Vector<Point> duplicates ){

        this.x=x;
        this.y=y;
        this.z=z;

        this.pageNumber=pageNumber;
        this.pkValue=pkValue;
        this.duplicates=duplicates;
    }
}
