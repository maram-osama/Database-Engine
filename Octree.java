import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
public class Octree implements java.io.Serializable{

    //each node contains 8 subdivisions
    //class for node and another for its subnodes?

        Vector<Octree> Octants;

        Vector<Point> Points;

        Object minX;
        Object maxX;

        Object minY;
        Object maxY;

        Object minZ;
        Object maxZ;


        Object midX;

        Object midY;

        Object midZ;

        int entries;

        int MaximumEntriesinOctreeNode;


        String indexName;

        String colX;

        String colY;

        String colZ;






        public Octree(Vector<Octree> Octants, Vector<Point> Points, Object minX, Object maxX,Object midX, Object minY, Object maxY,
                      Object midY, Object minZ, Object maxZ,  Object midZ, String indexName, int entries, String colX,
                      String colY, String colZ) throws DBAppException{
                this.Octants=Octants;
                this.Points=Points;

                this.minX=minX;
                this.maxX= maxX;

                this.minY=minY;
                this.maxY= maxY;

                this.minZ=minZ;
                this.maxZ= maxZ;

                this.midX=midX;

                this.midY=midY;

                this.midZ=midZ;

                this.indexName=indexName;
                this.entries=entries;

                this.colX=colX;
                this.colY=colY;
                this.colZ=colZ;


                try{
                        Properties props= new Properties();
                        InputStream input= new FileInputStream("./src/main/resources/DBApp.config");
                        //  InputStream input= getClass().getClassLoader().getResourceAsStream("DBApp.config");
                        props.load(input);
                        MaximumEntriesinOctreeNode= Integer.parseInt(props.getProperty("MaximumEntriesinOctreeNode"));
                } catch (FileNotFoundException e) {
                        throw new DBAppException("Config file not found");
                } catch (IOException e) {
                        throw new DBAppException("Failure loading config file");
                }

        }




}
