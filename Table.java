import java.io.*;
import java.util.*;
import java.lang.*;
public class Table implements java.io.Serializable{
    String strTableName;
    String strClusteringKeyColumn;
    Hashtable<String,String> htblColNameType;
    Hashtable<String,String> htblColNameMin;
    Hashtable<String,String> htblColNameMax;

    int page_count;

    Vector<ArrayList<String>> colsWithIndices;
    ArrayList<String> indices;

    ArrayList<Integer> PageNumbers;

            public Table(String strTableName,
                         String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin,
                         Hashtable<String,String> htblColNameMax,
            int page_count, ArrayList<Integer> PageNumbers,Vector<ArrayList<String>> colsWithIndices, ArrayList<String> indices ){
                this.strTableName= strTableName;
                this.strClusteringKeyColumn=strClusteringKeyColumn;
                this.htblColNameType= htblColNameType;
                this.htblColNameMin= htblColNameMin;
                this.htblColNameMax=htblColNameMax;
                this.page_count=page_count;
                this.PageNumbers= PageNumbers;
                this.colsWithIndices=colsWithIndices;
                this.indices=indices;

            }
             public Page load_page(String page_number) throws DBAppException{
                     String fileName= strTableName + page_number + ".ser";
                     Page p= null;
                     try {
                        FileInputStream fileIn = new FileInputStream("./src/main/resources/Data/" + fileName);
                        ObjectInputStream in = new ObjectInputStream(fileIn);
                        p = (Page) in.readObject();
                        in.close();
                        fileIn.close();
                    }catch(IOException i) {
                        i.printStackTrace();
                         throw new DBAppException("Page file not found");
                    }catch(ClassNotFoundException e) {
                        e.printStackTrace();
                         throw new DBAppException("Page file can't be loaded");
                    }
                    return p;
             }

             public void save_page(Page page) throws DBAppException{
                 String fileName= strTableName + page.page_number + ".ser";
                 try {
                     FileOutputStream fileOut = new FileOutputStream("./src/main/resources/Data/"+fileName);
                     ObjectOutputStream out = new ObjectOutputStream(fileOut);
                     out.writeObject(page);
                     out.close();
                     fileOut.close();
                 }catch(IOException i) {
                     i.printStackTrace();
                     throw new DBAppException("Failure in saving page");
                 }

             }




}
