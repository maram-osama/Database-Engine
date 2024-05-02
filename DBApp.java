import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.lang.*;
import java.text.SimpleDateFormat;
import java.time.Period;

import java.time.LocalDate;
import java.time.ZoneId;
import java.lang.Math.*;

public class DBApp implements java.io.Serializable {


    public void init() {
    }

    public void createTable(String strTableName, String StrClusterKeyColumn, Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException {


        ArrayList<Object> metadata_columns = new ArrayList<>();
        String defaultName = "null";
        String defaultType = "null";

        Set<String> typeSet = htblColNameType.keySet();
        Iterator<String> typeItr = typeSet.iterator();

        if (htblColNameType.get(StrClusterKeyColumn) == null) {
            throw new DBAppException("Primary key can't be null");
        }
        if (!(create_valid_input(strTableName, htblColNameType, htblColNameMin, htblColNameMax))) {
            throw new DBAppException("Table data isn't valid");
        }

        Table table = new Table(strTableName, StrClusterKeyColumn, htblColNameType, htblColNameMin, htblColNameMax, 0, new ArrayList<>(),
                new Vector<ArrayList<String>>(), new ArrayList<String>());
        String fileName = strTableName + ".ser";
        try {
            FileOutputStream fileOut = new FileOutputStream("./src/main/resources/Data/" + fileName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(table);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
            throw new DBAppException("Couldn't save table" + fileName);
        }

        while (typeItr.hasNext()) {

            String record;
            String typeKey = typeItr.next();

            if (typeKey.equals(StrClusterKeyColumn)) {
                record = strTableName + ", " + StrClusterKeyColumn + ", " + htblColNameType.get(typeKey) + ", " + "True" + ", " +
                        defaultName + ", " + defaultType + ", " + htblColNameMin.get(typeKey).toLowerCase() + ", " +
                        htblColNameMax.get(typeKey).toLowerCase() + "\n";
                metadata_columns.add(record);
            } else {
                record = strTableName + ", " + typeKey + ", " + htblColNameType.get(typeKey) + ", " + "False" + ", " +
                        defaultName + ", " + defaultType + ", " + htblColNameMin.get(typeKey).toLowerCase() + ", " +
                        htblColNameMax.get(typeKey).toLowerCase() + "\n";
                metadata_columns.add(record);
            }
        }
        write_to_metadata(metadata_columns);

    }

    // following method creates an octree
// depending on the count of column names passed.
// If three column names are passed, create an octree.
// If only one or two column names is passed, throw an Exception. public void createIndex(String strTableName,
    public void createIndex(String strTableName,
                            String[] strarrColName) throws DBAppException {
        //validate data, if table exists, if columns exist + if they're 3 columns

        if (!(create_valid_index(strTableName, strarrColName))) {
            throw new DBAppException("Index isn't valid");
        }

        ArrayList<Object> min_max_mid_values1 = getMin_Max_Mid(strTableName,
                strarrColName[0]);

        ArrayList<Object> min_max_mid_values2 = getMin_Max_Mid(strTableName,
                strarrColName[1]);

        ArrayList<Object> min_max_mid_values3 = getMin_Max_Mid(strTableName,
                strarrColName[2]);

        String indexName = strarrColName[0] + strarrColName[1] + strarrColName[2] + "Index";
        Octree octree = new Octree(new Vector<Octree>(), new Vector<Point>(), min_max_mid_values1.get(0), min_max_mid_values1.get(1),
                min_max_mid_values1.get(2), min_max_mid_values2.get(0), min_max_mid_values2.get(1),
                min_max_mid_values2.get(2), min_max_mid_values3.get(0), min_max_mid_values3.get(1),
                min_max_mid_values3.get(2), indexName, 0, strarrColName[0], strarrColName[1], strarrColName[2]);

        save_index(octree);

        addIndextoCSV(strTableName,
                strarrColName, indexName);

        ArrayList<String> colsWithIndices = new ArrayList<>();
        colsWithIndices.add(strarrColName[0]);
        colsWithIndices.add(strarrColName[1]);
        colsWithIndices.add(strarrColName[2]);

        ArrayList<String> indices = new ArrayList<>();
        indices.add(indexName);


        Table table = load_table(strTableName);
        table.colsWithIndices.add(colsWithIndices);
        table.indices = indices;

        if (table.page_count > 0) {
            save_table(table);
            occupyIndex(strTableName, indexName, strarrColName);
        } else {
            save_table(table);
        }

    }

    public void testOctree(Octree octree) throws DBAppException {

        if (octree.Points.size() > 0) {
            for (int i = 0; i < octree.Points.size(); i++) {

                System.out.println("Index name " + octree.indexName);
                System.out.println("Point " + (i + 1) + octree.colX + " = " + octree.Points.get(i).x.toString());
                System.out.println(octree.colY + " = " + octree.Points.get(i).y.toString());
                System.out.println(octree.colZ + " = " + octree.Points.get(i).z.toString());
                System.out.println(" MINX= " + octree.minX.toString());
                System.out.println(" MAXX= " + octree.maxX.toString());
                System.out.println(" MidX= " + octree.midX.toString());

                System.out.println(" MINZ= " + octree.minZ.toString());
                System.out.println(" MAXZ= " + octree.maxZ.toString());
                System.out.println(" MidZ= " + octree.midZ.toString());

                System.out.println("Page number: " + octree.Points.get(i).pageNumber);

                System.out.println("PK Value: " + octree.Points.get(i).pkValue.toString());
            }
            save_index(octree);
        } else {
            for (int j = 0; j < octree.Octants.size(); j++) {
                String indexName = octree.indexName + (j + 1);
                save_index(octree);

                Octree oct = load_index(indexName);
                testOctree(oct);

            }
        }
    }

    public void tester(Table table) throws DBAppException {
        System.out.println("Table page count: " + table.page_count + "Table name " + table.strTableName);
        for (int m = 0; m < table.PageNumbers.size(); m++) {
            System.out.println("Page numbers: " + table.PageNumbers.get(m));
        }
        for (int i = 0; i < table.page_count; i++) {
            Page page = table.load_page(i + 1 + "");
            System.out.println("Page number: " + page.page_number);
            System.out.println("Max number of rows: " + page.max_number_of_rows);
            System.out.println("Row count: " + page.row_count + "\n" + "ISfull :" + page.isFull + "\n" + "Min value" + page.min_value.toString()
                    + "Max Value" + page.max_value.toString());

            for (int j = 0; j < page.tuples.size(); j++) {
                System.out.println("Tuple number " + (j + 1) + page.tuples.get(j).toString());
            }
            table.save_page(page);

        }
    }

    public void occupyIndex(String strTableName, String indexName, String[] strarrColName) throws DBAppException {
        // loop on pages
        //for each record, create a point for it,
        //insert it into octee
        //if octree is full, run same method for octree's appropriate octant

        String col1 = strarrColName[0];
        String col2 = strarrColName[1];
        String col3 = strarrColName[2];


        Table table = load_table(strTableName);

        for (int i = 1; i <= table.page_count; i++) {

            Page page = table.load_page(i + "");

            for (int j = 0; j < page.row_count; j++) {

                Octree octree = load_index(indexName);

                Hashtable<String, Object> tuple = page.tuples.get(j);


                Object x = tuple.get(col1);
                Object y = tuple.get(col2);
                Object z = tuple.get(col3);

                Object pkValue = tuple.get(table.strClusteringKeyColumn);

                insertToOctree(octree, i, x, y, z, col1, col2, col3, pkValue);

                System.out.println("in occupyindex, first inserted tuple is " + col1 + x.toString() + " " +
                        col2 + y.toString() + " " + col3 + z.toString());

                octree = load_index(indexName);


            }
        }


    }

    public boolean equals(Object a, Object b) {
        if (a instanceof java.lang.String && b instanceof java.lang.String) {
            ((String) a).toLowerCase();
            ((String) b).toLowerCase();
            if (a.toString().compareTo(b.toString()) == 0) {
                return true;
            }
        } else if (a instanceof java.lang.Integer && b instanceof java.lang.Integer) {

            if ((int) a == (int) b) {
                return true;
            }
        } else if (a instanceof java.lang.Double && b instanceof java.lang.Double) {

            if (Double.compare((Double) a, (Double) b) == 0) {
                return true;
            }
        } else if (a instanceof java.util.Date && b instanceof java.util.Date) {
            if (((Date) a).compareTo((Date) b) == 0) {
                return true;
            }
        }
        return false;
    }

    public void insertToOctree(Octree octree, int pageNumber, Object x,
                               Object y, Object z, String col1, String col2, String col3, Object pkValue) throws DBAppException {

        if (octree.entries < octree.MaximumEntriesinOctreeNode && octree.Octants.isEmpty()) { //check for duplicates


            for (int i = 0; i < octree.Points.size(); i++) {
                Point p = octree.Points.get(i);

                if (equals(p.x, x) && equals(p.y, y) && equals(p.z, z)) {

                    Point point = new Point(x, y, z, pageNumber, pkValue, new Vector<Point>());
                    p.duplicates.add(point);
                    save_index(octree);
                    return;
                }
            }

            Point point = new Point(x, y, z, pageNumber, pkValue, new Vector<Point>());


            octree.entries++;
            octree.Points.add(point);
            save_index(octree);

            System.out.println("in insert to octree, tuple inserted is  " + col1 + x.toString() +
                    col2 + y.toString() + col3 + z.toString());

            return;
        } else {
            if (octree.Octants.size() == 0) { //split octree into 8 octants

                Object midXOct1 = getMid(octree.minX, octree.midX);
                Object midXOct2 = midXOct1;
                Object midXOct5 = midXOct1;
                Object midXOct6 = midXOct1;

                Object midXOct3 = getMid(octree.midX, octree.maxX);
                Object midXOct4 = midXOct3;
                Object midXOct7 = midXOct3;
                Object midXOct8 = midXOct3;

                Object midYOct1 = getMid(octree.minY, octree.midY);
                Object midYOct3 = midYOct1;
                Object midYOct5 = midYOct1;
                Object midYOct7 = midYOct1;

                Object midYOct2 = getMid(octree.midY, octree.maxY);
                Object midYOct4 = midYOct2;
                Object midYOct6 = midYOct2;
                Object midYOct8 = midYOct2;

                Object midZOct1 = getMid(octree.minZ, octree.midZ);
                Object midZOct2 = midZOct1;
                Object midZOct3 = midZOct1;
                Object midZOct4 = midZOct1;

                Object midZOct5 = getMid(octree.midZ, octree.maxZ);
                Object midZOct6 = midZOct5;
                Object midZOct7 = midZOct5;
                Object midZOct8 = midZOct5;

                Vector<Point> pointsOct1 = getPointsOct1(octree.Points, octree.minX, octree.midX,
                        octree.minY, octree.midY, octree.minZ, octree.midZ);

                String destinationOCT = "";

                Octree oct1 = new Octree(new Vector<Octree>(), pointsOct1, octree.minX, octree.midX, midXOct1,
                        octree.minY, octree.midY, midYOct1, octree.minZ, octree.midZ, midZOct1, octree.indexName + "1",
                        pointsOct1.size(), col1, col2, col3);

                for (int r = 0; r < pointsOct1.size(); r++) {

                    Point t = pointsOct1.get(r);
                    System.out.println("points in octant 1 " + col1 + t.x.toString() +
                            " " + col2 + t.y.toString() + " " + col3 + t.z.toString());

                }

                if (isOct1(octree.minX, octree.midX, octree.minY, octree.midY,
                        octree.minZ, octree.midZ, x, y, z)) {
                    destinationOCT = oct1.indexName;
                }

                octree.Octants.add(oct1);

                save_index(oct1);

                Vector<Point> pointsOct2 = getPointsOct2(octree.Points, octree.minX, octree.midX,
                        octree.midY, octree.maxY, octree.minZ, octree.midZ);

                Octree oct2 = new Octree(new Vector<Octree>(), pointsOct2, octree.minX, octree.midX, midXOct2,
                        octree.midY, octree.maxY, midYOct2, octree.minZ, octree.midZ, midZOct2, octree.indexName + "2",
                        pointsOct2.size(), col1, col2, col3);

                for (int r = 0; r < pointsOct2.size(); r++) {

                    Point t = pointsOct2.get(r);
                    System.out.println("points in octant 2 " + col1 + t.x.toString() +
                            " " + col2 + t.y.toString() + " " + col3 + t.z.toString());

                }

                if (isOct2(octree.minX, octree.midX, octree.midY, octree.maxY,
                        octree.minZ, octree.midZ, x, y, z)) {
                    destinationOCT = oct2.indexName;
                }

                octree.Octants.add(oct2);

                save_index(oct2);

                Vector<Point> pointsOct3 = getPointsOct3(octree.Points, octree.midX, octree.maxX,
                        octree.minY, octree.midY, octree.minZ, octree.midZ);

                Octree oct3 = new Octree(new Vector<Octree>(), pointsOct3, octree.midX, octree.maxX, midXOct3,
                        octree.minY, octree.midY, midYOct3, octree.minZ, octree.midZ, midZOct3, octree.indexName + "3",
                        pointsOct3.size(), col1, col2, col3);

                for (int r = 0; r < pointsOct3.size(); r++) {

                    Point t = pointsOct3.get(r);
                    System.out.println("points in octant 3 " + col1 + t.x.toString() +
                            " " + col2 + t.y.toString() + " " + col3 + t.z.toString());

                }

                if (isOct3(octree.midX, octree.maxX, octree.minY, octree.midY,
                        octree.minZ, octree.midZ, x, y, z)) {
                    destinationOCT = oct3.indexName;
                }

                octree.Octants.add(oct3);

                save_index(oct3);

                Vector<Point> pointsOct4 = getPointsOct4(octree.Points, octree.midX, octree.maxX,
                        octree.midY, octree.maxY, octree.minZ, octree.midZ);

                Octree oct4 = new Octree(new Vector<Octree>(), pointsOct3, octree.midX, octree.maxX, midXOct4,
                        octree.midY, octree.maxY, midYOct4, octree.minZ, octree.midZ, midZOct4, octree.indexName + "4",
                        pointsOct4.size(), col1, col2, col3);


                for (int r = 0; r < pointsOct4.size(); r++) {

                    Point t = pointsOct4.get(r);
                    System.out.println("points in octant 4 " + col1 + t.x.toString() +
                            " " + col2 + t.y.toString() + " " + col3 + t.z.toString());

                }

                if (isOct4(octree.midX, octree.maxX, octree.midY, octree.maxY,
                        octree.minZ, octree.midZ, x, y, z)) {
                    destinationOCT = oct4.indexName;
                }

                octree.Octants.add(oct4);

                save_index(oct4);

                Vector<Point> pointsOct5 = getPointsOct5(octree.Points, octree.minX, octree.midX,
                        octree.minY, octree.midY, octree.midZ, octree.maxZ);

                Octree oct5 = new Octree(new Vector<Octree>(), pointsOct5, octree.minX, octree.midX, midXOct5,
                        octree.minY, octree.midY, midYOct5, octree.midZ, octree.maxZ, midZOct5, octree.indexName + "5",
                        pointsOct5.size(), col1, col2, col3);


                for (int r = 0; r < pointsOct5.size(); r++) {

                    Point t = pointsOct5.get(r);
                    System.out.println("points in octant 5 " + col1 + t.x.toString() +
                            " " + col2 + t.y.toString() + " " + col3 + t.z.toString());

                }

                if (isOct5(octree.minX, octree.midX, octree.minY, octree.midY,
                        octree.midZ, octree.maxZ, x, y, z)) {
                    destinationOCT = oct5.indexName;
                }

                octree.Octants.add(oct5);

                save_index(oct5);

                Vector<Point> pointsOct6 = getPointsOct6(octree.Points, octree.minX, octree.midX,
                        octree.midY, octree.maxY, octree.midZ, octree.maxZ);

                Octree oct6 = new Octree(new Vector<Octree>(), pointsOct6, octree.minX, octree.midX, midXOct6,
                        octree.midY, octree.maxY, midYOct6, octree.midZ, octree.maxZ, midZOct6, octree.indexName + "6",
                        pointsOct6.size(), col1, col2, col3);

                for (int r = 0; r < pointsOct6.size(); r++) {

                    Point t = pointsOct6.get(r);
                    System.out.println("points in octant 6 " + col1 + t.x.toString() +
                            " " + col2 + t.y.toString() + " " + col3 + t.z.toString());

                }

                if (isOct6(octree.minX, octree.midX, octree.midY, octree.maxY,
                        octree.midZ, octree.maxZ, x, y, z)) {
                    destinationOCT = oct6.indexName;
                }

                octree.Octants.add(oct6);

                save_index(oct6);

                Vector<Point> pointsOct7 = getPointsOct7(octree.Points, octree.midX, octree.maxX,
                        octree.minY, octree.midY, octree.midZ, octree.maxZ);

                Octree oct7 = new Octree(new Vector<Octree>(), pointsOct7, octree.midX, octree.maxX, midXOct7,
                        octree.minY, octree.midY, midYOct7, octree.midZ, octree.maxZ, midZOct7, octree.indexName + "7",
                        pointsOct7.size(), col1, col2, col3);


                for (int r = 0; r < pointsOct7.size(); r++) {

                    Point t = pointsOct7.get(r);
                    System.out.println("points in octant 7 " + col1 + t.x.toString() +
                            " " + col2 + t.y.toString() + " " + col3 + t.z.toString());

                }

                if (isOct7(octree.midX, octree.maxX, octree.minY, octree.midY,
                        octree.midZ, octree.maxZ, x, y, z)) {
                    destinationOCT = oct7.indexName;
                }

                octree.Octants.add(oct7);

                save_index(oct7);

                Vector<Point> pointsOct8 = getPointsOct8(octree.Points, octree.midX, octree.maxX,
                        octree.midY, octree.maxY, octree.midZ, octree.maxZ);

                Octree oct8 = new Octree(new Vector<Octree>(), pointsOct8, octree.midX, octree.maxX, midXOct8,
                        octree.midY, octree.maxY, midYOct8, octree.midZ, octree.maxZ, midZOct8, octree.indexName + "8",
                        pointsOct8.size(), col1, col2, col3);

                for (int r = 0; r < pointsOct8.size(); r++) {

                    Point t = pointsOct8.get(r);
                    System.out.println("points in octant 8 " + col1 + t.x.toString() +
                            " " + col2 + t.y.toString() + " " + col3 + t.z.toString());

                }

                if (isOct8(octree.midX, octree.maxX, octree.midY, octree.maxY,
                        octree.midZ, octree.maxZ, x, y, z)) {
                    destinationOCT = oct8.indexName;
                }

                octree.Octants.add(oct8);

                save_index(oct8);


                octree.Points.removeAllElements();
                save_index(octree);

                Octree destination = load_index(destinationOCT);

                insertToOctree(destination, pageNumber, x, y, z, col1, col2, col3, pkValue);

                System.out.println("in insert to octree, just created octants, tuple inserted is  in oct" + destinationOCT + col1 + x.toString() +
                        col2 + y.toString() + col3 + z.toString());


                //we still haven't inserted out point yet
                //call inserttooctree on correct suboctree


            } else { //search for correct octant to insert in and repeat loop
                int octantNumber = getOctant(x, y, z, octree);
                String indexName = octree.indexName;
                save_index(octree);

                octree = load_index(indexName + octantNumber);

                insertToOctree(octree, pageNumber, x, y, z, col1, col2, col3, pkValue);
            }

        }
    }

    public boolean isOct1(Object minX, Object maxX, Object minY, Object maxY,
                          Object minZ, Object maxZ, Object pointX, Object pointY, Object pointZ) {

        if (!(lessThan(pointX, minX)) && lessThan(pointX, maxX) && !(lessThan(pointY, minY)) && lessThan(pointY, maxY)
                && !(lessThan(pointZ, minZ)) && lessThan(pointZ, maxZ)) {


            return true;
        }
        return false;

    }

    public boolean isOct2(Object minX, Object maxX, Object minY, Object maxY,
                          Object minZ, Object maxZ, Object pointX, Object pointY, Object pointZ) {

        if (!(lessThan(pointX, minX)) && lessThan(pointX, maxX) && !(lessThan(pointY, minY)) && !(greaterThan(pointY, maxY))
                && !(lessThan(pointZ, minZ)) && lessThan(pointZ, maxZ)) {
            return true;
        }
        return false;

    }

    public boolean isOct3(Object minX, Object maxX, Object minY, Object maxY,
                          Object minZ, Object maxZ, Object pointX, Object pointY, Object pointZ) {

        if (!(lessThan(pointX, minX)) && !(greaterThan(pointX, maxX)) && !(lessThan(pointY, minY)) && lessThan(pointY, maxY)
                && !(lessThan(pointZ, minZ)) && lessThan(pointZ, maxZ)) {
            return true;
        }
        return false;

    }

    public boolean isOct4(Object minX, Object maxX, Object minY, Object maxY,
                          Object minZ, Object maxZ, Object pointX, Object pointY, Object pointZ) {

        if (!(lessThan(pointX, minX)) && !(greaterThan(pointX, maxX)) && !(lessThan(pointY, minY)) && !(greaterThan(pointY, maxY))
                && !(lessThan(pointZ, minZ)) && lessThan(pointZ, maxZ)) {
            return true;
        }
        return false;

    }

    public boolean isOct5(Object minX, Object maxX, Object minY, Object maxY,
                          Object minZ, Object maxZ, Object pointX, Object pointY, Object pointZ) {

        if (!(lessThan(pointX, minX)) && lessThan(pointX, maxX) && !(lessThan(pointY, minY)) && lessThan(pointY, maxY)
                && !(lessThan(pointZ, minZ)) && !(greaterThan(pointZ, maxZ))) {
            return true;
        }
        return false;

    }

    public boolean isOct6(Object minX, Object maxX, Object minY, Object maxY,
                          Object minZ, Object maxZ, Object pointX, Object pointY, Object pointZ) {

        if (!(lessThan(pointX, minX)) && lessThan(pointX, maxX) && !(lessThan(pointY, minY)) && !(greaterThan(pointY, maxY))
                && !(lessThan(pointZ, minZ)) && !(greaterThan(pointZ, maxZ))) {
            return true;
        }
        return false;

    }

    public boolean isOct7(Object minX, Object maxX, Object minY, Object maxY,
                          Object minZ, Object maxZ, Object pointX, Object pointY, Object pointZ) {

        if (!(lessThan(pointX, minX)) && !(greaterThan(pointX, maxX)) && !(lessThan(pointY, minY)) && lessThan(pointY, maxY)
                && !(lessThan(pointZ, minZ)) && !(greaterThan(pointZ, maxZ))) {
            return true;
        }
        return false;

    }

    public boolean isOct8(Object minX, Object maxX, Object minY, Object maxY,
                          Object minZ, Object maxZ, Object pointX, Object pointY, Object pointZ) {

        if (!(lessThan(pointX, minX)) && !(greaterThan(pointX, maxX)) && !(lessThan(pointY, minY)) && !(greaterThan(pointY, maxY))
                && !(lessThan(pointZ, minZ)) && !(greaterThan(pointZ, maxZ))) {
            return true;
        }
        return false;

    }


    public Vector<Point> getPointsOct1(Vector<Point> points, Object minX, Object maxX, Object minY, Object maxY,
                                       Object minZ, Object maxZ) {

        Vector<Point> pointsOct = new Vector<>();

        for (int i = 0; i < points.size(); i++) {

            Point point = points.get(i);

            if ((!(lessThan(point.x, minX))) && lessThan(point.x, maxX) && (!(lessThan(point.y, minY))) && lessThan(point.y, maxY)
                    && (!(lessThan(point.z, minZ))) && lessThan(point.z, maxZ)) {
                pointsOct.add(point);

                System.out.println("in getpointsoct1, inserted point is " + "x= " + point.x.toString()
                        + "y= " + point.y.toString() + "z= " + point.z.toString());
            }

        }
        return pointsOct;
    }

    public Vector<Point> getPointsOct2(Vector<Point> points, Object minX, Object maxX, Object minY, Object maxY,
                                       Object minZ, Object maxZ) {

        Vector<Point> pointsOct = new Vector<>();

        for (int i = 0; i < points.size(); i++) {

            Point point = points.get(i);

            if (!(lessThan(point.x, minX)) && lessThan(point.x, maxX) && !(lessThan(point.y, minY)) && !(greaterThan(point.y, maxY))
                    && !(lessThan(point.z, minZ)) && lessThan(point.z, maxZ)) {
                pointsOct.add(point);
            }

        }
        return pointsOct;
    }

    public Vector<Point> getPointsOct3(Vector<Point> points, Object minX, Object maxX, Object minY, Object maxY,
                                       Object minZ, Object maxZ) {

        Vector<Point> pointsOct = new Vector<>();

        for (int i = 0; i < points.size(); i++) {

            Point point = points.get(i);

            if (!(lessThan(point.x, minX)) && !(greaterThan(point.x, maxX)) && !(lessThan(point.y, minY)) && lessThan(point.y, maxY)
                    && !(lessThan(point.z, minZ)) && lessThan(point.z, maxZ)) {
                pointsOct.add(point);
            }

        }
        return pointsOct;
    }

    public Vector<Point> getPointsOct4(Vector<Point> points, Object minX, Object maxX, Object minY, Object maxY,
                                       Object minZ, Object maxZ) {

        Vector<Point> pointsOct = new Vector<>();

        for (int i = 0; i < points.size(); i++) {

            Point point = points.get(i);

            if (!(lessThan(point.x, minX)) && !(greaterThan(point.x, maxX)) && !(lessThan(point.y, minY)) && !(greaterThan(point.y, maxY))
                    && !(lessThan(point.z, minZ)) && lessThan(point.z, maxZ)) {
                pointsOct.add(point);
            }

        }
        return pointsOct;
    }


    public Vector<Point> getPointsOct5(Vector<Point> points, Object minX, Object maxX, Object minY, Object maxY,
                                       Object minZ, Object maxZ) {

        Vector<Point> pointsOct = new Vector<>();

        for (int i = 0; i < points.size(); i++) {

            Point point = points.get(i);

            if (!(lessThan(point.x, minX)) && lessThan(point.x, maxX) && !(lessThan(point.y, minY)) && lessThan(point.y, maxY)
                    && !(lessThan(point.z, minZ)) && !(greaterThan(point.z, maxZ))) {
                pointsOct.add(point);
            }

        }
        return pointsOct;
    }

    public Vector<Point> getPointsOct6(Vector<Point> points, Object minX, Object maxX, Object minY, Object maxY,
                                       Object minZ, Object maxZ) {

        Vector<Point> pointsOct = new Vector<>();

        for (int i = 0; i < points.size(); i++) {

            Point point = points.get(i);

            if (!(lessThan(point.x, minX)) && lessThan(point.x, maxX) && !(lessThan(point.y, minY)) && !(greaterThan(point.y, maxY))
                    && !(lessThan(point.z, minZ)) && !(greaterThan(point.z, maxZ))) {
                pointsOct.add(point);
            }

        }
        return pointsOct;
    }

    public Vector<Point> getPointsOct7(Vector<Point> points, Object minX, Object maxX, Object minY, Object maxY,
                                       Object minZ, Object maxZ) {

        Vector<Point> pointsOct = new Vector<>();

        for (int i = 0; i < points.size(); i++) {

            Point point = points.get(i);

            if (!(lessThan(point.x, minX)) && !(greaterThan(point.x, maxX)) && !(lessThan(point.y, minY)) && lessThan(point.y, maxY)
                    && !(lessThan(point.z, minZ)) && !(greaterThan(point.z, maxZ))) {
                pointsOct.add(point);
            }

        }
        return pointsOct;
    }

    public Vector<Point> getPointsOct8(Vector<Point> points, Object minX, Object maxX, Object minY, Object maxY,
                                       Object minZ, Object maxZ) {

        Vector<Point> pointsOct = new Vector<>();

        for (int i = 0; i < points.size(); i++) {

            Point point = points.get(i);

            if (!(lessThan(point.x, minX)) && !(greaterThan(point.x, maxX)) && !(lessThan(point.y, minY)) && !(greaterThan(point.y, maxY))
                    && !(lessThan(point.z, minZ)) && !(greaterThan(point.z, maxZ))) {
                pointsOct.add(point);
            }

        }
        return pointsOct;
    }

    public Object getMid(Object min, Object max) {
        if (min instanceof java.lang.Integer && max instanceof java.lang.Integer) {
            int mid = (((int) max) - ((int) min)) / 2;
            return mid;
        }

        if (min instanceof java.lang.Double && max instanceof java.lang.Double) {
            Double mid = (((Double) max) - ((Double) min)) / 2.0;
            return mid;
        }

        if (min instanceof java.lang.String && max instanceof java.lang.String) {


            int toConcatinate = ((String) max).length() - ((String) min).length();

            if (toConcatinate != 0) {

                int index = ((String) max).length() - toConcatinate;


                min += ((String) max).substring(index, ((String) max).length());
            }


            String minlow = ((String) min).toLowerCase();
            String maxlow = ((String) max).toLowerCase();


            String mid = getMiddleString((String) min, (String) max, ((String) max).length());
            return mid;
        }

        if (min instanceof java.util.Date && max instanceof java.util.Date) {

            LocalDate minLocalDate = ((Date) min).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            LocalDate maxLocalDate = ((Date) max).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

            Period period = Period.between(minLocalDate, maxLocalDate);

            int years = (int) Math.ceil(period.getYears() / 2);
            int months = (int) Math.ceil(period.getMonths() / 2);
            int days = (int) Math.ceil(period.getDays() / 2);

            Period halfPeriod = Period.of(years, months, days);

            LocalDate midDate = minLocalDate.plus(halfPeriod);

            Date mid = Date.from(midDate.atStartOfDay(ZoneId.systemDefault()).toInstant());

            return mid;


        }
        return null;

    }


    public int getOctant(Object x, Object y, Object z, Octree octree) {
        //z
        if ((lessThan(z, octree.midZ))) {
            //lower half 1 2 3 4
            if ((lessThan(x, octree.midX))) {
                // 1 2
                if ((lessThan(y, octree.midY))) {
                    return 1;
                } else {
                    return 2;
                }
            } else {// 3 4
                if ((lessThan(y, octree.midY))) {
                    return 3;
                } else {
                    return 4;
                }
            }
        } else {//upper half

            if ((lessThan(x, octree.midX))) {
                // 5 6
                if ((lessThan(y, octree.midY))) {
                    return 5;
                } else {
                    return 6;
                }
            } else { //7 8

                if (lessThan(y, octree.midY)) {
                    return 7;
                } else {
                    return 8;
                }
            }
        }

    }

    public void addIndextoCSV(String strTableName,
                              String[] strarrColName, String indexName) throws DBAppException {

        ArrayList<String> metadata = read_from_metadata();


        for (int i = 0; i < strarrColName.length; i++) {
            for (int j = 0; j < metadata.size(); j++) {
                String row = metadata.get(j);
                String segments[] = row.split(", ");
                if (segments[0].equals(strTableName) && segments[1].equals(strarrColName[i])) {

                    segments[4] = indexName;
                    segments[5] = "Octree";

                    String newRow = "";
                    for (int n = 0; n < segments.length - 1; n++) {
                        newRow += segments[n] + ", ";
                    }

                    newRow += segments[segments.length - 1];


                    metadata.set(j, newRow);
                    continue;
                }

            }
        }

        try {

            FileOutputStream output = new FileOutputStream("./src/main/resources/metadata.csv", false);

            Iterator<String> itr = metadata.iterator();
            while (itr.hasNext()) {

                byte[] array = itr.next().toString().getBytes();
                output.write(array);
            }
            output.close();


        } catch (Exception e) {
            e.getStackTrace();
            throw new DBAppException("Error when writing metadata");
        }

    }

    public ArrayList<Object> getMin_Max_Mid(String strTableName, String strarrColName) throws DBAppException {

        ArrayList<String> metadata = read_from_metadata();

        ArrayList<Object> min_max_mid = new ArrayList<>();

        for (int i = 0; i < metadata.size(); i++) {
            String row = metadata.get(i);
            String segments[] = row.split(", ");

            if (segments[0].equals(strTableName) && segments[1].equals(strarrColName)) {

                if (segments[2].equals("java.lang.Integer")) {

                    int min = Integer.parseInt(segments[6]);
                    int max = Integer.parseInt(segments[7]);

                    int mid = (max - min) / 2;

                    min_max_mid.add(min);
                    min_max_mid.add(max);
                    min_max_mid.add(mid);
                    return min_max_mid;

                }

                if (segments[2].equals("java.lang.Double")) {

                    Double min = Double.parseDouble(segments[6]);
                    Double max = Double.parseDouble(segments[7]);

                    Double mid = (max - min) / 2.0;

                    min_max_mid.add(min);
                    min_max_mid.add(max);
                    min_max_mid.add(mid);
                    return min_max_mid;

                }

                if (segments[2].equals("java.lang.String")) {
                    String min = segments[6].toString();
                    String max = segments[7].toString();

                    int toConcatinate = max.length() - min.length();

                    if (toConcatinate != 0) {
                        int index = max.length() - toConcatinate;

                        min += max.substring(index, max.length());
                    }


                    String minlow = min.toLowerCase();
                    String maxlow = max.toLowerCase();

                    String mid = getMiddleString(minlow, maxlow, max.length());

                    min_max_mid.add(min);
                    min_max_mid.add(max);
                    min_max_mid.add(mid);
                    return min_max_mid;
                }

                if (segments[2].equals("java.util.Date")) { // ?????????
                    try {
                        Date min = new SimpleDateFormat("yyyy-MM-dd").parse(segments[6]);
                        Date max = new SimpleDateFormat("yyyy-MM-dd").parse(segments[7]);

                        LocalDate minLocalDate = ((Date) min).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                        LocalDate maxLocalDate = ((Date) max).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

                        Period period = Period.between(minLocalDate, maxLocalDate);

                        int years = (int) Math.ceil(period.getYears() / 2);
                        int months = (int) Math.ceil(period.getMonths() / 2);
                        int days = (int) Math.ceil(period.getDays() / 2);

                        Period halfPeriod = Period.of(years, months, days);

                        LocalDate midDate = minLocalDate.plus(halfPeriod);

                        Date mid = Date.from(midDate.atStartOfDay(ZoneId.systemDefault()).toInstant());


                        min_max_mid.add(min);
                        min_max_mid.add(max);
                        min_max_mid.add(mid);
                        return min_max_mid;


                    } catch (java.text.ParseException e) {
                        throw new DBAppException("Error parsing date");
                    }


                }

            }

        }
        return min_max_mid;

    }

    static String getMiddleString(String S, String T, int N) {
        // Stores the base 26 digits after addition


        int[] a1 = new int[N + 1];

        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int) S.charAt(i) - 97
                    + (int) T.charAt(i) - 97;
        }

        // Iterate from right to left
        // and add carry to next position
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int) a1[i] / 26;
            a1[i] %= 26;
        }

        // Reduce the number to find the middle
        // string by dividing each position by 2
        for (int i = 0; i <= N; i++) {

            // If current value is odd,
            // carry 26 to the next index value
            if ((a1[i] & 1) != 0) {

                if (i + 1 <= N) {
                    a1[i + 1] += 26;
                }
            }

            a1[i] = (int) a1[i] / 2;
        }

        String middle = "";

        for (int i = 1; i <= N; i++) {

            middle += (char) (a1[i] + 97);
            System.out.print((char) (a1[i] + 97));
        }

        return middle;


    }


    public Octree load_index(String strIndexName) throws DBAppException {
        String fileName = strIndexName + ".ser";
        Octree t = null;
        try {
            FileInputStream fileIn = new FileInputStream("./src/main/resources/Data/" + fileName);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            t = (Octree) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException i) {
            i.printStackTrace();
            throw new DBAppException("Index file not found");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new DBAppException("Index file can't be loaded");
        }
        return t;
    }

    public void save_index(Octree octree) throws DBAppException {
        String fileName = octree.indexName + ".ser";
        try {
            FileOutputStream fileOut = new FileOutputStream("./src/main/resources/Data/" + fileName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(octree);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
            throw new DBAppException("Failure in saving index");
        }

    }


    public boolean create_valid_index(String strTableName,
                                      String[] strarrColName) throws DBAppException {
        //3 columns
        //columns exist in table
        //no index has been created for any column
        //table exists


        if (strarrColName.length != 3) {
            return false;
        }
        int count = 0;

        for (int i = 0; i < strarrColName.length; i++) {
            ArrayList<String> metadata = read_from_metadata();

            for (int j = 0; j < metadata.size(); j++) {
                String row = metadata.get(j);
                String segments[] = row.split(", ");

                if (segments[0].equals(strTableName)) {
                    if (segments[1].equals(strarrColName[i])) {
                        count++;
                        if (!(segments[4].equals("null")) || !(segments[5].equals("null"))) {
                            return false;
                        }
                        continue;
                    }
                }
            }
        }
        if (count != 3) {
            return false;
        }

        return true;

    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
            throws DBAppException {

        //validate the select statement
        //check if any of columns have index created on them (from metadata)
        //otherwise search without index
        //do linear scanning, but check if search is done on pk
        //if it is, do binary search until reaching primary key, then check rest of columns for that record
        //otherwise, do linear search
        //case for and, or and xor?
        //switch cases for operator in sqlterms
        if (!(select_valid_input(arrSQLTerms, strarrOperators))) {
            throw new DBAppException("Data isn't valid");
        }


        //to know if i need index

        boolean hasIndex = false;
        boolean andOperators = true;

        String tableN = arrSQLTerms[0]._strTableName;

        ArrayList<String> sqltermCols = new ArrayList<String>();

        for (int j = 0; j < arrSQLTerms.length; j++) {
            sqltermCols.add(arrSQLTerms[j]._strColumnName);
        }

        ArrayList<String> operators = new ArrayList<String>();

        for (int n = 0; n < strarrOperators.length; n++) {
            operators.add(strarrOperators[n]);
        }

        //ArrayList<String> colsinIndex= new ArrayList<String>();

        for (int n = 0; n < operators.size(); n++) {
            if (!(operators.get(n).equals("AND"))) {
                andOperators = false;
            }
        }

        ArrayList<String> colsinIndex = new ArrayList<>();

        Table t = load_table(tableN);
        boolean colx = false;
        boolean coly = false;
        boolean colz = false;

        String indexName = "";

        int indexX=0;
        int indexY=0;
        int indexZ=0;


        if (sqltermCols.size() >= 3 && andOperators) {
            if (t.colsWithIndices.size() > 0) {

                for (int i = 0; i < t.colsWithIndices.size(); i++) {


                    for (int j = 0; j < sqltermCols.size(); j++) {
                        String colX = t.colsWithIndices.get(i).get(0);
                        String colY = t.colsWithIndices.get(i).get(1);
                        String colZ = t.colsWithIndices.get(i).get(2);

                        if (colX.equals(sqltermCols.get(j))) {
                            colx = true;
                            indexX=j;
                            //sqltermCols.remove(j);
                        } else if (colY.equals(sqltermCols.get(j))) {
                            coly = true;
                            indexY=j;
                           // sqltermCols.remove(j);

                        } else if (colZ.equals(sqltermCols.get(j))) {
                            colz = true;
                            indexZ=j;
                           // sqltermCols.remove(j);

                        }
                        if (colx && coly && colz) {
                            hasIndex = true;
                            sqltermCols.remove(indexX);
                            sqltermCols.remove(indexY);
                            sqltermCols.remove(indexZ);
                            indexName = t.indices.get(i);
                            colsinIndex = t.colsWithIndices.get(i);
                            break;

                        }

                    }
                    if (colx && coly && colz) {
                        indexName = t.indices.get(i);
                        break;

                    } else {
                        colx = false;
                        coly = false;
                        colz = false;
                    }
                }

            }
        }

        save_table(t);

        for (int k = 0; k < arrSQLTerms.length; k++) {
            if (arrSQLTerms[k]._strOperator.equals("!=")) {
                hasIndex = false;
                break;
            }

        }

        if (hasIndex) {
            //look through index
            //since ik they're 3 columns anded together,


           // Octree index = load_index(indexName);

            ArrayList<Integer> pageNumbers = new ArrayList<Integer>();

            Hashtable<Object, Integer> primaryKeyVals = new Hashtable<Object, Integer>(); //integer is page number and object is val of
            //pk in this page for the tuple we want to add

            Object valX = new Object();
            Object valY = new Object();
            Object valZ = new Object();

            String opX = "";
            String opY = "";
            String opZ = "";


            for (int i = 0; i < arrSQLTerms.length; i++) {
                if (arrSQLTerms[i]._strColumnName.equals(colsinIndex.get(0))) {
                    valX = arrSQLTerms[i]._objValue;
                    opX = arrSQLTerms[i]._strOperator;
                } else if (arrSQLTerms[i]._strColumnName.equals(colsinIndex.get(1))) {
                    valY = arrSQLTerms[i]._objValue;
                    opY = arrSQLTerms[i]._strOperator;
                } else if (arrSQLTerms[i]._strColumnName.equals(colsinIndex.get(2))) {
                    valZ = arrSQLTerms[i]._objValue;
                    opZ = arrSQLTerms[i]._strOperator;
                }
            }

            Object minX= new Object();
            Object maxX= new Object();

            boolean minXIncl= false;
            boolean maxXIncl= false;


            Object minY= new Object();
            Object maxY= new Object();

            boolean minYIncl=false;
            boolean maxYIncl=false;


            Object minZ= new Object();
            Object maxZ= new Object();

            boolean minZIncl=false;
            boolean maxZIncl=false;


            switch (opX) {
                case ("="):
                    minX = valX;
                    maxX = valX;
                    minXIncl = true;
                    maxXIncl = true;

                    ;
                    break;

                case (">"):
                    minX = valX;
                    minXIncl = false;

                    ArrayList<String> metadata = read_from_metadata();
                    String maximumX = "";
                    String type = "";

                    for (int i = 0; i < metadata.size(); i++) {
                        String row = metadata.get(i);
                        String segments[] = row.split(", ");
                        if (segments[0].equals(tableN)) {
                            if (segments[1].equals(colsinIndex.get(0))) {
                                maximumX = segments[7];
                                type = segments[2];
                                break;
                            }
                        }
                    }

                    if (type.equals("java.lang.String")) {
                        maxX = maximumX.toString().toLowerCase();
                    } else if (type.equals("java.lang.Integer")) {
                        maxX = Integer.parseInt(maximumX);
                    } else if (type.equals("java.lang.Double")) {
                        maxX = Double.parseDouble(maximumX);
                    } else {
                        try {
                            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(maximumX);
                            maxX = date;
                        } catch (java.text.ParseException e) {

                        }
                    }

                    maxXIncl = true;


                    ;
                    break;
                case (">="):
                    minX = valX;
                    minXIncl = true;

                    ArrayList<String> metadata2 = read_from_metadata();
                    String maximumX2 = "";
                    String type2 = "";

                    for (int i = 0; i < metadata2.size(); i++) {
                        String row = metadata2.get(i);
                        String segments[] = row.split(", ");
                        if (segments[0].equals(tableN)) {
                            if (segments[1].equals(colsinIndex.get(0))) {
                                maximumX2 = segments[7];
                                type2 = segments[2];
                                break;
                            }
                        }
                    }

                    if (type2.equals("java.lang.String")) {
                        maxX = maximumX2.toString().toLowerCase();
                    } else if (type2.equals("java.lang.Integer")) {
                        maxX = Integer.parseInt(maximumX2);
                    } else if (type2.equals("java.lang.Double")) {
                        maxX = Double.parseDouble(maximumX2);
                    } else {
                        try {
                            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(maximumX2);
                            maxX = date;
                        } catch (java.text.ParseException e) {

                        }
                    }

                    maxXIncl = true;


                    ;
                    break;
                case ("<"):
                    maxX = valX;
                    maxXIncl = false;

                    ArrayList<String> metadata3 = read_from_metadata();
                    String minimumX = "";
                    String type3 = "";

                    for (int i = 0; i < metadata3.size(); i++) {
                        String row = metadata3.get(i);
                        String segments[] = row.split(", ");
                        if (segments[0].equals(tableN)) {
                            if (segments[1].equals(colsinIndex.get(0))) {
                                minimumX = segments[7];
                                type3 = segments[6];
                                break;
                            }
                        }
                    }

                    if (type3.equals("java.lang.String")) {
                        minX = minimumX.toString().toLowerCase();
                    } else if (type3.equals("java.lang.Integer")) {
                        minX = Integer.parseInt(minimumX);
                    } else if (type3.equals("java.lang.Double")) {
                        minX = Double.parseDouble(minimumX);
                    } else {
                        try {
                            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(minimumX);
                            minX = date;
                        } catch (java.text.ParseException e) {

                        }
                    }

                    minXIncl = true;


                    ;
                    break;
                case ("<="):
                    maxX = valX;
                    maxXIncl = true;

                    ArrayList<String> metadata4 = read_from_metadata();
                    String minimumX2 = "";
                    String type4 = "";

                    for (int i = 0; i < metadata4.size(); i++) {
                        String row = metadata4.get(i);
                        String segments[] = row.split(", ");
                        if (segments[0].equals(tableN)) {
                            if (segments[1].equals(colsinIndex.get(0))) {
                                minimumX2 = segments[7];
                                type4 = segments[6];
                                break;
                            }
                        }
                    }

                    if (type4.equals("java.lang.String")) {
                        minX = minimumX2.toString().toLowerCase();
                    } else if (type4.equals("java.lang.Integer")) {
                        minX = Integer.parseInt(minimumX2);
                    } else if (type4.equals("java.lang.Double")) {
                        minX = Double.parseDouble(minimumX2);
                    } else {
                        try {
                            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(minimumX2);
                            minX = date;
                        } catch (java.text.ParseException e) {

                        }
                    }

                    minXIncl = true;
                    ;
                    break;

                default:
                    break;

            }

            switch (opY) {
                case ("="):
                    minY = valY;
                    maxY = valY;
                    minYIncl = true;
                    maxYIncl = true;

                    ;
                    break;

                case (">"):
                    minY = valY;
                    minYIncl = false;

                    ArrayList<String> metadata = read_from_metadata();
                    String maximumY = "";
                    String type = "";

                    for (int i = 0; i < metadata.size(); i++) {
                        String row = metadata.get(i);
                        String segments[] = row.split(", ");
                        if (segments[0].equals(tableN)) {
                            if (segments[1].equals(colsinIndex.get(0))) {
                                maximumY = segments[7];
                                type = segments[2];
                                break;
                            }
                        }
                    }

                    if (type.equals("java.lang.String")) {
                        maxY = maximumY.toString().toLowerCase();
                    } else if (type.equals("java.lang.Integer")) {
                        maxY = Integer.parseInt(maximumY);
                    } else if (type.equals("java.lang.Double")) {
                        maxY = Double.parseDouble(maximumY);
                    } else {
                        try {
                            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(maximumY);
                            maxY = date;
                        } catch (java.text.ParseException e) {

                        }
                    }

                    maxYIncl = true;


                    ;
                    break;
                case (">="):
                    minY = valY;
                    minYIncl = true;

                    ArrayList<String> metadata2 = read_from_metadata();
                    String maximumY2 = "";
                    String typeY2 = "";

                    for (int i = 0; i < metadata2.size(); i++) {
                        String row = metadata2.get(i);
                        String segments[] = row.split(", ");
                        if (segments[0].equals(tableN)) {
                            if (segments[1].equals(colsinIndex.get(0))) {
                                maximumY2 = segments[7];
                                typeY2 = segments[2];
                                break;
                            }
                        }
                    }

                    if (typeY2.equals("java.lang.String")) {
                        maxY = maximumY2.toString().toLowerCase();
                    } else if (typeY2.equals("java.lang.Integer")) {
                        maxY = Integer.parseInt(maximumY2);
                    } else if (typeY2.equals("java.lang.Double")) {
                        maxY = Double.parseDouble(maximumY2);
                    } else {
                        try {
                            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(maximumY2);
                            maxY = date;
                        } catch (java.text.ParseException e) {

                        }
                    }

                    maxYIncl = true;


                    ;
                    break;
                case ("<"):
                    maxY = valY;
                    maxYIncl = false;

                    ArrayList<String> metadata3 = read_from_metadata();
                    String minimumY = "";
                    String typeY3 = "";

                    for (int i = 0; i < metadata3.size(); i++) {
                        String row = metadata3.get(i);
                        String segments[] = row.split(", ");
                        if (segments[0].equals(tableN)) {
                            if (segments[1].equals(colsinIndex.get(0))) {
                                minimumY = segments[7];
                                typeY3 = segments[6];
                                break;
                            }
                        }
                    }

                    if (typeY3.equals("java.lang.String")) {
                        minY = minimumY.toString().toLowerCase();
                    } else if (typeY3.equals("java.lang.Integer")) {
                        minY = Integer.parseInt(minimumY);
                    } else if (typeY3.equals("java.lang.Double")) {
                        minY = Double.parseDouble(minimumY);
                    } else {
                        try {
                            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(minimumY);
                            minX = date;
                        } catch (java.text.ParseException e) {

                        }
                    }

                    minYIncl = true;


                    ;
                    break;
                case ("<="):
                    maxY = valY;
                    maxYIncl = true;

                    ArrayList<String> metadata4 = read_from_metadata();
                    String minimumY2 = "";
                    String type4Y = "";

                    for (int i = 0; i < metadata4.size(); i++) {
                        String row = metadata4.get(i);
                        String segments[] = row.split(", ");
                        if (segments[0].equals(tableN)) {
                            if (segments[1].equals(colsinIndex.get(0))) {
                                minimumY2 = segments[7];
                                type4Y = segments[6];
                                break;
                            }
                        }
                    }

                    if (type4Y.equals("java.lang.String")) {
                        minY = minimumY2.toString().toLowerCase();
                    } else if (type4Y.equals("java.lang.Integer")) {
                        minY = Integer.parseInt(minimumY2);
                    } else if (type4Y.equals("java.lang.Double")) {
                        minY = Double.parseDouble(minimumY2);
                    } else {
                        try {
                            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(minimumY2);
                            minY = date;
                        } catch (java.text.ParseException e) {

                        }
                    }

                    minYIncl = true;
                    ;
                    break;

                default:
                    break;

            }

            switch (opZ) {
                case ("="):
                    minZ = valZ;
                    maxZ = valZ;
                    minZIncl = true;
                    maxZIncl = true;

                    ;
                    break;

                case (">"):
                    minZ = valZ;
                    minZIncl = false;

                    ArrayList<String> metadata = read_from_metadata();
                    String maximumZ = "";
                    String type = "";

                    for (int i = 0; i < metadata.size(); i++) {
                        String row = metadata.get(i);
                        String segments[] = row.split(", ");
                        if (segments[0].equals(tableN)) {
                            if (segments[1].equals(colsinIndex.get(0))) {
                                maximumZ = segments[7];
                                type = segments[2];
                                break;
                            }
                        }
                    }

                    if (type.equals("java.lang.String")) {
                        maxZ = maximumZ.toString().toLowerCase();
                    } else if (type.equals("java.lang.Integer")) {
                        maxZ = Integer.parseInt(maximumZ);
                    } else if (type.equals("java.lang.Double")) {
                        maxZ = Double.parseDouble(maximumZ);
                    } else {
                        try {
                            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(maximumZ);
                            maxZ = date;
                        } catch (java.text.ParseException e) {

                        }
                    }

                    maxZIncl = true;


                    ;
                    break;
                case (">="):
                    minZ = valZ;
                    minZIncl = true;

                    ArrayList<String> metadata2 = read_from_metadata();
                    String maximumZ2 = "";
                    String type2 = "";

                    for (int i = 0; i < metadata2.size(); i++) {
                        String row = metadata2.get(i);
                        String segments[] = row.split(", ");
                        if (segments[0].equals(tableN)) {
                            if (segments[1].equals(colsinIndex.get(0))) {
                                maximumZ2 = segments[7];
                                type2 = segments[2];
                                break;
                            }
                        }
                    }

                    if (type2.equals("java.lang.String")) {
                        maxZ = maximumZ2.toString().toLowerCase();
                    } else if (type2.equals("java.lang.Integer")) {
                        maxZ = Integer.parseInt(maximumZ2);
                    } else if (type2.equals("java.lang.Double")) {
                        maxZ = Double.parseDouble(maximumZ2);
                    } else {
                        try {
                            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(maximumZ2);
                            maxZ = date;
                        } catch (java.text.ParseException e) {

                        }
                    }

                    maxZIncl = true;


                    ;
                    break;
                case ("<"):
                    maxZ = valZ;
                    maxZIncl = false;

                    ArrayList<String> metadata3 = read_from_metadata();
                    String minimumZ = "";
                    String type3 = "";

                    for (int i = 0; i < metadata3.size(); i++) {
                        String row = metadata3.get(i);
                        String segments[] = row.split(", ");
                        if (segments[0].equals(tableN)) {
                            if (segments[1].equals(colsinIndex.get(0))) {
                                minimumZ = segments[7];
                                type3 = segments[6];
                                break;
                            }
                        }
                    }

                    if (type3.equals("java.lang.String")) {
                        minZ = minimumZ.toString().toLowerCase();
                    } else if (type3.equals("java.lang.Integer")) {
                        minZ = Integer.parseInt(minimumZ);
                    } else if (type3.equals("java.lang.Double")) {
                        minZ = Double.parseDouble(minimumZ);
                    } else {
                        try {
                            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(minimumZ);
                            minZ = date;
                        } catch (java.text.ParseException e) {

                        }
                    }

                    minZIncl = true;


                    ;
                    break;
                case ("<="):
                    maxZ = valZ;
                    maxZIncl = true;

                    ArrayList<String> metadata4 = read_from_metadata();
                    String minimumZ2 = "";
                    String type4 = "";

                    for (int i = 0; i < metadata4.size(); i++) {
                        String row = metadata4.get(i);
                        String segments[] = row.split(", ");
                        if (segments[0].equals(tableN)) {
                            if (segments[1].equals(colsinIndex.get(0))) {
                                minimumZ2 = segments[7];
                                type4 = segments[6];
                                break;
                            }
                        }
                    }

                    if (type4.equals("java.lang.String")) {
                        minZ = minimumZ2.toString().toLowerCase();
                    } else if (type4.equals("java.lang.Integer")) {
                        minZ = Integer.parseInt(minimumZ2);
                    } else if (type4.equals("java.lang.Double")) {
                        minZ = Double.parseDouble(minimumZ2);
                    } else {
                        try {
                            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(minimumZ2);
                            minZ = date;
                        } catch (java.text.ParseException e) {

                        }
                    }

                    minZIncl = true;
                    ;
                    break;

                default:
                    break;

            }

            Hashtable<Object,String> pkValPageNumber= new Hashtable<>();
            getPagesSelect(minX,maxX,minXIncl,maxXIncl,minY,maxY,minYIncl,maxYIncl,minZ,maxZ,minZIncl,maxZIncl,indexName,pkValPageNumber);

            Vector<Hashtable<String,Object >> tuples= getTuples(pkValPageNumber,tableN );

            if(sqltermCols.size()>0){



                return selectHelper2(tuples,sqltermCols, arrSQLTerms);

            }
            else{
                Iterator itr = tuples.iterator();
                return itr;

            }








        } else {


            // NO INDEX CASE
            String tableName = arrSQLTerms[0]._strTableName;
            String columnName = arrSQLTerms[0]._strColumnName;
            String operand = arrSQLTerms[0]._strOperator;

            Object value = arrSQLTerms[0]._objValue;
            //  ArrayList<String> results= new ArrayList<>();
            Table table = load_table(tableName);
            Vector<Hashtable<String, Object>> tempResult = new Vector<>();

            //get first result set here, then pass it to method that filters it with and, or or xor

            for (int i = 1; i <= table.PageNumbers.size(); i++) {

                Page page = table.load_page(i + "");

                for (int j = 0; j < page.tuples.size(); j++) {
                    Hashtable<String, Object> tuple = page.tuples.get(j);


                    switch (operand) {
                        case (">"):
                            if (greaterThan(tuple.get(columnName), value)) {
                                tempResult.add(tuple);
                            }
                            ;
                            break;

                        case (">="):
                            if (!(lessThan(tuple.get(columnName), value))) {
                                tempResult.add(tuple);
                            }
                            ;
                            break;

                        case ("<"):
                            if ((lessThan(tuple.get(columnName), value))) {
                                tempResult.add(tuple);
                            }
                            ;
                            break;

                        case ("<="):
                            if (!(greaterThan(tuple.get(columnName), value))) {
                                tempResult.add(tuple);
                            }
                            ;
                            break;

                        case ("!="):
                            if (!(equals(tuple.get(columnName), value))) {
                                tempResult.add(tuple);
                            }
                            ;
                            break;

                        case ("="):
                            if (equals(tuple.get(columnName), value)) {
                                tempResult.add(tuple);
                            }
                            ;
                            break;

                        default:
                            break;
                    }
                }
                table.save_page(page);
            }

            save_table(table);

            if (strarrOperators.length > 0) {
                Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();

                selectHelper(tempResult, arrSQLTerms, strarrOperators, 1, result);

                System.out.println("result size:" + result.size());

                Iterator itr = result.iterator();
                return itr;
            } else {

                Iterator itr = tempResult.iterator();
                return itr;
            }
        }


    }

    public Iterator selectHelper2(Vector<Hashtable<String,Object >> firstSet,ArrayList<String> sqltermCols, SQLTerm[] arrSQLTerms) {


        for (int i = 0; i < sqltermCols.size(); i++) {

            for (int j = 0; j < arrSQLTerms.length; j++) {

                if (arrSQLTerms[j]._strColumnName.equals(sqltermCols.get(i))) {

                    String columnName = arrSQLTerms[j]._strColumnName;
                    String operand = arrSQLTerms[j]._strOperator;

                    Object value = arrSQLTerms[j]._objValue;

                    for (int k = 0; k< firstSet.size(); k++) {
                        Hashtable<String, Object> tuple = firstSet.get(k);

                        switch (operand) {
                            case (">"):
                                if (greaterThan(tuple.get(columnName), value)) {
                                    firstSet.add(tuple);
                                }
                                ;
                                break;

                            case (">="):
                                if (!(lessThan(tuple.get(columnName), value))) {
                                    firstSet.add(tuple);
                                }
                                ;
                                break;

                            case ("<"):
                                if ((lessThan(tuple.get(columnName), value))) {
                                    firstSet.add(tuple);
                                }
                                ;
                                break;

                            case ("<="):
                                if (!(greaterThan(tuple.get(columnName), value))) {
                                    firstSet.add(tuple);
                                }
                                ;
                                break;

                            case ("!="):
                                if (!(equals(tuple.get(columnName), value))) {
                                    firstSet.add(tuple);
                                }
                                ;
                                break;

                            case ("="):
                                if (equals(tuple.get(columnName), value)) {
                                    firstSet.add(tuple);
                                }
                                ;
                                break;

                            default:
                                break;
                        }




                    }

                }
            }
        }

        Iterator itr= firstSet.iterator();
        return itr;




    }

    public Vector<Hashtable<String,Object >> getTuples( Hashtable<Object,String> pkValPageNumber, String tableN ) throws DBAppException{

        Vector<Hashtable<String,Object>> tuples= new Vector<>();

        Set<Object> typeSet = pkValPageNumber.keySet();
        Iterator<Object> typeItr = typeSet.iterator();
        Table t= load_table(tableN);

        while(typeItr.hasNext()){

            Object pkVal= typeItr.next();
            String pageNumber= pkValPageNumber.get(pkVal);


            Page p= t.load_page(pageNumber);
            for(int i=0; i<p.tuples.size(); i++){
                if ( equals(p.tuples.get(i).get(t.strClusteringKeyColumn), pkVal)){
                    tuples.add(p.tuples.get(i));
                }
            }

            t.save_page(p);

        }
        save_table(t);
        return tuples;

    }


    public void getPagesSelect(Object minX, Object maxX,boolean minXIncl,
    boolean maxXIncl, Object minY, Object maxY, boolean minYIncl,
                                             boolean maxYIncl, Object minZ, Object maxZ, boolean minZIncl,
                                             boolean maxZIncl, String indexName, Hashtable<Object,String> pkValPageNumber) throws DBAppException{

        Octree octree= load_index(indexName);
        if( !(octree.Points.isEmpty()) && octree.Octants.isEmpty()){
            for(int i=0; i<octree.Points.size(); i++){
                Point p= octree.Points.get(i);

                if ( !(lessThan(p.x,minX)) && !(greaterThan(p.x,maxX)) &&
                        !(lessThan(p.y,minY)) && !(greaterThan(p.y,maxY)) &&
                        !(lessThan(p.z,minZ)) && !(greaterThan(p.z,maxZ))){

                    if( (!(minXIncl) && equals(p.x,minX)) || (!(maxXIncl) && equals(p.x,maxX)) ||
                            (!(minYIncl) && equals(p.y,minY)) || (!(maxYIncl) && equals(p.y,maxY)) ||
                            (!(minZIncl) && equals(p.z,minZ)) || (!(maxZIncl) && equals(p.z,maxZ)) ){
                        continue;

                    }
                    else{
                        pkValPageNumber.put(p.pkValue, p.pageNumber + "");
                    }




                }

            }
            save_index(octree);
            return;


        }
        else{
            for(int j=0; j<octree.Octants.size(); j++){
                Octree t= load_index(octree.Octants.get(j).indexName);

                char number= octree.Octants.get(j).indexName.charAt(octree.Octants.get(j).indexName.length()-1);
                if(number=='1'){

                    if (!(lessThan(minX, t.minX)) && lessThan(maxX, t.maxX) && !(lessThan(minY, t.minY)) && lessThan(maxY, t.maxY)
                            && !(lessThan( minZ, t.minZ)) && lessThan(maxZ, t.maxZ)) {
                        save_index(t);
                        getPagesSelect(minX,maxX,minXIncl,
                        maxXIncl, minY, maxY, minYIncl,
                        maxYIncl, minZ, maxZ, minZIncl,
                        maxZIncl, octree.Octants.get(j).indexName, pkValPageNumber);

                    }


                }
                else if (number=='2'){

                    if (!(lessThan(minX, t.minX)) && lessThan(maxX, t.maxX) && !(lessThan(minY, t.minY)) && !(greaterThan(maxY, t.maxY))
                            && !(lessThan(minZ, t.minZ)) && lessThan(maxZ, t.maxZ)) {
                                save_index(t);
                                getPagesSelect(minX,maxX,minXIncl,
                                maxXIncl, minY, maxY, minYIncl,
                                maxYIncl, minZ, maxZ, minZIncl,
                                maxZIncl, octree.Octants.get(j).indexName, pkValPageNumber);
                    }

                }
                else if (number=='3'){
                    if (!(lessThan(minX, t.minX)) && !(greaterThan(maxX, t.maxX)) && !(lessThan(minY, t.minY)) && lessThan(maxY, t.maxY)
                            && !(lessThan(minZ, t.minZ) && lessThan(maxZ, t.maxZ))){
                        save_index(t);
                        getPagesSelect(minX, maxX, minXIncl,
                                maxXIncl, minY, maxY, minYIncl,
                                maxYIncl, minZ, maxZ, minZIncl,
                                maxZIncl, octree.Octants.get(j).indexName, pkValPageNumber);
                    }
                }
                else if (number=='4'){
                    if (!(lessThan(minX, t.minX)) && !(greaterThan(maxX, t.maxX)) && !(lessThan(minY, t.minY)) && !(greaterThan(maxY, t.maxY))
                            && !(lessThan(minZ, t.minZ)) && lessThan(maxZ, t.maxZ)) {
                        save_index(t);
                        getPagesSelect(minX, maxX, minXIncl,
                        maxXIncl, minY, maxY, minYIncl,
                        maxYIncl, minZ, maxZ, minZIncl,
                        maxZIncl, octree.Octants.get(j).indexName, pkValPageNumber);
                    }
                }
                else if (number=='5'){
                    if (!(lessThan(minX, t.minX)) && lessThan(maxX, t.maxX) && !(lessThan(minY, t.minY)) && lessThan(maxY, t.maxY)
                            && !(lessThan(minZ, t.minZ)) && !(greaterThan(maxZ, t.maxZ))) {
                        save_index(t);
                        getPagesSelect(minX, maxX, minXIncl,
                                maxXIncl, minY, maxY, minYIncl,
                                maxYIncl, minZ, maxZ, minZIncl,
                                maxZIncl, octree.Octants.get(j).indexName, pkValPageNumber);
                    }
                }
                else if (number=='6'){
                    if (!(lessThan(minX, t.minX)) && lessThan(maxX, t.maxX) && !(lessThan(minY, t.minY)) && !(greaterThan(maxY, t.maxY))
                            && !(lessThan(minZ, t.minZ)) && !(greaterThan(maxZ, t.maxZ))) {
                        save_index(t);
                        getPagesSelect(minX, maxX, minXIncl,
                                maxXIncl, minY, maxY, minYIncl,
                                maxYIncl, minZ, maxZ, minZIncl,
                                maxZIncl, octree.Octants.get(j).indexName, pkValPageNumber);
                    }
                }
                else if (number=='7'){
                    if (!(lessThan(minX, t.minX)) && !(greaterThan(maxX, t.maxX)) && !(lessThan(minY, t.minY)) && lessThan(maxY, t.maxY)
                            && !(lessThan(minZ, t.minZ)) && !(greaterThan(maxZ, t.maxZ))) {
                        save_index(t);
                        getPagesSelect(minX, maxX, minXIncl,
                                maxXIncl, minY, maxY, minYIncl,
                                maxYIncl, minZ, maxZ, minZIncl,
                                maxZIncl, octree.Octants.get(j).indexName, pkValPageNumber);
                    }
                }
                else if (number=='8'){
                    if (!(lessThan(minX, t.minX)) && !(greaterThan(maxX, t.maxX)) && !(lessThan(minY, t.minY)) && !(greaterThan(maxY, t.maxY))
                            && !(lessThan(minZ, t.minZ)) && !(greaterThan(maxZ, t.maxZ))) {
                        save_index(t);
                        getPagesSelect(minX, maxX, minXIncl,
                                maxXIncl, minY, maxY, minYIncl,
                                maxYIncl, minZ, maxZ, minZIncl,
                                maxZIncl, octree.Octants.get(j).indexName, pkValPageNumber);
                    }
                }

            }

        }





    }



    public void selectHelper(Vector<Hashtable<String, Object>> firstResultSet,
                                                         SQLTerm[] arrSQLTerms,String[] strarrOperators, int sqlTermIndex,
                             Vector<Hashtable<String, Object>> intermediateResult  ) throws DBAppException{

        String operator= strarrOperators[sqlTermIndex-1];



        switch(operator){

            case("AND"):


                System.out.println("in and sql index is " + sqlTermIndex);

                System.out.println("first resultset is " );

                Iterator resultSet = firstResultSet.iterator();

                while(resultSet.hasNext()){

                    System.out.println(resultSet.next().toString());
                }



                String columnName= arrSQLTerms[sqlTermIndex]._strColumnName;
                String operand= arrSQLTerms[sqlTermIndex]._strOperator;

                Object value= arrSQLTerms[sqlTermIndex]._objValue;

                for(int i=0; i< firstResultSet.size(); i++){
                    Hashtable<String,Object> tuple= firstResultSet.get(i);

                    switch(operand){
                        case(">"): if (greaterThan(tuple.get(columnName), value)){
                            intermediateResult.add(tuple);
                        }; break;

                        case(">="): if( !(lessThan(tuple.get(columnName), value))){
                            intermediateResult.add(tuple);
                        }; break;

                        case("<"): if( (lessThan(tuple.get(columnName), value))){
                            intermediateResult.add(tuple);
                        }; break;

                        case ("<="): if (!(greaterThan(tuple.get(columnName), value))){
                            intermediateResult.add(tuple);
                        }; break;

                        case("!="): if (!(equals(tuple.get(columnName), value))){
                            intermediateResult.add(tuple);
                        }; break;

                        case("="): if(equals(tuple.get(columnName), value)){
                            intermediateResult.add(tuple);
                        }; break;

                        default: break;
                    }

                }

                System.out.println("intermediate is is " );

                Iterator resultSetitr = intermediateResult.iterator();

                while(resultSetitr.hasNext()){

                    System.out.println(resultSetitr.next().toString());
                }


                if (strarrOperators.length>sqlTermIndex){

                    Vector<Hashtable<String,Object>> first= new Vector<Hashtable<String,Object>>();


                    for(int i=0; i< intermediateResult.size(); i++){
                        first.add(intermediateResult.get(i));
                    }

                    System.out.println("in if, first aka intermediate is " );

                    Iterator resultSetitr1 = first.iterator();

                    while(resultSetitr1.hasNext()){

                        System.out.println(resultSetitr1.next().toString());
                    }

                    intermediateResult.removeAllElements();
                    selectHelper(first,arrSQLTerms,strarrOperators,sqlTermIndex+1,
                            intermediateResult);

                }

                else{
                    System.out.println("in else index = " + sqlTermIndex );

                    System.out.println("intermediate aka result is ");

                    Iterator resultSetitr2 = intermediateResult.iterator();

                    while(resultSetitr2.hasNext()){

                        System.out.println(resultSetitr2.next().toString());
                    }

                    return;
                }


                ;break;

            case("OR"):

                String tableName= arrSQLTerms[sqlTermIndex]._strTableName;
                String colName= arrSQLTerms[sqlTermIndex]._strColumnName;
                String opr= arrSQLTerms[sqlTermIndex]._strOperator;

                Object val= arrSQLTerms[sqlTermIndex]._objValue;

                Table table= load_table(tableName);

                String clustKeyColumn= table.strClusteringKeyColumn;

                for(int i=1; i<=table.PageNumbers.size(); i++){

                    Page page= table.load_page(i +"");

                    for(int j=0;j<page.tuples.size(); j++){
                        Hashtable<String,Object> tuple= page.tuples.get(j);


                        switch(opr){
                            case(">"): if (greaterThan(tuple.get(colName), val)){
                                intermediateResult.add(tuple);
                            }; break;

                            case(">="): if( !(lessThan(tuple.get(colName), val))){
                                intermediateResult.add(tuple);
                            }; break;

                            case("<"): if( (lessThan(tuple.get(colName), val))){
                                intermediateResult.add(tuple);
                            }; break;

                            case ("<="): if (!(greaterThan(tuple.get(colName), val))){
                                intermediateResult.add(tuple);
                            }; break;

                            case("!="): if (!(equals(tuple.get(colName), val))){
                                intermediateResult.add(tuple);
                            }; break;

                            case("="): if(equals(tuple.get(colName), val)){
                                intermediateResult.add(tuple);
                            }; break;

                            default: break;
                        }
                    }
                    table.save_page(page);
                }

                save_table(table);

                Vector<Hashtable<String, Object>> temp= filterOR(firstResultSet, intermediateResult, clustKeyColumn);



                System.out.println("in helper in or, temp (filtered or) is ");

                Iterator result123 = temp.iterator();

                while(result123.hasNext()){

                    System.out.println(result123.next().toString());
                }

                System.out.println("in helper in or, intermediate is ");

                Iterator ittt = intermediateResult.iterator();

                while(ittt.hasNext()){

                    System.out.println(ittt.next().toString());
                }



                if (strarrOperators.length>sqlTermIndex){
                    intermediateResult.removeAllElements();
                    selectHelper(temp,arrSQLTerms,strarrOperators,sqlTermIndex+1, intermediateResult);

                }
                else{

                    intermediateResult.removeAllElements();

                    for(int i=0; i< temp.size(); i++){
                        intermediateResult.add(temp.get(i));
                    }



                   // intermediateResult=temp;
//
//                    intermediateResult.removeAllElements();
//
//                    for(int i=0; i> temp.size(); i++){
//                        intermediateResult.add(temp.get(i));
//                    }

                    System.out.println("in helper in or, intermediate in else (filtered or) is ");

                    Iterator resultt = intermediateResult.iterator();

                    while(resultt.hasNext()){

                        System.out.println(resultt.next().toString());
                    }


                    return;
                }

                ;break;

            case("XOR"):

                Vector<Hashtable<String, Object>> resultSetAND= new Vector<Hashtable<String, Object>>();

                String tableNameXOR=arrSQLTerms[sqlTermIndex]._strTableName;

                String columnXOR= arrSQLTerms[sqlTermIndex]._strColumnName;
                String operXOR= arrSQLTerms[sqlTermIndex]._strOperator;

                Object valueXOR= arrSQLTerms[sqlTermIndex]._objValue;

                for(int i=0; i< firstResultSet.size(); i++){
                    Hashtable<String,Object> tuple= firstResultSet.get(i);

                    switch(operXOR){
                        case(">"): if (greaterThan(tuple.get(columnXOR), valueXOR)){
                            resultSetAND.add(tuple);
                        }; break;

                        case(">="): if( !(lessThan(tuple.get(columnXOR), valueXOR))){
                            resultSetAND.add(tuple);
                        }; break;

                        case("<"): if( (lessThan(tuple.get(columnXOR), valueXOR))){
                            resultSetAND.add(tuple);
                        }; break;

                        case ("<="): if (!(greaterThan(tuple.get(columnXOR), valueXOR))){
                            resultSetAND.add(tuple);
                        }; break;

                        case("!="): if (!(equals(tuple.get(columnXOR), valueXOR))){
                            resultSetAND.add(tuple);
                        }; break;

                        case("="): if(equals(tuple.get(columnXOR), valueXOR)){
                            resultSetAND.add(tuple);
                        }; break;

                        default: break;
                    }
                }
                System.out.println("in XOR RESULTSETAND is ");

                Iterator iter = resultSetAND.iterator();

                while(iter.hasNext()){

                    System.out.println(iter.next().toString());
                }

                Vector<Hashtable<String, Object>> resultSetOR= new Vector<Hashtable<String, Object>>();

                Table tableXOR= load_table(tableNameXOR);

                String clusteringKeyColumn= tableXOR.strClusteringKeyColumn;

                for(int i=1; i<=tableXOR.PageNumbers.size(); i++){

                    Page page= tableXOR.load_page(i +"");

                    for(int j=0;j<page.tuples.size(); j++){
                        Hashtable<String,Object> tuple= page.tuples.get(j);


                        switch(operXOR){
                            case(">"): if (greaterThan(tuple.get(columnXOR), valueXOR)){
                                resultSetOR.add(tuple);
                            }; break;

                            case(">="): if( !(lessThan(tuple.get(columnXOR), valueXOR))){
                                resultSetOR.add(tuple);
                            }; break;

                            case("<"): if( (lessThan(tuple.get(columnXOR), valueXOR))){
                                resultSetOR.add(tuple);
                            }; break;

                            case ("<="): if (!(greaterThan(tuple.get(columnXOR), valueXOR))){
                                resultSetOR.add(tuple);
                            }; break;

                            case("!="): if (!(equals(tuple.get(columnXOR), valueXOR))){
                                resultSetOR.add(tuple);
                            }; break;

                            case("="): if(equals(tuple.get(columnXOR), valueXOR)){
                                resultSetOR.add(tuple);
                            }; break;

                            default: break;
                        }
                    }
                    tableXOR.save_page(page);
                }

                save_table(tableXOR);

                System.out.println("in XOR RESULTSETORis ");

                Iterator iterr = resultSetOR.iterator();

                while(iterr.hasNext()){

                    System.out.println(iterr.next().toString());
                }

                System.out.println("in XOR firstresultset is ");

                Iterator iter3 = firstResultSet.iterator();

                while(iter3.hasNext()){

                    System.out.println(iter3.next().toString());
                }

                resultSetOR= filterOR(firstResultSet, resultSetOR, clusteringKeyColumn);

                System.out.println("in XOR filtered resultsetOR is ");

                Iterator iter6 = resultSetOR.iterator();

                while(iter6.hasNext()){

                    System.out.println(iter6.next().toString());
                }



                //remove anything in resultsetAND from resultsetOR

                for(int i=0; i< resultSetOR.size(); i++){
                    Hashtable<String, Object> tuple1= resultSetOR.get(i);


                    for(int j=0; j< resultSetAND.size(); j++){
                        Hashtable<String, Object> tuple2= resultSetAND.get(j);

                        if ( (equals(tuple1.get(clusteringKeyColumn), tuple2.get(clusteringKeyColumn)))){
                            resultSetOR.removeElementAt(i);
                            break;
                        }
                    }

                }

                intermediateResult.removeAllElements();

                for(int i=0; i< resultSetOR.size(); i++){
                    intermediateResult.add(resultSetOR.get(i));
                }

                System.out.println("in XOR intermediate is is ");

                Iterator iterrre = intermediateResult.iterator();

                while(iterrre.hasNext()){

                    System.out.println(iterrre.next().toString());
                }


                // intermediateResult= filterOR(resultSetOR, resultSetAND,columnXOR);

                if (strarrOperators.length>sqlTermIndex){

                    Vector<Hashtable<String,Object>> first= new Vector<Hashtable<String,Object>>();

                    for(int i=0; i< intermediateResult.size(); i++){
                        first.add(intermediateResult.get(i));
                    }

                    intermediateResult.removeAllElements();
                    selectHelper(first,arrSQLTerms,strarrOperators,sqlTermIndex+1,
                            intermediateResult);
                }
                else{
                    return;
                }

                ;break;

            default:
                ;break;
        }

    }


    public Vector<Hashtable<String, Object>> filterOR( Vector<Hashtable<String, Object>> firstSet, Vector<Hashtable<String, Object>> secondSet,
                                                       String clustKeyColumn){





        for(int i=0; i< firstSet.size(); i++){
            Hashtable<String, Object> tuple1= firstSet.get(i);


            for(int j=0; j< secondSet.size(); j++){
                Hashtable<String, Object> tuple2= secondSet.get(j);

                if ( (equals(tuple1.get(clustKeyColumn), tuple2.get(clustKeyColumn)))){
                    secondSet.removeElementAt(j);
                    break;
                }
            }

        }

        for(int m=0; m<secondSet.size(); m++){

            firstSet.add(secondSet.get(m));
        }

        return firstSet;
    }








    public boolean select_valid_input(SQLTerm[] arrSQLTerms, String[] strarrOperators) {
        //table exists?
        //columns exist in table
        //operator in sqlterms is  >, >=, <, <=, != or =
        //strarroperator is OR, AND or XOR
        //number of elements in strarroperator is sqlterms - 1 (operator between each 2 sqlterms

        boolean tableExists = false;
        boolean sameTable = true;
        String tableName = arrSQLTerms[0]._strTableName;
        ArrayList<String> metadata = read_from_metadata();
        for (int i = 0; i < metadata.size(); i++) {
            String row = metadata.get(i);
            String segments[] = row.split(", ");
            if (segments[0].equals(tableName)) {
                for (int j = 0; j < arrSQLTerms.length; j++) {
                    if (!(arrSQLTerms[j]._strTableName.equals(tableName))) {
                        sameTable = false;
                    }
                }
                tableExists = sameTable;
                break;
            }
        }
        if (!tableExists) {
            return tableExists;
        } else {// table exists and sqlterms include same table,
            //check for columns and that objvalue is of correct type
            ArrayList<String> meta = read_from_metadata();
            for (int i = 0; i < arrSQLTerms.length; i++) {
                String tName = arrSQLTerms[i]._strTableName;
                String cName = arrSQLTerms[i]._strColumnName;

                for (int j = 0; j < meta.size(); j++) {
                    String row = metadata.get(i);
                    String segments[] = row.split(", ");
                    if (segments[0].equals(tName) && segments[1].equals(cName)) {
                        //check if data type matches
                        if (segments[2].equals("java.lang.String") && !(arrSQLTerms[i]._objValue instanceof java.lang.String)) {
                            return false;
                        }
                        if (segments[2].equals("java.lang.Integer") && !(arrSQLTerms[i]._objValue instanceof java.lang.Integer)) {
                            return false;
                        }
                        if (segments[2].equals("java.lang.Double") && !(arrSQLTerms[i]._objValue instanceof java.lang.Double)) {
                            return false;
                        }
                        if (segments[2].equals("java.util.Date") && !(arrSQLTerms[i]._objValue instanceof java.util.Date)) {
                            return false;
                        }
                        //obj type matches, check operator
                        //operator in sqlterms is  >, >=, <, <=, != or =
                        if (!(arrSQLTerms[i]._strOperator.equals(">")) && !(arrSQLTerms[i]._strOperator.equals(">="))
                                && !(arrSQLTerms[i]._strOperator.equals("<")) && !(arrSQLTerms[i]._strOperator.equals("<="))
                                && !(arrSQLTerms[i]._strOperator.equals("!=")) && !(arrSQLTerms[i]._strOperator.equals("="))) {
                            return false;
                        }
                    }
                }
            }
            int sqlTermsLength = arrSQLTerms.length;
            int opLength = strarrOperators.length;
            if (!(sqlTermsLength == (opLength + 1))) {
                return false;
            }
            for (int i = 0; i < strarrOperators.length; i++) {
                if (!(strarrOperators[i].equals("AND")) && !(strarrOperators[i].equals("OR")) &&
                        !(strarrOperators[i].equals("XOR"))) {
                    return false;
                }
            }
            return true;
        }
    }

    public void write_to_metadata(ArrayList<Object> metadata_columns) throws DBAppException {

        try {

            FileOutputStream output = new FileOutputStream("./src/main/resources/metadata.csv", true);

            Iterator<Object> itr = metadata_columns.iterator();
            while (itr.hasNext()) {

                byte[] array = itr.next().toString().getBytes();
                output.write(array);
            }
            output.close();


        } catch (Exception e) {
            e.getStackTrace();
            throw new DBAppException("Error when writing metadata");
        }

    }

    public static ArrayList<String> read_from_metadata() {

        ArrayList<String> metadata = new ArrayList<>();
        try {
            FileInputStream input = new FileInputStream("./src/main/resources/metadata.csv");

            int i = input.read();
            String metadata_line = "";

            while (i != -1) {
                if (((char) i) == '\n') {
                    metadata.add(metadata_line);
                    metadata.add("\n");
                    metadata_line = "";
                } else {
                    metadata_line += ((char) i) + "";
                }
                i = input.read();
            }

        } catch (Exception e) {
            e.getStackTrace();
        }
        return metadata;
    }


    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        Table table = load_table(strTableName);
        Object clusteringKeyValue = htblColNameValue.get(table.strClusteringKeyColumn);

        if (!(htblColNameValue.containsKey(table.strClusteringKeyColumn))) {
            save_table(table);
            throw new DBAppException("No value specified for primary key");
        }
        if (htblColNameValue.get(table.strClusteringKeyColumn) == null) {
            save_table(table);
            throw new DBAppException("Primary key can't be null");
        }
        if (!insert_valid_input(strTableName, htblColNameValue)) {
            save_table(table);
            throw new DBAppException("Input is not valid");
        }
        htblColNameValue_modified(strTableName, htblColNameValue);

        Set<String> typeSet = htblColNameValue.keySet();
        Iterator<String> typeItr = typeSet.iterator();

        while(typeItr.hasNext()){

            String typekey= typeItr.next();
            if(htblColNameValue.get(typekey) instanceof java.lang.String){
                ((String) htblColNameValue.get(typekey)).toLowerCase();
            }
        }


        if (table.page_count == 0) {
            Page page = new Page(1, clusteringKeyValue,
                    clusteringKeyValue, new Vector<>(), 0);
            Vector<Hashtable<String, Object>> tuples = new Vector<>();
            tuples.add(htblColNameValue); // check missing columns and add them as null
            page.tuples = tuples;
            page.row_count++;
            table.page_count++;
            table.PageNumbers.add(1);
            table.save_page(page);

            if(table.indices.size()!=0){
                for(int i=0; i<table.indices.size(); i++){
                    Octree octree= load_index(table.indices.get(i));
                    Object x= htblColNameValue.get(octree.colX);
                    Object y= htblColNameValue.get(octree.colY);
                    Object z= htblColNameValue.get(octree.colZ);
                    Object pk= htblColNameValue.get(table.strClusteringKeyColumn);

                    insertToOctree(octree, 1,x,y,z,octree.colX,octree.colY,octree.colZ,pk );
                }

            }

            save_table(table);


            return;
        } else {
            for (int i = 1; i <= table.page_count; i++) {
                Page page = table.load_page(i + "");
                if (!page.compareToMax(clusteringKeyValue) && !page.isFull) { //clustering key value
                    //is greater than max but page isn't full then it's the last page
                    page.tuples.add(htblColNameValue);
                    page.row_count++;
                    page.max_value = clusteringKeyValue;
                    if (page.row_count == page.max_number_of_rows) {
                        page.isFull = true;
                    }
                    table.save_page(page);

                    if(table.indices.size()>0){
                        for(int j=0; j<table.indices.size(); j++){
                            Octree octree= load_index(table.indices.get(j));
                            Object x= htblColNameValue.get(octree.colX);
                            Object y= htblColNameValue.get(octree.colY);
                            Object z= htblColNameValue.get(octree.colZ);
                            Object pk= htblColNameValue.get(table.strClusteringKeyColumn);

                            insertToOctree(octree, i,x,y,z,octree.colX,octree.colY,octree.colZ,pk );
                        }

                    }
                    save_table(table);
                    return;
                }
                if (page.compareToMax(clusteringKeyValue)) { //value is within page and perform binary search
                    int first = 0;
                    int last = page.row_count - 1;
                    int mid = first + (last - first) / 2;

                    Hashtable<String, Object> midTuple = page.tuples.get(mid);
                    while (first != mid && last != mid) {
                        midTuple = page.tuples.get(mid);
                        boolean go_left = binarySearch(midTuple.get(table.strClusteringKeyColumn), clusteringKeyValue);
                        if (go_left) {
                            last = mid - 1;
                            mid = first + (last - first) / 2;
                        } else {
                            first = mid + 1;
                            mid = first + (last - first) / 2;
                        }
                    }
                    if (!page.isFull) {
                        if (binarySearch(midTuple.get(table.strClusteringKeyColumn), clusteringKeyValue)) {
                            page.tuples.add(mid, htblColNameValue);
                        } else {
                            page.tuples.add(mid + 1, htblColNameValue);
                        }

                        page.row_count++;
                        if (page.row_count == page.max_number_of_rows) {
                            page.isFull = true;
                        }
                        Hashtable<String, Object> maxTuple = page.tuples.get(page.row_count - 1);
                        page.max_value = maxTuple.get(table.strClusteringKeyColumn);
                        Hashtable<String, Object> minTuple = page.tuples.get(0);
                        page.min_value = minTuple.get(table.strClusteringKeyColumn);

                        table.save_page(page);

                        if(table.indices.size()>0){
                            for(int j=0; j<table.indices.size(); j++){
                                Octree octree= load_index(table.indices.get(j));
                                Object x= htblColNameValue.get(octree.colX);
                                Object y= htblColNameValue.get(octree.colY);
                                Object z= htblColNameValue.get(octree.colZ);
                                Object pk= htblColNameValue.get(table.strClusteringKeyColumn);

                                insertToOctree(octree, i,x,y,z,octree.colX,octree.colY,octree.colZ,pk );
                            }

                        }
                        save_table(table);
                        break;
                    } else { //page is full
                        if (mid == first) {
                            if (!binarySearch(midTuple.get(table.strClusteringKeyColumn), clusteringKeyValue)) {
                                mid++;
                            }
                        }
                        shift(strTableName, table, page, htblColNameValue, mid);
                        return;
                    }

                } else { //value is not in page and need to create a new page for it
                    if (i == table.page_count) {
                        table.save_page(page);
                        int newLastPageNo = i + 1;
                        Page newLastPage = new Page(newLastPageNo, clusteringKeyValue, clusteringKeyValue, new Vector<>(), 0);
                        newLastPage.tuples.add(htblColNameValue);
                        newLastPage.row_count++;
                        table.page_count++;
                        table.PageNumbers.add(newLastPageNo);
                        table.save_page(newLastPage);

                        if(table.indices.size()>0){
                            for(int m=0; m<table.indices.size(); m++){
                                Octree octree= load_index(table.indices.get(m));
                                Object x= htblColNameValue.get(octree.colX);
                                Object y= htblColNameValue.get(octree.colY);
                                Object z= htblColNameValue.get(octree.colZ);
                                Object pk= htblColNameValue.get(table.strClusteringKeyColumn);

                                insertToOctree(octree,newLastPageNo ,x,y,z,octree.colX,octree.colY,octree.colZ,pk );
                            }

                        }

                        save_table(table);
                       return;
                    }
                }
                table.save_page(page);
            }
        }


    }

    public void updateIndexPage(Octree octree, Object pk, int newPageNumber) throws DBAppException{

        if (octree.Points.size()>0 && octree.Octants.size()==0){
            for(int i=0; i<octree.Points.size(); i++){

                Point p= octree.Points.get(i);
                if (equals(p.pkValue, pk)){
                    p.pageNumber=newPageNumber;
                    return;
                }

            }
        }
        else{
            for(int j=0; j<octree.Octants.size(); j++){
                Octree oct= octree.Octants.get(j);
                updateIndexPage(oct, pk, newPageNumber);
                save_index(oct);

            }
        }
    }
    public void shift(String strTableName, Table table, Page page, Hashtable<String, Object> htblColNameValue, int mid) throws DBAppException {
        if (page.page_number == table.PageNumbers.get(table.page_count - 1)) { //i'm in last page and it's full
            int newPageNumber = table.PageNumbers.get(table.page_count - 1) + 1;
            Object clusteringKeyValue = htblColNameValue.get(table.strClusteringKeyColumn);

            Hashtable<String, Object> lastTuple = page.tuples.get(page.tuples.size() - 1);
            Page newPage = new Page(newPageNumber, lastTuple.get(table.strClusteringKeyColumn),
                    lastTuple.get(table.strClusteringKeyColumn), new Vector<>(), 0);
            newPage.tuples.add(lastTuple); // update lastTuple page number in index

            for(int i=0; i<table.indices.size(); i++){

                String indexName= table.indices.get(i);
                Octree octree= load_index(indexName);
                updateIndexPage(octree, lastTuple.get(table.strClusteringKeyColumn),newPageNumber );
                save_index(octree);


            }





            newPage.row_count++;
            table.page_count++;
            table.PageNumbers.add(newPageNumber);

            table.save_page(newPage);
            page.tuples.add(mid, htblColNameValue);
            page.tuples.remove(page.row_count);

            Hashtable<String, Object> maxTuple = page.tuples.get(page.row_count - 1);
            page.max_value = maxTuple.get(table.strClusteringKeyColumn);
            Hashtable<String, Object> minTuple = page.tuples.get(0);
            page.min_value = minTuple.get(table.strClusteringKeyColumn);


            if(table.indices.size()>0){
                for(int m=0; m<table.indices.size(); m++){
                    Octree octree= load_index(table.indices.get(m));
                    Object x= htblColNameValue.get(octree.colX);
                    Object y= htblColNameValue.get(octree.colY);
                    Object z= htblColNameValue.get(octree.colZ);
                    Object pk= htblColNameValue.get(table.strClusteringKeyColumn);

                    insertToOctree(octree,page.page_number ,x,y,z,octree.colX,octree.colY,octree.colZ,pk );
                }

            }

            table.save_page(page);



            save_table(table);

        } else { //i'm not in last page
            Page last_page = table.load_page(table.PageNumbers.get(table.page_count - 1) + "");
            int lastPageNumber = last_page.page_number;
            int currentPageNumber = last_page.page_number;
            Page currentPage = last_page;
            if (!last_page.isFull) { //last page isn't full (won't create a new page)

                if (last_page.row_count == last_page.max_number_of_rows - 1) {
                    last_page.isFull = true; //last page will be full since we'll shift 1 row down
                }
                while (currentPageNumber > page.page_number) {
                    Vector<Hashtable<String, Object>> tuples = currentPage.tuples;

                    int newPageNumber = currentPageNumber - 1;
                    if (newPageNumber == page.page_number) {
                        break;
                    }
                    Page newPage = table.load_page(table.PageNumbers.get(newPageNumber - 1) + "");
                    Hashtable<String, Object> lastTuple = newPage.tuples.get(newPage.tuples.size() - 1);
                    currentPage.tuples.add(0, lastTuple);


                    for(int i=0; i<table.indices.size(); i++){

                        String indexName= table.indices.get(i);
                        Octree octree= load_index(indexName);
                        updateIndexPage(octree, lastTuple.get(table.strClusteringKeyColumn),newPage.page_number);
                        save_index(octree);


                    }

                    currentPage.min_value = lastTuple.get(table.strClusteringKeyColumn);
                    Hashtable<String, Object> newMax = currentPage.tuples.get(currentPage.row_count - 1);
                    currentPage.max_value = newMax.get(table.strClusteringKeyColumn);
                    if (!(currentPageNumber == lastPageNumber)) {
                        currentPage.tuples.remove(currentPage.row_count);
                    }
                    table.save_page(currentPage);
                    currentPageNumber = newPageNumber;
                    currentPage = newPage;
                }
                currentPage.tuples.add(0, page.tuples.get(page.tuples.size() - 1));

                for(int i=0; i<table.indices.size(); i++){

                    String indexName= table.indices.get(i);
                    Octree octree= load_index(indexName);
                    updateIndexPage(octree, page.tuples.get(page.tuples.size() - 1).get(table.strClusteringKeyColumn),currentPage.page_number);
                    save_index(octree);


                }


                Hashtable<String, Object> newMincurr = page.tuples.get(page.tuples.size() - 1);
                currentPage.min_value = newMincurr;
                Hashtable<String, Object> newMaxcurr = currentPage.tuples.get(currentPage.row_count - 1);
                currentPage.max_value = newMaxcurr.get(table.strClusteringKeyColumn);

                currentPage.tuples.remove(currentPage.row_count);
                table.save_page(currentPage); //page right after page to be inserted into

                page.tuples.add(mid, htblColNameValue);

                if(table.indices.size()>0){
                    for(int m=0; m<table.indices.size(); m++){
                        Octree octree= load_index(table.indices.get(m));
                        Object x= htblColNameValue.get(octree.colX);
                        Object y= htblColNameValue.get(octree.colY);
                        Object z= htblColNameValue.get(octree.colZ);
                        Object pk= htblColNameValue.get(table.strClusteringKeyColumn);

                        insertToOctree(octree,page.page_number ,x,y,z,octree.colX,octree.colY,octree.colZ,pk );
                    }

                }

                page.tuples.remove(page.row_count);

                Hashtable<String, Object> myPageMin = page.tuples.get(0);
                page.min_value = myPageMin.get(table.strClusteringKeyColumn);
                Hashtable<String, Object> myPageMax = page.tuples.get(page.row_count - 1);
                page.max_value = myPageMax.get(table.strClusteringKeyColumn);


                table.save_page(page);
                save_table(table);

            } else { //we're not in last page but last page is full, need to create new page
                Page lastPage = table.load_page(table.PageNumbers.get(table.page_count - 1) + "");

                Hashtable<String, Object> lastTuple = lastPage.tuples.get(lastPage.tuples.size() - 1);
                int newPageNumber = table.PageNumbers.get(table.page_count - 1) + 1;
                Page newPage = new Page(newPageNumber, lastTuple.get(table.strClusteringKeyColumn),
                        lastTuple.get(table.strClusteringKeyColumn), new Vector<>(), 0);
                newPage.tuples.add(lastTuple);
                newPage.row_count++;
                table.page_count++;
                table.PageNumbers.add(newPageNumber);
                table.save_page(newPage); //new page done
                int currentPageNo = lastPage.page_number - 1;
                Page currPage = lastPage;
                while (currentPageNo > page.page_number) {
                    Vector<Hashtable<String, Object>> tuples = currPage.tuples;

                    int newPageNo = currentPageNo - 1;
                    if (newPageNo == page.page_number) {
                        break;
                    }
                    Page nPage = table.load_page(table.PageNumbers.get(newPageNo - 1) + ""); //new page
                    Hashtable<String, Object> Tuple_last = nPage.tuples.get(nPage.tuples.size() - 1);

                    currPage.tuples.add(0, Tuple_last); //adding last tuple from previous page to new page

                    for(int i=0; i<table.indices.size(); i++){

                        String indexName= table.indices.get(i);
                        Octree octree= load_index(indexName);
                        updateIndexPage(octree, Tuple_last.get(table.strClusteringKeyColumn),currPage.page_number);
                        save_index(octree);


                    }

                    currPage.min_value = Tuple_last.get(table.strClusteringKeyColumn);
                    Hashtable<String, Object> newMaxVal = currPage.tuples.get(currPage.row_count - 1);
                    currPage.max_value = newMaxVal.get(table.strClusteringKeyColumn);
                    if (!(currentPageNo == lastPage.page_number)) {
                        currPage.tuples.remove(currPage.row_count);
                    }

                    table.save_page(currPage);
                    currentPageNo = newPageNo;
                    currPage = nPage;
                }
                currPage.tuples.add(0, page.tuples.get(page.tuples.size() - 1));


                for(int i=0; i<table.indices.size(); i++){

                    String indexName= table.indices.get(i);
                    Octree octree= load_index(indexName);
                    updateIndexPage(octree, page.tuples.get(page.tuples.size() - 1).get(table.strClusteringKeyColumn),currPage.page_number);
                    save_index(octree);


                }

                Hashtable<String, Object> newMinimum = page.tuples.get(page.tuples.size() - 1);
                currPage.min_value = newMinimum.get(table.strClusteringKeyColumn);
                Hashtable<String, Object> newMaximum = currPage.tuples.get(currPage.row_count - 1);
                currPage.max_value = newMaximum.get(table.strClusteringKeyColumn);

                currPage.tuples.remove(currPage.row_count);
                table.save_page(currPage); //page right after page to be inserted into

                page.tuples.add(mid, htblColNameValue);

                if(table.indices.size()>0){
                    for(int m=0; m<table.indices.size(); m++){
                        Octree octree= load_index(table.indices.get(m));
                        Object x= htblColNameValue.get(octree.colX);
                        Object y= htblColNameValue.get(octree.colY);
                        Object z= htblColNameValue.get(octree.colZ);
                        Object pk= htblColNameValue.get(table.strClusteringKeyColumn);

                        insertToOctree(octree,page.page_number ,x,y,z,octree.colX,octree.colY,octree.colZ,pk );
                    }

                }


                page.tuples.remove(page.row_count);

                Hashtable<String, Object> myPageMinimum = page.tuples.get(0);
                page.min_value = myPageMinimum.get(table.strClusteringKeyColumn);
                Hashtable<String, Object> myPageMaximum = page.tuples.get(page.row_count - 1);
                page.max_value = myPageMaximum.get(table.strClusteringKeyColumn);

                table.save_page(page);
                save_table(table);
            }
        }


    }

    public boolean binarySearch(Object original, Object inserted) throws DBAppException {
        boolean go_left = false;

        if (original instanceof String) {
            if (original.toString().compareTo(inserted.toString()) > 0) {
                go_left = true;
            } else if (original.toString().compareTo(inserted.toString()) < 0) {
                return false;
            } else if (original.toString().compareTo(inserted.toString()) == 0) {
                throw new DBAppException("Duplicate value for clustering key");
            }
        } else if (original instanceof Integer) {
            if ((int) original > (int) inserted) {
                go_left = true;
            } else if ((int) original < (int) inserted) {
                return false;
            } else if ((int) original == (int) inserted) {
                throw new DBAppException("Duplicate value for clustering key");
            }
        } else if (original instanceof Double) {
            if (Double.compare((Double) original, (Double) inserted) > 0) {
                go_left = true;
            } else if (Double.compare((Double) original, (Double) inserted) < 0) {
                return false;
            } else if (Double.compare((Double) original, (Double) inserted) == 0) {
                throw new DBAppException("Duplicate value for clustering key");
            }
        } else {
            if (((Date) original).compareTo((Date) inserted) > 0) {
                go_left = true;
            } else if (((Date) original).compareTo((Date) inserted) < 0) {
                return false;
            } else {
                throw new DBAppException("Duplicate value for clustering key");
            }
        }
        return go_left;
    }

    public void htblColNameValue_modified(String strTableName, Hashtable<String, Object> htblColNameValue) {
        Hashtable<String, Object> htblModified = new Hashtable<>();
        ArrayList<String> metadata = read_from_metadata();

        Set<String> columnsSet = htblColNameValue.keySet();
        Iterator<String> columnsItr = columnsSet.iterator();


        for (int i = 0; i < metadata.size(); i++) {
            String row = metadata.get(i);
            String segments[] = row.split(", ");
            if (!segments[0].equals(strTableName)) {
                continue;
            } else {
                if (!htblColNameValue.containsKey(segments[1])) {
                    htblColNameValue.put(segments[1], new DBAppNull());
                }

            }

        }

    }

    public boolean create_valid_input(String strTableName, Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
                                      Hashtable<String, String> htblColNameMax) throws DBAppException {
        boolean isValid = true;
        ArrayList<String> metadata = read_from_metadata();
        for (int i = 0; i < metadata.size(); i++) {
            String row = metadata.get(i);
            String segments[] = row.split(", ");
            if (segments[0].equals(strTableName)) {
                return false;

            }
        }
        Set<String> typeSet = htblColNameType.keySet();
        Iterator<String> typeItr = typeSet.iterator();

        while (typeItr.hasNext()) {
            String typeKey = typeItr.next();
            if (!htblColNameType.get(typeKey).equals("java.lang.String") && !htblColNameType.get(typeKey).equals("java.lang.Integer")
                    && !htblColNameType.get(typeKey).equals("java.lang.Double") && !htblColNameType.get(typeKey).equals("java.util.Date")) {
                throw new DBAppException("Type of column isn't valid. Acceptable types are " +
                        "java.lang.String, java.lang.Integer, java.lang.Double and java.util.Date");
            } else {
                if (htblColNameMin.get(typeKey) == null || htblColNameMax.get(typeKey) == null) {
                    return false;
                }
            }
        }

        return isValid;

    }

    public boolean insert_valid_input(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        ArrayList<String> metadata = read_from_metadata();
        Boolean dataType_matches = true;
        Boolean same_columns = true;
        Boolean withinBoundaries = true;
        Set<String> columnsSet = htblColNameValue.keySet();
        Iterator<String> columnsItr = columnsSet.iterator();
        int count = 0;

        int number_of_columns = 0;
        ArrayList<String> metadata_temp = read_from_metadata();
        for (int j = 0; j < metadata_temp.size(); j++) { //to know number of columns in table
            String row_temp = metadata.get(j);
            String segments[] = row_temp.split(", ");
            if (segments[0].equals(strTableName)) {
                number_of_columns++;
            }
        }
        if (number_of_columns == 0) { //table doesn't exist
            return false;
        }
        while (columnsItr.hasNext()) {
            count = 0;
            String key = columnsItr.next();
            for (int i = 0; i < metadata.size(); i++) {
                String row = metadata.get(i);
                String segm[] = row.split(", ");
                if (!segm[0].equals(strTableName)) {
                    continue;
                } else {
                    if (!segm[1].equals((key))) {
                        count++;
                        if (count >= number_of_columns) {
                            same_columns = false;
                            break;
                        }
                    } else {
                        Object value = htblColNameValue.get(key);

                        if (value instanceof java.lang.String) {
                            if (!segm[2].contains("java.lang.String")) {
                                dataType_matches = false;
                                break;
                            }
                            ((String) value).toLowerCase();
                            if (segm[6].compareTo((String) value) > 0) {
                                withinBoundaries = false;
                                break;
                            }
                            if (segm[7].compareTo((String) value) < 0) {
                                withinBoundaries = false;
                                break;
                            }

                        } else if (value instanceof java.lang.Integer) {
                            if (!segm[2].contains("java.lang.Integer")) {
                                dataType_matches = false;
                                break;
                            }
                            int min = Integer.parseInt(segm[6]);
                            int max = Integer.parseInt(segm[7]);

                            if (min > (int) value) {
                                withinBoundaries = false;
                                break;
                            }
                            if (max < (int) value) {
                                withinBoundaries = false;
                                break;
                            }
                        } else if (value instanceof java.lang.Double) {
                            if (!segm[2].contains("java.lang.Double")) {
                                dataType_matches = false;
                                break;
                            }
                            Double min = Double.parseDouble(segm[6]);
                            Double max = Double.parseDouble(segm[7]);

                            if (Double.compare(min, (Double) value) > 0) {
                                withinBoundaries = false;
                                break;
                            }
                            if (Double.compare(max, (Double) value) < 0) {
                                withinBoundaries = false;
                                break;
                            }

                        } else if (value instanceof java.util.Date) {
                            if (!segm[2].contains("java.util.Date")) {
                                dataType_matches = false;
                                break;
                            }
                            try {
                                Date min_Date = new SimpleDateFormat("yyyy-MM-dd").parse(segm[6]);
                                Date max_Date = new SimpleDateFormat("yyyy-MM-dd").parse(segm[7]);

                                if (min_Date.compareTo((Date) value) > 0) {
                                    withinBoundaries = false;
                                    break;
                                }
                                if (max_Date.compareTo((Date) value) < 0) {
                                    withinBoundaries = false;
                                    break;
                                }
                            } catch (java.text.ParseException e) {
                                throw new DBAppException("Error parsing date");
                            }

                        }
                    }
                }

            }
            if (!dataType_matches || !same_columns) {
                break;
            }

        }
        return (same_columns && dataType_matches && withinBoundaries);

    }

    public Table load_table(String strTableName) throws DBAppException {
        String fileName = strTableName + ".ser";
        Table t = null;
        try {
            FileInputStream fileIn = new FileInputStream("./src/main/resources/Data/" + fileName);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            t = (Table) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException i) {
            i.printStackTrace();
            throw new DBAppException("Table file not found");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new DBAppException("Table file can't be loaded");
        }
        return t;
    }

    public void save_table(Table table) throws DBAppException {
        String fileName = table.strTableName + ".ser";
        try {
            FileOutputStream fileOut = new FileOutputStream("./src/main/resources/Data/" + fileName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(table);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
            throw new DBAppException("Failure in saving table");
        }

    }


    public boolean indexAdded(ArrayList<String> indices, String index){
        for(int i=0; i< indices.size(); i++){
            if (indices.get(i).equals(index)){
                return true;
            }
        }
        return false;
    }


    public boolean columnsAdded(Vector<ArrayList<String>> colsinIndex, ArrayList<String> newCols){
        for(int i=0; i< colsinIndex.size(); i++){
            for(int j=0; j<=colsinIndex.get(i).size()-3; j++){
                if (colsinIndex.get(i).get(j).equals(newCols.get(j))
                 && colsinIndex.get(i).get(j+1).equals(newCols.get(j+1)) &&
                        colsinIndex.get(i).get(j+2).equals(newCols.get(j+2))){
                    return true;
                }
            }
        }
        return false;

    }
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        // strClusteringKeyValue is value of clustering key in which i'll update tuple
        //load table with table name
        //find name of clustering key from table
        //look through metadata and store type of clustering key in string ("java.blabla")
        //based on said string, typecast strClusteringKeyValue into its correct type
        //do linear search on pages to know if tuple is in page
        //do binary search within page
        //save pages and save table
        if (!update_valid_input(strTableName, strClusteringKeyValue, htblColNameValue)) {
            System.out.println("Row to be updated doesn't exist or is invalid");
        } else {
            Table table = load_table(strTableName);
            String strClusteringKeyColumn = table.strClusteringKeyColumn;
            String clusteringKeyType = "";
            ArrayList<String> metadata = read_from_metadata();

            for (int i = 0; i < metadata.size(); i++) {
                String row = metadata.get(i);
                String segments[] = row.split(", ");
                if (segments[0].equals(strTableName)) {
                    if (segments[1].equals(strClusteringKeyColumn)) {
                        clusteringKeyType = segments[2];
                        break;
                    }
                }
            }
            Object clusteringKeyValue = new Object(); //typecasting value to correct type
            if (clusteringKeyType.equals("java.lang.String")) {
                clusteringKeyValue = strClusteringKeyValue.toString().toLowerCase();
            } else if (clusteringKeyType.equals("java.lang.Integer")) {
                clusteringKeyValue = Integer.parseInt(strClusteringKeyValue);
            } else if (clusteringKeyType.equals("java.lang.Double")) {
                clusteringKeyValue = Double.parseDouble(strClusteringKeyValue);
            } else {
                try {
                    Date date = new SimpleDateFormat("yyyy-MM-dd").parse(strClusteringKeyValue);
                    clusteringKeyValue = date;
                } catch (java.text.ParseException e) {

                }
            }
            boolean updated = false;

            ArrayList<String> updatedColumns=new ArrayList<>(); //cols in update method to be updated
            Vector<ArrayList<String>> colsinIndex= new Vector<ArrayList<String>>();

            ArrayList<String> Indices= new ArrayList<>();

            Set<String> typeSet1 = htblColNameValue.keySet();
            Iterator<String> typeItr1 = typeSet1.iterator();

            while(typeItr1.hasNext()){
                updatedColumns.add(typeItr1.next());

            }

            for(int i=0; i<updatedColumns.size(); i++){
                for(int j=0; j<table.colsWithIndices.size(); j++){
                     for(int k=0; k<table.colsWithIndices.get(j).size(); k++){
                         if (table.colsWithIndices.get(j).get(k).equals(updatedColumns.get(i))){

                             //boolean method that checks if cols and indices exist

                             if( !(indexAdded(Indices,table.indices.get(j))) &&
                                     (!(columnsAdded(colsinIndex,table.colsWithIndices.get(j))))){
                                 colsinIndex.add(table.colsWithIndices.get(j));
                                 Indices.add(table.indices.get(j));
                             }

                         }
                     }
                }

            }








            for (int i = 1; i <= table.page_count; i++) {
                Page page = table.load_page(i + "");
                if (page.greaterThanOrEqual(clusteringKeyValue)) {  //my value is in this page so do binary search
                    int first = 0;
                    int last = page.tuples.size() - 1;
                    int mid = (first + last) / 2;

                    while (first <= last) {
                        if (greaterThan(page.tuples.get(mid).get(table.strClusteringKeyColumn), clusteringKeyValue)) {
                            last = mid - 1;
                        } else if (lessThan(page.tuples.get(mid).get(table.strClusteringKeyColumn), clusteringKeyValue)) {
                            first = mid + 1;
                        } else {
                            break;
                        }
                    }

                    for(int n=0; n< Indices.size(); n++){
                        Hashtable<String, Object> tuple = page.tuples.get(mid);

                        String colX= colsinIndex.get(n).get(0);
                        String colY= colsinIndex.get(n).get(1);
                        String colZ= colsinIndex.get(n).get(2);


                        Object x= page.tuples.get(mid).get(colX);
                        Object y= page.tuples.get(mid).get(colY);
                        Object z= page.tuples.get(mid).get(colZ);

                        Octree octree= searchIndex(x,y,z,Indices.get(n));

                        removePoint(x,y,z,Indices.get(n),clusteringKeyValue);

                        Set<String> typeSet2 = htblColNameValue.keySet();
                        Iterator<String> typeItr2 = typeSet2.iterator();

                        while(typeItr2.hasNext()){
                            String key2= typeItr2.next();

                            if(key2.equals(colX)){
                                x= htblColNameValue.get(key2);
                            }
                            else if(key2.equals(colY)){
                                y= htblColNameValue.get(key2);
                            }
                            else if(key2.equals(colZ)){
                                z= htblColNameValue.get(key2);
                            }

                        }

                        Octree oct= load_index(Indices.get(n));

                        insertToOctree(oct, i,x,y,z,colX,colY,colZ,clusteringKeyValue);
                    }




                    Set<String> typeSet = htblColNameValue.keySet();
                    Iterator<String> typeItr = typeSet.iterator();


                    while (typeItr.hasNext()) {
                        String key = typeItr.next();
                        Hashtable<String, Object> tupleToUpdate = page.tuples.get(mid);
                        if(htblColNameValue.get(key) instanceof java.lang.String){
                            ((String) htblColNameValue.get(key)).toLowerCase();
                        }
                        tupleToUpdate.replace(key, htblColNameValue.get(key));
                    }
                    updated = true;
                }
                table.save_page(page);
                if (updated) {
                    break;
                }
            }
            save_table(table);

        }


    }


    public boolean greaterThan(Object a, Object b) {

        if (a instanceof java.lang.String && b instanceof java.lang.String) {
            ((String) a).toLowerCase();
            ((String) b).toLowerCase();
            if (a.toString().compareTo(b.toString()) > 0) {
                return true;
            }
        } else if (a instanceof java.lang.Integer && b instanceof java.lang.Integer) {

            if ((int) a > (int) b) {
                return true;
            }
        } else if (a instanceof java.lang.Double && b instanceof java.lang.Double) {

            if (Double.compare((Double) a, (Double) b) > 0) {
                return true;
            }
        } else if (a instanceof java.util.Date && b instanceof java.util.Date) {
            if (((Date) a).compareTo((Date) b) > 0) {
                return true;
            }
        }
        if (!(a instanceof java.lang.String) && !(a instanceof java.lang.Integer) && !(a instanceof java.lang.Double)
                && !(a instanceof java.util.Date)) {
            return true;
        }
        return false;
    }

    public boolean lessThan(Object a, Object b) {
        if (a instanceof java.lang.String && b instanceof java.lang.String) {
            ((String) a).toLowerCase();
            ((String) b).toLowerCase();
            if (a.toString().compareTo(b.toString()) < 0) {
                return true;
            }
        } else if (a instanceof java.lang.Integer && b instanceof java.lang.Integer) {

            if ((int) a < (int) b) {
                return true;
            }
        } else if (a instanceof java.lang.Double && b instanceof java.lang.Double) {

            if (Double.compare((Double) a, (Double) b) < 0) {
                return true;
            }
        } else if (a instanceof java.util.Date && b instanceof java.util.Date) {
            if (((Date) a).compareTo((Date) b) < 0) {
                return true;
            }
        }
        return false;

    }

    public boolean update_valid_input(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        //if table exists
        //loop on htblColNameValue to check if it's keys exist in metadata
        // check value for each key if it matches type of key in metadata
        //check value is within boundaries of min and max
        int countTableMatches = 0;
        int countColumnsFound = 0; //have to be equal to hashtable entries in the end
        ArrayList<String> metadata = read_from_metadata();
        for (int i = 0; i < metadata.size(); i++) {
            String row = metadata.get(i);
            String segments[] = row.split(", ");
            if (segments[0].equals(strTableName)) {
                countTableMatches++;
            }
        }
        if (countTableMatches == 0) {
            System.out.println("Table doesn't exist");
            return false; //Table doesn't exist;
        }

        Set<String> typeSet = htblColNameValue.keySet();
        Iterator<String> typeItr = typeSet.iterator();

        while (typeItr.hasNext()) {
            String key = typeItr.next();
            Object value = htblColNameValue.get(key);
            for (int j = 0; j < metadata.size(); j++) {
                String metaRow = metadata.get(j);
                String metaSegments[] = metaRow.split(", ");
                if (metaSegments[0].equals(strTableName)) {
                    if (metaSegments[1].equals(key)) {
                        countColumnsFound++;
                        if (metaSegments[3].equals("True")) {//i'm trying to update clustering key
                            System.out.println("Can't update clustering key");
                            return false;
                        }
                        if (metaSegments[2].equals("java.lang.String")) {
                            if (!(value instanceof java.lang.String)) {
                                return false;
                            } else {
                                ((String) value).toLowerCase();
                                if (metaSegments[6].compareTo((String) value) > 0) {
                                    return false;
                                }
                                if (metaSegments[7].compareTo((String) value) < 0) {
                                    return false;
                                }
                            }
                        } else if (metaSegments[2].equals("java.lang.Integer")) {
                            if (!(value instanceof java.lang.Integer)) {
                                return false;
                            } else {
                                int min = Integer.parseInt(metaSegments[6]);
                                int max = Integer.parseInt(metaSegments[7]);
                                if (min > (int) value) {
                                    return false;
                                }
                                if (max < (int) value) {
                                    return false;
                                }
                            }
                        } else if (metaSegments[2].equals("java.lang.Double")) {
                            if (!(value instanceof java.lang.Double)) {
                                return false;
                            } else {
                                Double min = Double.parseDouble(metaSegments[6]);
                                Double max = Double.parseDouble(metaSegments[7]);
                                if (Double.compare(min, (Double) value) > 0) {
                                    return false;
                                }
                                if (Double.compare(max, (Double) value) < 0) {
                                    return false;
                                }
                            }
                        } else {
                            if (!(value instanceof java.util.Date)) {
                                return false;
                            }
                            try {
                                Date min_Date = new SimpleDateFormat("yyyy-MM-dd").parse(metaSegments[6]);
                                Date max_Date = new SimpleDateFormat("yyyy-MM-dd").parse(metaSegments[7]);

                                if (min_Date.compareTo((Date) value) > 0) {
                                    return false;
                                }
                                if (max_Date.compareTo((Date) value) < 0) {
                                    return false;
                                }
                            } catch (java.text.ParseException e) {
                                throw new DBAppException("Error parsing date");
                            }
                        }
                    }
                }
            }
        }
        return (countColumnsFound == htblColNameValue.size());

    }

    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
            throws DBAppException {

        //verify table exists
        //verify columns exist
        //verify values are within min and max
        //conditions are anded together
        //if u use remove it autoshifts, but take care of next pages
        // if pk is in hashtable use binary search to find the single row, otherwise linearly traverse
        if (!(delete_valid_input(strTableName, htblColNameValue))) {
            System.out.println("Data to be deleted is not valid");
            return;
        } else {


            boolean pkGiven = false;
            Table table = load_table(strTableName);
            Set<String> typeSet = htblColNameValue.keySet();
            Iterator<String> typeItr = typeSet.iterator();
            while (typeItr.hasNext()) {
                String key = typeItr.next();
                if (key.equals(table.strClusteringKeyColumn)) {
                    pkGiven = true;
                    break;
                }
            }

            boolean deleted = false;

//            ArrayList<String> updatedColumns=new ArrayList<>(); //cols in update method to be updated
//            Vector<ArrayList<String>> colsinIndex= new Vector<ArrayList<String>>();
//
//            ArrayList<String> Indices= new ArrayList<>();
//
//            Set<String> typeSet1 = htblColNameValue.keySet();
//            Iterator<String> typeItr1 = typeSet1.iterator();
//
//            while(typeItr1.hasNext()){
//                updatedColumns.add(typeItr1.next());
//
//            }
//
//            for(int i=0; i<updatedColumns.size(); i++){
//                for(int j=0; j<table.colsWithIndices.size(); j++){
//                    for(int k=0; k<table.colsWithIndices.get(j).size(); k++){
//                        if (table.colsWithIndices.get(j).get(k).equals(updatedColumns.get(i))){
//
//                            //boolean method that checks if cols and indices exist
//
//                            if( !(indexAdded(Indices,table.indices.get(j))) &&
//                                    (!(columnsAdded(colsinIndex,table.colsWithIndices.get(j))))){
//                                colsinIndex.add(table.colsWithIndices.get(j));
//                                Indices.add(table.indices.get(j));
//                            }
//
//                        }
//                    }
//                }
//
//            }


            if (pkGiven) { //pk is given so perform binary search to find row to delete
                Object pkValue = htblColNameValue.get(table.strClusteringKeyColumn);//primary key value to search with

                if(pkValue instanceof java.lang.String){
                    ((String) pkValue).toLowerCase();
                }
                for (int i = 1; i <= table.page_count; i++) {
                    Page page = table.load_page(i + "");
                    if (page.greaterThanOrEqual(pkValue)) { //row is in this page
                        int first = 0;
                        int last = page.tuples.size() - 1;
                        int mid = (first + last) / 2;

                        while (first <= last) {
                            mid = (first + last) / 2;
                            if (greaterThan(page.tuples.get(mid).get(table.strClusteringKeyColumn), pkValue)) {
                                last = mid - 1;
                            } else if (lessThan(page.tuples.get(mid).get(table.strClusteringKeyColumn), pkValue)) {
                                first = mid + 1;
                            } else {
                                break;
                            }

                        }



                        if (!(page.isFull) || (page.page_number == table.page_count)) { //page isn't full or i'm in last page
                            Hashtable<String, Object> toberemoved = page.tuples.get(mid);
                            if (!(remove(toberemoved, htblColNameValue))) {
                                table.save_page(page);
                                save_table(table);
                                return;
                            }

                            for(int n=0; n< table.indices.size(); n++){
                                Hashtable<String, Object> tuple = page.tuples.get(mid);

                                String colX= table.colsWithIndices.get(n).get(0);
                                String colY= table.colsWithIndices.get(n).get(1);
                                String colZ= table.colsWithIndices.get(n).get(2);


                                Object x= page.tuples.get(mid).get(colX);
                                Object y= page.tuples.get(mid).get(colY);
                                Object z= page.tuples.get(mid).get(colZ);

                                Octree octree= searchIndex(x,y,z,table.indices.get(n));

                                removePoint(x,y,z,table.indices.get(n),pkValue);


                            }

                            page.tuples.remove(mid);
                            page.row_count--;
                            if (page.row_count == 0) {
                                table.page_count--;
                                table.PageNumbers.remove(table.page_count);
                                page = null;
                            } else {
                                page.min_value = page.tuples.get(0).get(table.strClusteringKeyColumn);
                                page.max_value = page.tuples.get(page.row_count - 1).get(table.strClusteringKeyColumn);
                                table.save_page(page);
                            }
                            save_table(table);
                            deleted = true;
                        } else {// page is full, so have to shift within pages
                            Hashtable<String, Object> toberemoved = page.tuples.get(mid);
                            if (!(remove(toberemoved, htblColNameValue))) {
                                table.save_page(page);
                                save_table(table);
                                return;
                            }
                            page.tuples.remove(mid);

                            for(int n=0; n< table.indices.size(); n++){
                                Hashtable<String, Object> tuple = page.tuples.get(mid);

                                String colX= table.colsWithIndices.get(n).get(0);
                                String colY= table.colsWithIndices.get(n).get(1);
                                String colZ= table.colsWithIndices.get(n).get(2);


                                Object x= page.tuples.get(mid).get(colX);
                                Object y= page.tuples.get(mid).get(colY);
                                Object z= page.tuples.get(mid).get(colZ);

                                Octree octree= searchIndex(x,y,z,table.indices.get(n));

                                removePoint(x,y,z,table.indices.get(n),pkValue);


                            }


                            int currentPageNumber = page.page_number;
                            Page currentPage = page;
                            while (currentPageNumber < table.page_count) {
                                int nextPageNumber = currentPageNumber + 1;
                                Page nextPage = table.load_page(nextPageNumber + "");
                                currentPage.tuples.add(nextPage.tuples.get(0));

                                for(int k=0; k<table.indices.size(); k++){

                                    String indexName= table.indices.get(k);
                                    Octree octree= load_index(indexName);
                                    updateIndexPage(octree, nextPage.tuples.get(0).get(table.strClusteringKeyColumn),currentPageNumber);
                                    save_index(octree);


                                }


                                nextPage.tuples.remove(0);
                                currentPage.max_value = currentPage.tuples.get(currentPage.row_count - 1).get(table.strClusteringKeyColumn);
                                currentPage.min_value = currentPage.tuples.get(0).get(table.strClusteringKeyColumn);
                                currentPageNumber = nextPageNumber;
                                if (currentPageNumber == table.page_count) {
                                    table.save_page(currentPage);
                                    nextPage.row_count--;
                                    if (nextPage.row_count == 0) {
                                        table.page_count--;
                                        table.PageNumbers.remove(table.page_count);
                                        nextPage = null;
                                    } else {
                                        table.save_page(nextPage);
                                    }
                                    save_table(table);
                                    break;
                                }
                                table.save_page(currentPage);
                                currentPage = nextPage;
                            }
                            save_table(table);
                            deleted = true;
                        }
                        if (deleted) {
                            break;
                        }
                    }
                }


            } else if (!pkGiven) { //pk not given so must traverse pages linearly
                boolean reformat = false;
                Table table2 = load_table(strTableName); //same table but it was saved in previous cases so has to be loaded again

                for (int i = 1; i <= table2.page_count; i++) {
                    Page myPage = table2.load_page(i + "");
                    for (int j = 0; j < myPage.row_count; j++) {
                        Hashtable<String, Object> tupleInPage = myPage.tuples.get(j);

                        if (remove(tupleInPage, htblColNameValue)) {
                            reformat = true;
                            myPage.tuples.remove(j);
                            myPage.row_count--;

                            for(int n=0; n< table2.indices.size(); n++){
                                //Hashtable<String, Object> tuple = page.tuples.get(mid);

                                String colX= table2.colsWithIndices.get(n).get(0);
                                String colY= table2.colsWithIndices.get(n).get(1);
                                String colZ= table2.colsWithIndices.get(n).get(2);


                                Object x= tupleInPage.get(colX);
                                Object y= tupleInPage.get(colY);
                                Object z= tupleInPage.get(colZ);

                                Octree octree= searchIndex(x,y,z,table2.indices.get(n));

                                removePoint(x,y,z,table2.indices.get(n),tupleInPage.get(table2.strClusteringKeyColumn));
                            }



                        }
                    }
                    table2.save_page(myPage);
                }
                if (reformat) {
                    reformat(table2); //reformats table's pages and does necessary shifts
                }
                save_table(table2);
            }
        }

    }

    public void reformat(Table table) throws DBAppException {
        int originalPageCount = table.page_count;
        boolean reformat = false;

        for (int i = 1; i < originalPageCount; i++) {
            Page page = table.load_page(i + "");
            if (page.row_count < page.max_number_of_rows) {
                int nextPageNumber = i + 1;
                Page nextPage = table.load_page(nextPageNumber + "");
                int diff = page.max_number_of_rows - page.row_count;
                int diff2 = nextPage.row_count;
                if (diff <= diff2) {
                    for (int j = 0; j < diff; j++) {
                        page.tuples.add(nextPage.tuples.get(j));
                        nextPage.tuples.remove(j);

                        for(int k=0; k<table.indices.size(); k++){

                            String indexName= table.indices.get(k);
                            Octree octree= load_index(indexName);
                            updateIndexPage(octree, nextPage.tuples.get(j).get(table.strClusteringKeyColumn),page.page_number );
                            save_index(octree);
                        }

                        page.row_count++;
                        if (page.row_count == page.max_number_of_rows) {
                            page.isFull = true;
                        }
                        nextPage.row_count--;
                        if (nextPage.row_count < nextPage.max_number_of_rows) {
                            nextPage.isFull = false;
                        }
                    }
                } else {
                    for (int j = 0; j < diff2; j++) {
                        page.tuples.add(nextPage.tuples.get(j));
                        nextPage.tuples.remove(j);

                        for(int k=0; k<table.indices.size(); k++){

                            String indexName= table.indices.get(k);
                            Octree octree= load_index(indexName);
                            updateIndexPage(octree, nextPage.tuples.get(j).get(table.strClusteringKeyColumn),page.page_number );
                            save_index(octree);
                        }

                        page.row_count++;
                        if (page.row_count == page.max_number_of_rows) {
                            page.isFull = true;
                        }
                        nextPage.row_count--;
                        if (nextPage.row_count < nextPage.max_number_of_rows) {
                            nextPage.isFull = false;
                        }
                    }
                }
                page.min_value = page.tuples.get(0).get(table.strClusteringKeyColumn);
                page.max_value = page.tuples.get(page.row_count - 1).get(table.strClusteringKeyColumn);
                table.save_page(page);
                table.save_page(nextPage);
            } else {
                table.save_page(page);
            }
        }//i'm in last page
        Page lastPage = table.load_page(originalPageCount + "");
        if (lastPage.row_count == 0) {
            table.page_count--;
            table.PageNumbers.remove(table.page_count);
            lastPage = null;
        } else {
            lastPage.min_value = lastPage.tuples.get(0).get(table.strClusteringKeyColumn);
            lastPage.max_value = lastPage.tuples.get(lastPage.row_count - 1).get(table.strClusteringKeyColumn);
            if (lastPage.row_count < lastPage.max_number_of_rows) {
                lastPage.isFull = false;
            }
            table.save_page(lastPage);
        }
        int currentPageCount = table.page_count;
        for (int i = 1; i <= currentPageCount; i++) {
            Page checkPage = table.load_page(i + "");
            if ((checkPage.row_count < checkPage.max_number_of_rows) && (i != currentPageCount)) {
                // reformat(table);
                reformat = true;
                break;
            }
            table.save_page(checkPage);
        }
        if (reformat) {
            reformat(table);
        }

    }

    public boolean remove(Hashtable<String, Object> a, Hashtable<String, Object> b) {
        Set<String> typeSet = b.keySet();
        Iterator<String> typeItr = typeSet.iterator();
        while (typeItr.hasNext()) {
            String key = typeItr.next();
            if (greaterThan(a.get(key), b.get(key)) || greaterThan(b.get(key), a.get(key))) {
                return false;
            }
        }
        return true;
    }

    public boolean delete_valid_input(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        int countTableMatches = 0;
        int countColumnsFound = 0; //have to be equal to hashtable entries in the end
        ArrayList<String> metadata = read_from_metadata();
        for (int i = 0; i < metadata.size(); i++) {
            String row = metadata.get(i);
            String segments[] = row.split(", ");
            if (segments[0].equals(strTableName)) {
                countTableMatches++;
            }
        }
        if (countTableMatches == 0) {
            return false; //Table doesn't exist;
        }

        Set<String> typeSet = htblColNameValue.keySet();
        Iterator<String> typeItr = typeSet.iterator();

        while (typeItr.hasNext()) {
            String key = typeItr.next();
            Object value = htblColNameValue.get(key);
            for (int j = 0; j < metadata.size(); j++) {
                String metaRow = metadata.get(j);
                String metaSegments[] = metaRow.split(", ");
                if (metaSegments[0].equals(strTableName)) {
                    if (metaSegments[1].equals(key)) {
                        countColumnsFound++;
                        if (metaSegments[2].equals("java.lang.String")) {
                            if (!(value instanceof java.lang.String)) {
                                return false;
                            } else {
                                ((String) value).toLowerCase();
                                if (metaSegments[6].compareTo((String) value) > 0) {
                                    return false;
                                }
                                if (metaSegments[7].compareTo((String) value) < 0) {
                                    return false;
                                }
                            }
                        } else if (metaSegments[2].equals("java.lang.Integer")) {
                            if (!(value instanceof java.lang.Integer)) {
                                return false;
                            } else {
                                int min = Integer.parseInt(metaSegments[6]);
                                int max = Integer.parseInt(metaSegments[7]);
                                if (min > (int) value) {
                                    return false;
                                }
                                if (max < (int) value) {
                                    return false;
                                }
                            }
                        } else if (metaSegments[2].equals("java.lang.Double")) {
                            if (!(value instanceof java.lang.Double)) {
                                return false;
                            } else {
                                Double min = Double.parseDouble(metaSegments[6]);
                                Double max = Double.parseDouble(metaSegments[7]);
                                if (Double.compare(min, (Double) value) > 0) {
                                    return false;
                                }
                                if (Double.compare(max, (Double) value) < 0) {
                                    return false;
                                }
                            }
                        } else {
                            if (!(value instanceof java.util.Date)) {
                                return false;
                            }
                            try {
                                Date min_Date = new SimpleDateFormat("yyyy-MM-dd").parse(metaSegments[6]);
                                Date max_Date = new SimpleDateFormat("yyyy-MM-dd").parse(metaSegments[7]);

                                if (min_Date.compareTo((Date) value) > 0) {
                                    return false;
                                }
                                if (max_Date.compareTo((Date) value) < 0) {
                                    return false;
                                }
                            } catch (java.text.ParseException e) {
                                throw new DBAppException("Error parsing date");
                            }
                        }
                    }
                }
            }
        }
        return (countColumnsFound == htblColNameValue.size());
    }


    public Octree searchIndex(Object X, Object Y, Object Z, String indexName) throws DBAppException {
        Octree octree = load_index(indexName);

        //case1: entries<max

        if ((octree.Octants.size()==0)) {
            //point is in the octant
            return octree;
        } else {
            String octant = "";

            if (isOct1(octree.minX, octree.midX, octree.minY, octree.midY, octree.minZ, octree.midZ, X, Y, Z)) {
                octant = octree.indexName + "1";
            } else if (isOct2(octree.minX, octree.midX, octree.midY, octree.maxY, octree.minZ, octree.midZ, X, Y, Z)) {
                octant = octree.indexName + "2";
            } else if (isOct3(octree.midX, octree.maxX, octree.minY, octree.midY, octree.minZ, octree.midZ, X, Y, Z)) {
                octant = octree.indexName + "3";
            } else if (isOct4(octree.midX, octree.maxX, octree.midY, octree.maxY, octree.minZ, octree.midZ, X, Y, Z)) {
                octant = octree.indexName + "4";
            } else if (isOct5(octree.minX, octree.midX, octree.minY, octree.midY, octree.midZ, octree.maxZ, X, Y, Z)) {
                octant = octree.indexName + "5";
            } else if (isOct6(octree.minX, octree.midX, octree.midY, octree.maxY, octree.midZ, octree.maxZ, X, Y, Z)) {
                octant = octree.indexName + "6";
            } else if (isOct7(octree.midX, octree.maxX, octree.minY, octree.midY, octree.midZ, octree.maxZ, X, Y, Z)) {
                octant = octree.indexName + "7";
            } else if (isOct8(octree.midX, octree.maxX, octree.midY, octree.maxY, octree.midZ, octree.maxZ, X, Y, Z)) {
                octant = octree.indexName + "8";
            }
            save_index(octree);
            return searchIndex(X, Y, Z, octant);

        }
    }
    public void removePoint(Object X, Object Y, Object Z, String indexName, Object pkValue ) throws DBAppException {
        //na2es hetet serializable (ezay ba3melha)?????
        //does it remove every point under the octant?????
        Octree destination=searchIndex(X,Y,Z,indexName);
        //Vector<Point> dest= destination.Points;
        for (int i=0; i<destination.Points.size(); i++)
        {
            if(equals(destination.Points.get(i).x,X) && equals(destination.Points.get(i).y,Y) && equals(destination.Points.get(i).z,Z))
            {
                if (equals(destination.Points.get(i).pkValue,pkValue)){
                    destination.Points.remove(i);
                    save_index(destination);
                    return;
                }
                else
                {
                    for(int j=0; j<destination.Points.get(i).duplicates.size(); j++)
                    {
                        if (equals(destination.Points.get(i).duplicates.get(j).pkValue,pkValue))
                        {
                            destination.Points.get(i).duplicates.get(j);
                            save_index(destination);
                            return;
                        }
                    }
                }
            }


        }
    }


    public static void main(String[] args) throws DBAppException{
        DBApp dbApp= new DBApp();

        String strTableName = "TEST";
        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameType.put("date", "java.util.Date");

        Hashtable min= new Hashtable();
        min.put("id", "1");
        min.put("name", "a");
        min.put("gpa", "0.7");
        Hashtable max= new Hashtable();

            min.put("date","2002-05-12");

            max.put("id", "500");
            max.put("name", "zzzzzz");
            max.put("gpa", "5");
            max.put("date", "2024-05-12");

      // dbApp.createTable(strTableName, "id", htblColNameType, min, max);


            Hashtable<String,Object> values= new Hashtable<>();

            values.put("id", new Integer(5));
            values.put("name", new String("d"));
            values.put("gpa", new Double(3));

            try{
                Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2003-02-04");
                values.put("date", date);

            }catch(java.text.ParseException e){
                throw new DBAppException("error parsing date");
            }

       //  dbApp.insertIntoTable(strTableName, values);

        String[] columns= new String[3];
            columns[0]="id";
        columns[1]="name";
        columns[2]="gpa";

        Hashtable<String, Object> updated= new Hashtable<>();
        updated.put("id", new Integer(5));
       // updated.put("name", new String("zzzzzy"));

     //   updated.put("gpa", new Double(4));

//        dbApp.updateTable(strTableName,"2",updated);


//        dbApp.createIndex(strTableName,columns);


        dbApp.deleteFromTable(strTableName,updated);
//
//        Octree octree= dbApp.load_index("idnamegpaIndex");
//
//        dbApp.testOctree(octree);



//        SQLTerm[] arrSQLTerms=new SQLTerm[3];
//            SQLTerm sqlterm= new SQLTerm("TEST","name", "=" ,"a");
//        SQLTerm sqlterm2= new SQLTerm("TEST","gpa", "=" ,new Double(2));
//        SQLTerm sqlterm3= new SQLTerm("TEST","id", "=" ,new Integer(5));
//        arrSQLTerms[0]= sqlterm;
//        arrSQLTerms[1]= sqlterm2;
//        arrSQLTerms[2]= sqlterm3;
//        arrSQLTerms[0]._strTableName = ;
//        arrSQLTerms[0]._strColumnName= ;
//        arrSQLTerms[0]._strOperator = ;
//        arrSQLTerms[0]._objValue = ;
//        arrSQLTerms[1]._strTableName = "Student";
//        arrSQLTerms[1]._strColumnName= "gpa";
//        arrSQLTerms[1]._strOperator = "=";
//        arrSQLTerms[1]._objValue = new Double( 1.5 );
//        String[]strarrOperators = new String[2];
//        strarrOperators[0] = "OR";
//        strarrOperators[1] = "XOR";

// select * from Student where name = “John Noor” or gpa = 1.5;
       // Iterator resultttt = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
//
//        System.out.println("in main");
//
//        while(resultttt.hasNext()){
//
//            System.out.println(resultttt.next().toString() + " length");
//        }



//        String[] cols= new String[3];
//        cols[0]= "id";
//        cols[1]="name";
//        cols[2]= "gpa";

//        Table table= dbApp.load_table(strTableName);
//        table.indices= new ArrayList<>();
//        table.colsWithIndices= new ArrayList<>();
//        dbApp.save_table(table);

//        dbApp.createIndex(strTableName, cols);

//       ArrayList<String> metadata=new ArrayList<>();
//        metadata.add("TEST, gpa, java.lang.Double, False, null, null, 0.7, 5" + "\n");
//        metadata.add("TEST, date, java.util.Date, False, null, null, 2002-05-12, 2024-05-12" + "\n");
//        metadata.add("TEST, name, java.lang.String, False, null, null, a, zzzzzz" + "\n");
//        metadata.add("TEST, id, java.lang.Integer, True, null, null, 1, 500" + "\n");

//        try {
//
//            FileOutputStream output = new FileOutputStream("./src/main/resources/metadata.csv", false);
//
//            Iterator<String> itr = metadata.iterator();
//            while (itr.hasNext()) {
//
//                byte[] array = itr.next().toString().getBytes();
//                output.write(array);
//            }
//            output.close();
//
//
//        } catch (Exception e) {
//            e.getStackTrace();
//            throw new DBAppException("Error when writing metadata");
//        }
//
//





//






        //dbApp.addIndextoCSV(strTableName,cols,"idnamegpaIndex");



    }
}
