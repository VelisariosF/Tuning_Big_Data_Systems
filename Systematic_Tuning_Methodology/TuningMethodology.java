package Systematic_Tuning_Methodology;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Scanner;


public class TuningMethodology{
    
    // Number of parameters 
    private final static int parametersNumber = 13;

    private static HashMap<Integer, String> parameterIdPairs= constructParameterIdPairs();
    

    //Threasold for the step3 of algorith 1 of the systematic tuning methodolgy
    private final static int t = -2; //could set to 3


    //============= SORT BY KEY ===================\\
    //SORT BY KEY DATA 10 GB FILE 
    private static final int SORT_BY_KEY_DEFAULT_CONF_TIME_10GB_FILE = 169;
    private static int[][] SORT_BY_KEY_RESULTS_10GB_FILE;
    private final static String SORT_BY_KEY_RESULTS_FILE_NAME_10GB_FILE = "SortByKey/SortByKeyData/10GBFILE/sortByKeyResultsData10gbFile.txt",
    SORT_BY_KEY_DESCRETIZED_RESULTS_FILE_NAME_10GB_FILE = "SortByKey/SortByKeyData/10GBFILE/sortByKeyDescretizedResults10gbFile.txt";
 
    //SORT BY KEY DATA 1 GB FILE
    private static final int SORT_BY_KEY_DEFAULT_CONF_TIME_1GB_FILE = 38;
    private static int[][] SORT_BY_KEY_RESULTS_1GB_FILE;
    private final static String SORT_BY_KEY_RESULTS_FILE_NAME_1GB_FILE = "SortByKey/SortByKeyData/1GBFILE/sortByKeyResultsData1gbFile.txt",
    SORT_BY_KEY_DESCRETIZED_RESULTS_FILE_NAME_1GB_FILE = "SortByKey/SortByKeyData/1GBFILE/sortByKeyDescretizedResults1gbFile.txt";
     //---------------------------------------\\





    //============= SUFFLING ===================\\
    //SHUFFLING DATA 10 GB FILE
    private static final int SHUFFLING_DEFAULT_CONF_TIME_10GB_FILE = 137;
    private static int[][] SHUFFLING_RESULTS_10GB_FILE;
    private final static String SHUFFLING_RESULTS_FILE_NAME_10GB_FILE = "Shuffling/ShufflingData/10GBFILE/shufflingResultsData10gbFile.txt",
    SHUFFLING_DESCRETIZED_RESULTS_FILE_NAME_10GB_FILE = "Shuffling/ShufflingData/10GBFILE/shufflingDescretizedResults10gbFile.txt";

     //SHUFFLING DATA 1 GB FILE
     private static final int SHUFFLING_DEFAULT_CONF_TIME_1GB_FILE = 15;
     private static int[][] SHUFFLING_RESULTS_1GB_FILE;
     private final static String SHUFFLING_RESULTS_FILE_NAME_1GB_FILE = "Shuffling/ShufflingData/1GBFILE/shufflingResultsData1gbFile.txt",
     SHUFFLING_DESCRETIZED_RESULTS_FILE_NAME_1GB_FILE = "Shuffling/ShufflingData/1GBFILE/shufflingDescretizedResults1gbFile.txt";
     //---------------------------------------\\





     //============= KMEANS ===================\\
    //KMEANS DATA 10 GB FILE
    private static final int KMEANS_DEFAULT_CONF_TIME_10GB_FILE = 146;
    private static int[][] KMEANS_RESULTS_10GB_FILE;
    private final static String KMEANS_RESULTS_FILE_NAME_10GB_FILE = "KMeans/KmeansData/10GBFILE/kmeansResultsData10gbFile.txt",
    KMEANS_DESCRETIZED_RESULTS_FILE_NAME_10GB_FILE = "KMeans/KmeansData/10GBFILE/kmeansDescretizedResults10gbFile.txt";

    //KMEANS DATA 1_5 GB FILE
    private static final int KMEANS_DEFAULT_CONF_TIME_1_5GB_FILE = 38;
    private static int[][] KMEANS_RESULTS_1_5GB_FILE;
    private final static String KMEANS_RESULTS_FILE_NAME_1_5GB_FILE = "KMeans/KmeansData/1_5GBFILE/kmeansResultsData1_5gbFile.txt",
    KMEANS_DESCRETIZED_RESULTS_FILE_NAME_1_5GB_FILE = "KMeans/KmeansData/1_5GBFILE/kmeansDescretizedResults1_5gbFile.txt";

    
    //This set contains the discretized tables of the step 3 of the algorithm 1 of the systematic tuning methodology
    private static ArrayList<int[][]> descretizedTablesSet10GBFILE = new ArrayList<>(),
     descretizedTablesSet1GBFILE = new ArrayList<>();
  

     //This is the B matrix that is derived from the step 4 of the algorithm 1 of the systematic tuning methodology
     private static int[][] BTable10GBFILE = new int[parametersNumber][parametersNumber];
     private static int[][] BTable1GBFILE = new int[parametersNumber][parametersNumber];

     //This is the B' matrix that is derived from the step 5 of the algorithm 1 of the systematic tuning methodology
     private static int[][] B2Table10GBFILE = new int[parametersNumber][parametersNumber];
     private static int[][] B2Table1GBFILE = new int[parametersNumber][parametersNumber];

    //This is the adjacency matrix of step 6 of the algorithm 1 of the systematic tuning methodology
    private static int[][] adjacencyTable10GBFILE = new int[parametersNumber][parametersNumber];
    private static int[][] adjacencyTable1GBFILE = new int[parametersNumber][parametersNumber];
     

    //Triangle sets of step 6 of the algorithm 1 of the systematic tuning methodology
    private static ArrayList<Triangle> trianglesSet10GBFILE = new ArrayList<>();
    private static ArrayList<Triangle> trianglesSet1GBFILE = new ArrayList<>();

     //Triangle sets with negative score,  of step 6 of the algorithm 1 of the systematic tuning methodology
     private static ArrayList<Triangle> negativeScoreTrianglesSet10GBFILE = new ArrayList<>();
     private static ArrayList<Triangle> negativeScoreTrianglesSet1GBFILE = new ArrayList<>();
    

     //Bit Arrays of step 6 of the algorithm 1 of the systematic tuning methodology
     private static ArrayList<boolean[]> cComplex10GBFILE = new ArrayList<>();
     private static ArrayList<boolean[]> cComplex1GBFILE = new ArrayList<>();

     //CComplex Configyrations
     private static ArrayList<ArrayList<String>> cComplexConfigurationsNames10GBFILE = new ArrayList<>();
     private final static String cComplexConfigurationsNames10GBFileName = "CComplexConfigurations/cComplexConfigurationsNames10gbFile.txt";
     private static ArrayList<ArrayList<String>> cComplexConfigurationsNames1GBFILE = new ArrayList<>();
     private final static String cComplexConfigurationsNames1GBFileName = "CComplexConfigurations/cComplexConfigurationsNames1gbFile.txt";

   //Helper classescC
  //Class that represents a triangle
  static class Triangle {
   
    //triangles vertices
      int v1, v2, v3;
      int triangleScore;
      public Triangle(int v1, int v2, int v3, ArrayList<int[][]> descretizedTableSet){
        this.v1 = v1;
        this.v2 = v2;
        this.v3 = v3;
        this.triangleScore = calculateScore(descretizedTableSet);
      }

      public int calculateScore(ArrayList<int[][]> descretizedTableSet){
        int scoreOfTable1 = descretizedTableSet.get(0)[v1][v2] + descretizedTableSet.get(0)[v2][v3] + descretizedTableSet.get(0)[v3][v1];
        int scoreOfTable2 = descretizedTableSet.get(1)[v1][v2] + descretizedTableSet.get(1)[v2][v3] + descretizedTableSet.get(1)[v3][v1];;
        int scoreOfTable3 = descretizedTableSet.get(2)[v1][v2] + descretizedTableSet.get(2)[v2][v3] + descretizedTableSet.get(2)[v3][v1];;

        int minScore = scoreOfTable1;

        if(scoreOfTable2 < minScore){
          minScore = scoreOfTable2;
        }

        if(scoreOfTable3 < minScore){
          minScore = scoreOfTable3;
        }

        return minScore;
      }
      

      public int getScore(){
        return this.triangleScore;
      }
      
      public int getV1(){
        return v1;
      }

      public int getV2(){
        return v2;
      }

      public int getV3(){
        return v3;
      }

  }
  

  //comarator to compare triangles based on their score
  static class TrianglesScoreComparator implements Comparator<Triangle>{
    
    @Override
    public int compare(TuningMethodology.Triangle triangle0, TuningMethodology.Triangle triangle1) {
      // TODO Auto-generated method stub
      return Integer.compare(triangle0.getScore(), triangle1.getScore());
    }
    
  }
   
     

    public static int[][] getResultsFromFile(String fileName){
        int[][] results = new int[parametersNumber][parametersNumber];
        try{
            int i_counter = 0, j_counter = 0;
            File file = new File(fileName);
            if(file.length() != 0){
                Scanner scanner = new Scanner(file);
                 
                int result;
                while(scanner.hasNext()){
                    result = Integer.parseInt(scanner.next());
                    results[i_counter][j_counter] = result;
                    j_counter++;
                    if(j_counter == 13){
                        j_counter = 0;
                        i_counter++;
                    }
                }
                scanner.close();
            }

         


        }catch (Exception e){
            e.printStackTrace();
        }

        return results;
    }


    public static void writeDecretizedResultsToFile(int[][] descretizedResults, String fileName){
      try(
          BufferedWriter bufferWriter = new BufferedWriter(new FileWriter(fileName));
      ){
        for(int i = 0; i < parametersNumber; i++){
            for(int j = 0; j < parametersNumber; j++){
                if(j != parametersNumber - 1){
                 bufferWriter.write(String.valueOf(descretizedResults[i][j]) + ", "); 
                }else{
                 bufferWriter.write(String.valueOf(descretizedResults[i][j])); 
                }
            }
            bufferWriter.write("\n");
        }
        bufferWriter.close();
      }catch (Exception e){
        e.printStackTrace();
    }
    }



    public static void writeCComplexConfToFile(ArrayList<ArrayList<String>> cComplexConfigurationsNames, String fileName){
      try(
        BufferedWriter bufferWriter = new BufferedWriter(new FileWriter(fileName));
         ){
          for(ArrayList<String> confNames : cComplexConfigurationsNames){
            for(String confName : confNames){
              bufferWriter.write(confName + ", ");
            }
            bufferWriter.write("\n");
         
          }
        
          bufferWriter.close();
         }catch (Exception e){
         e.printStackTrace();
      }
    }


     /**
     * 
     * STEP 3 METHODS
     * 
     */
    public static int descretizeResult(double result){
        int descretizedResult = 0;
          if(result <= -0.3){
            descretizedResult = -6;
          }else if(result <= -0.2){
            descretizedResult = -5;
          }else if(result <= -0.15){
            descretizedResult = -4;
          }else if(result <= -0.1){
            descretizedResult = -3;
          }else if(result <= -0.05){
            descretizedResult = -2;
          }else if(result <= -0.02){
            descretizedResult = -1;
          }else if(result <= 0.0){
            descretizedResult = 0;
          }else if(result <= 0.02){
            descretizedResult = 0;
          }else if(result <= 0.05){
            descretizedResult = 1;
          }else if(result <= 0.1){
            descretizedResult = 2;
          }else if(result <= 0.15){
            descretizedResult = 3;
          }else if(result <= 0.2){
            descretizedResult = 4;
          }else if(result <= 0.3){
            descretizedResult = 5;
          }else{
            descretizedResult = 6;
          }
         



        return descretizedResult;
    }
    
    public static int[][] normalizeTimes(int defaultConfigurationTime, int[][] results){
        int[][] discretizedResTable = new int[parametersNumber][parametersNumber];
        for(int i = 0; i < parametersNumber; i++){
            for(int j = 0; j < parametersNumber; j++){
                //zero is for the "-"
                if(results[i][j] != 0){
                    
                   
                    double numberToDescretize = (results[i][j] - defaultConfigurationTime) / (double) defaultConfigurationTime;
                    
                 
                    discretizedResTable[i][j] = descretizeResult(numberToDescretize);
                }else{
                  discretizedResTable[i][j] = 0;
                }
            }
        }

        return discretizedResTable;
    }




    

    public static void generateDiscretizedTables(){
         /**
         * SORT BY KEY BENCHMARK RESULTS
         */
         //get descretized results for sort By key 10gb file
         SORT_BY_KEY_RESULTS_10GB_FILE = getResultsFromFile(SORT_BY_KEY_RESULTS_FILE_NAME_10GB_FILE);
         int[][] sortByKeyDiscretizedResults10gbFile = normalizeTimes(SORT_BY_KEY_DEFAULT_CONF_TIME_10GB_FILE, SORT_BY_KEY_RESULTS_10GB_FILE);
         writeDecretizedResultsToFile(sortByKeyDiscretizedResults10gbFile, SORT_BY_KEY_DESCRETIZED_RESULTS_FILE_NAME_10GB_FILE);
          
         //get descretized results for sort By key 1gb file
         SORT_BY_KEY_RESULTS_1GB_FILE = getResultsFromFile(SORT_BY_KEY_RESULTS_FILE_NAME_1GB_FILE);
         int[][] sortByKeyDiscretizedResults1gbFile = normalizeTimes(SORT_BY_KEY_DEFAULT_CONF_TIME_1GB_FILE, SORT_BY_KEY_RESULTS_1GB_FILE);
         writeDecretizedResultsToFile(sortByKeyDiscretizedResults1gbFile, SORT_BY_KEY_DESCRETIZED_RESULTS_FILE_NAME_1GB_FILE);




        /**
         * SHUFFLING BENCHMARK RESULTS
         */
         ////get descretized results for shuffling (10gb file)
         SHUFFLING_RESULTS_10GB_FILE = getResultsFromFile(SHUFFLING_RESULTS_FILE_NAME_10GB_FILE);
         int[][] shufflingDiscretizedResults10gbFile = normalizeTimes(SHUFFLING_DEFAULT_CONF_TIME_10GB_FILE, SHUFFLING_RESULTS_10GB_FILE);
         writeDecretizedResultsToFile(shufflingDiscretizedResults10gbFile, SHUFFLING_DESCRETIZED_RESULTS_FILE_NAME_10GB_FILE);

         ////get descretized results for shuffling (1gb file)
         SHUFFLING_RESULTS_1GB_FILE = getResultsFromFile(SHUFFLING_RESULTS_FILE_NAME_1GB_FILE);
         int[][] shufflingDiscretizedResults1gbFile = normalizeTimes(SHUFFLING_DEFAULT_CONF_TIME_1GB_FILE, SHUFFLING_RESULTS_1GB_FILE);
         writeDecretizedResultsToFile(shufflingDiscretizedResults1gbFile, SHUFFLING_DESCRETIZED_RESULTS_FILE_NAME_1GB_FILE);
        
        /**
         * KMEANS BENCHMARK RESULTS
         */
         //get descretized results for kmeans (10gb file)
         KMEANS_RESULTS_10GB_FILE = getResultsFromFile(KMEANS_RESULTS_FILE_NAME_10GB_FILE);
         int[][] kmeansDiscretizedResults10gbFile = normalizeTimes(KMEANS_DEFAULT_CONF_TIME_10GB_FILE, KMEANS_RESULTS_10GB_FILE);
         writeDecretizedResultsToFile(kmeansDiscretizedResults10gbFile, KMEANS_DESCRETIZED_RESULTS_FILE_NAME_10GB_FILE);

         //get descretized results for kmeans (10gb file)
         KMEANS_RESULTS_1_5GB_FILE = getResultsFromFile(KMEANS_RESULTS_FILE_NAME_1_5GB_FILE);
         int[][] kmeansDiscretizedResults1_5gbFile = normalizeTimes(KMEANS_DEFAULT_CONF_TIME_1_5GB_FILE, KMEANS_RESULTS_1_5GB_FILE);
         writeDecretizedResultsToFile(kmeansDiscretizedResults1_5gbFile, KMEANS_DESCRETIZED_RESULTS_FILE_NAME_1_5GB_FILE);
        
         //save the descretized tabler to a set
         //set for the 10GBFILE
         descretizedTablesSet10GBFILE.add(sortByKeyDiscretizedResults10gbFile);
         descretizedTablesSet10GBFILE.add(shufflingDiscretizedResults10gbFile);
         descretizedTablesSet10GBFILE.add(kmeansDiscretizedResults10gbFile);

         //save the descretized tabler to a set
         //set for the 1GBFILE
         descretizedTablesSet1GBFILE.add(sortByKeyDiscretizedResults1gbFile);
         descretizedTablesSet1GBFILE.add(shufflingDiscretizedResults1gbFile);
         descretizedTablesSet1GBFILE.add(kmeansDiscretizedResults1_5gbFile);

    }


     /**
     * 
     * STEP 4 METHODS
     * 
     */
    //This method checks if the matrix given as a parameter meets the requirements of the step 3 of
    // algorithm 1 of the Systematic Tuning Methodology 
    public static boolean meetsTheRequirements(int[][] table, int pos_i, int pos_j){
        if(pos_i == pos_j){
          if(table[pos_i][pos_j] <= t){
            return true;
          }else{
            return false;
          }
        }else if(pos_i != pos_j){
          if((table[pos_i][pos_j] <= t) && (table[pos_i][pos_j] - table[pos_i][pos_i] <= -1)){
            return true;
          }else{
            return false;
          }
        }

        return false;
    }

    //This method returns the appropriate result based on the requirements that
    // the spedcific matrices meet
     public static int getRequirementsResult(int [][] table1, int [][] table2, int[][] table3, int pos_i, int pos_j){
         if(meetsTheRequirements(table1, pos_i, pos_j) || meetsTheRequirements(table2, pos_i, pos_j) || meetsTheRequirements(table3, pos_i, pos_j)){
            return 1;
         }
       return 0;
    }
  
    //This method returns the resulating B matrix from the matrices
    //that contain the descretized results
    public static int[][] getBTable(int [][] table1, int [][] table2, int[][] table3){
       int[][] BTable = new int[parametersNumber][parametersNumber];
       for(int i = 0; i < parametersNumber; i ++){
         for(int j = 0; j < parametersNumber; j++){
             BTable[i][j] = getRequirementsResult(table1, table2, table3, i, j);
         }
       }

       return BTable;

    }

    public static void generateBTables(){
      //10GB FILE BTable
      BTable10GBFILE = getBTable(descretizedTablesSet10GBFILE.get(0), descretizedTablesSet10GBFILE.get(1), descretizedTablesSet10GBFILE.get(2));

      //10GB FILE BTable
      BTable1GBFILE = getBTable(descretizedTablesSet1GBFILE.get(0), descretizedTablesSet1GBFILE.get(1), descretizedTablesSet1GBFILE.get(2));
      
      
      
    }
    

     /**
     * 
     * STEP 5 METHODS
     * 
     */
    public static int[][] getB2Table(int[][] BTable){
      int[][] B2Table = new int[parametersNumber][parametersNumber];
      for(int i = 0; i < parametersNumber; i ++){
        for(int j = 0; j < parametersNumber; j++){
          if(BTable[i][j] == BTable[j][i] && BTable[j][i] == 1){
            B2Table[i][j] = 1;
          }else{
            B2Table[i][j] = 0;
          }
        }
      }

      return B2Table;
    }
    
    public static void generateB2Table(){
        //10GB FILE BTable
        B2Table10GBFILE = getB2Table(BTable10GBFILE);
        adjacencyTable10GBFILE = B2Table10GBFILE;

        //10GB FILE BTable
        B2Table1GBFILE = getB2Table(BTable1GBFILE);
        adjacencyTable1GBFILE = B2Table1GBFILE;
    }
     

    /**
     * 
     * STEP 6 METHODS
     * 
     */
    
    
    public static ArrayList<Triangle> getTriangles(int[][] B2Table, ArrayList<int[][]> descretizedTableSet){
       ArrayList<Triangle> triangles = new ArrayList<>();
       for(int i = 0; i < parametersNumber; i++){
         for(int j = i + 1; j < parametersNumber; j++){
          if (B2Table[i][j] == 1){
            for (int k = j+1;k < parametersNumber;k++) {
              if (B2Table[i][k] == 1 && B2Table[j][k] == 1){
                Triangle triangle = new Triangle(i, j, k, descretizedTableSet);
                triangles.add(triangle);
              }
            }
          } 
         }
        }

       return triangles;
    }

    //method that prunes triangles with non negative (>=0) score
    public static ArrayList<Triangle> getTrianglesWithNegativeScore(ArrayList<Triangle> trianglesSet){
      ArrayList<Triangle> trianglesWithNegativeScoreSet = new ArrayList<>();  
      for(int i = 0; i < trianglesSet.size(); i++){
          if(trianglesSet.get(i).getScore() < 0){
            trianglesWithNegativeScoreSet.add(trianglesSet.get(i));
          }
        }

        return trianglesWithNegativeScoreSet;
    }


    public static void generateTrianglesSet(){
      //10GB FILE
      trianglesSet10GBFILE = getTriangles(adjacencyTable10GBFILE, descretizedTablesSet10GBFILE);
      //prune triangles with non negative score
      negativeScoreTrianglesSet10GBFILE = getTrianglesWithNegativeScore(trianglesSet10GBFILE);
      //sort them in ascending order based on their score
      negativeScoreTrianglesSet10GBFILE.sort(new TrianglesScoreComparator());

      //1GB FILE
      trianglesSet1GBFILE = getTriangles(adjacencyTable1GBFILE, descretizedTablesSet1GBFILE);
       //prune triangles with non negative score
      negativeScoreTrianglesSet1GBFILE = getTrianglesWithNegativeScore(trianglesSet1GBFILE);
      //sort them in ascending order based on their score
      negativeScoreTrianglesSet1GBFILE.sort(new TrianglesScoreComparator());

    }
   
    //This method checks a specific of if at least two of the pos_1, pos_2, pos_3 is one.
    //if they are then it return true
    //false otherwise
    public static boolean checkCComplexRow(boolean[] cComplexRow, int rowNumber, int pos_1, int pos_2, int pos_3){
      if(cComplexRow[pos_1] && cComplexRow[pos_2] ){
        return true;
      }else if(cComplexRow[pos_1] && cComplexRow[pos_3]){
        return true;
      }else if(cComplexRow[pos_2] && cComplexRow[pos_3]){
        return true;
      }
      return false;
    }
    
    //This method returns the rows of the CComplex where at least two of the pos_1, pos_2, pos_3 is one.
    //if there is not any it returns null;
    public static ArrayList<Integer> getCComplexRows(ArrayList<boolean[]> cComplex, int pos_1, int pos_2, int pos_3){
      ArrayList<Integer> rows = new ArrayList<>();
      for(int i = 0; i < cComplex.size(); i++){
        if(checkCComplexRow(cComplex.get(i), i, pos_1, pos_2, pos_3)){
          rows.add(i);
        }
      }
      return rows;
      
    }

    //Algorithm 2 of the systematic tuning methodology
    //L <- sorted triangles (negativeScoreTrianglesSet) 
    public static ArrayList<boolean[]> getCComplexConfigurationsTable(ArrayList<Triangle> negativeScoreTrianglesSet){
      ArrayList<boolean[]> cComplex = new ArrayList<>(); //CCcomplex ← empty bit-array with 13 columns
      int flag;
      //while L != empty set
      while(negativeScoreTrianglesSet.size() != 0){
        flag = 1;
        
        for(int i = 0; i < negativeScoreTrianglesSet.size(); i++){// for tr(i, j, k) ∈ L
          Triangle tr = negativeScoreTrianglesSet.get(i);
          ArrayList<Integer> rows = getCComplexRows(cComplex, tr.getV1(), tr.getV2(), tr.getV3());
         
          if(rows.size() != 0){ //if there exists row in CCcomplex where at least 2 of the 3 places
                               //i, j, k are 1
            for(int row: rows){
              //Set all 3 places i, j, k to 1 in this row
              cComplex.get(row)[tr.getV1()] = true;
              cComplex.get(row)[tr.getV2()] = true;
              cComplex.get(row)[tr.getV2()] = true;
                negativeScoreTrianglesSet.remove(i); // L <- L - tr
                i--;
            }
          }else if(flag == 1){
            //Create a new row in CCcomplex with places i, j, k set to 1
            boolean[] cComplexRow = new boolean[parametersNumber];
            cComplexRow[tr.getV1()] = true;
            cComplexRow[tr.getV2()] = true;
            cComplexRow[tr.getV2()] = true;
            cComplex.add(cComplexRow);
            flag = 0; // flag <- 0
            negativeScoreTrianglesSet.remove(i); // L <- L - tr
            i--;
        }
          

        }
      }

      return cComplex;
    }

    public static void constructCComplex(){
      //10GB FILE
      cComplex10GBFILE = getCComplexConfigurationsTable(negativeScoreTrianglesSet10GBFILE);

      //1GB FILE
      cComplex1GBFILE = getCComplexConfigurationsTable(negativeScoreTrianglesSet1GBFILE);

      
    }




  public static HashMap<Integer, String> constructParameterIdPairs(){
    HashMap<Integer, String> parameterIdPairs = new HashMap<>();
    parameterIdPairs.put(0, "spark.serializer.KryoSerializer");
    parameterIdPairs.put(1, "spark.memory.storageFraction: 0.3");
    parameterIdPairs.put(2, "spark.memory.storageFraction: 0.1");
    parameterIdPairs.put(3, "spark.memory.storageFraction: 0.7");
    parameterIdPairs.put(4, "spark.reducer.maxSizeInFlight: 72m");
    parameterIdPairs.put(5, "spark.reducer.maxSizeInFlight: 24m");
    parameterIdPairs.put(6, "spark.shuffle.file.buffer: 48k");
    parameterIdPairs.put(7, "spark.shuffle.file.buffer: 16k");
    parameterIdPairs.put(8, "spark.shuffle.compress: false");
    parameterIdPairs.put(9, "spark.io.compression.codec: lzf");
    parameterIdPairs.put(10, "spark.rdd.compress: true");
    parameterIdPairs.put(11, "spark.shuffle.io.preferDirectBufs: false");
    parameterIdPairs.put(12, "spark.shuffle.spill.compress: false");
    return parameterIdPairs;
  }


   public static ArrayList<ArrayList<String>> getCComplexConfigurationsNames(ArrayList<boolean[]> cComplex){
     ArrayList<ArrayList<String>> cComplexConfigurationsNames = new ArrayList<>();
       int k =0;
      for(boolean[] cComplexRow : cComplex){
        ArrayList<String> configurationNamesSetOfcComplexRow = new ArrayList<>();  
        
        for(int i = 0; i < parametersNumber; i++){
          if(cComplexRow[i]){
             configurationNamesSetOfcComplexRow.add(parameterIdPairs.get(i));
          }
        }
        cComplexConfigurationsNames.add(configurationNamesSetOfcComplexRow);
        k++;
      }

      return cComplexConfigurationsNames;
    }

    public static void constructConfigurationsNames(){
      //10GB FILE
      cComplexConfigurationsNames10GBFILE = getCComplexConfigurationsNames(cComplex10GBFILE);
      writeCComplexConfToFile(cComplexConfigurationsNames10GBFILE, cComplexConfigurationsNames10GBFileName);

      //1GB FILE
      cComplexConfigurationsNames1GBFILE = getCComplexConfigurationsNames(cComplex1GBFILE);
      writeCComplexConfToFile(cComplexConfigurationsNames1GBFILE, cComplexConfigurationsNames1GBFileName);
    }
   

    



    public static void printTable(int [][] tableToPrint){
      for(int i = 0; i < parametersNumber; i++){
          for(int j = 0; j < parametersNumber; j++){
              if(j != parametersNumber - 1){
                 System.out.print(tableToPrint[i][j] + ", "); 
              }else{
                 System.out.print(tableToPrint[i][j]); 
              }
          }
          System.out.println("");
      }
  }


  public static void printTable(boolean[] tableToPrint){
    for(int i = 0; i < parametersNumber; i++){
      if(i != parametersNumber - 1){
        if(tableToPrint[i]){
          System.out.print("1 ,");
        }else{
          System.out.print("0 ,");
        }
        
      }else{
        if(tableToPrint[i]){
          System.out.println("1");
        }else{
          System.out.println("0");
        }
      }
     
    }
  }

  
    public static void main (String[] args){
      //step 3
      generateDiscretizedTables();
     

      //step 4 
      generateBTables();
    
      //step 5
      generateB2Table();
     
      //step 6 
      generateTrianglesSet();
      
      constructCComplex();
      
      constructConfigurationsNames();

     
      for(boolean[] b : cComplex1GBFILE){
          for(int i = 0 ; i< b.length; i++){
            System.out.print(b[i] + ", ");
          }
        System.out.println("");
      }
   
     for(ArrayList<String> confNames : cComplexConfigurationsNames1GBFILE){
       System.out.println(confNames);
    
     }
      
    }
}
