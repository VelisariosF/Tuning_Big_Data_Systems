package FileGenerator;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class FileGenerator {
    
   public static String generateRandomWord(int numOfCharacters){
     
      Random random = new Random();
      char[] word = new char[numOfCharacters]; // words of length 3 through 10. (1 and 2 letter words are boring.)
          for(int j = 0; j < word.length; j++)
          {
            word[j] = (char)('a' + random.nextInt(26));
          }
          return new String(word);
   }
   
   public static void writeToFile(String strContent){
    OutputStream opStream = null;
    try {
       // String strContent = "This example shows how to write byte content to a file";
        byte[] byteContent = strContent.getBytes();
        File myFile = new File("sortByKeyTestFile10gb.txt");
        // check if file exist, otherwise create the file before writing
        if (!myFile.exists()) {
            myFile.createNewFile();
        }
        opStream = new FileOutputStream(myFile, true);
        opStream.write(byteContent);
        opStream.flush();
    } catch (IOException e) {
        e.printStackTrace();
    } finally{
        try{
            if(opStream != null) opStream.close();
        } catch(Exception ex){
             
        }
    }
   }
   

   public static void generateSortByKeyTestFile(int size){
    for(int i = 0; i < size; i ++){
        String key = generateRandomWord(10);
        String value = generateRandomWord(90);
        writeToFile(key + ":" + value + "\n");
    }

   }

   public static  void generateKMeansTestFile(int gb){
        int numOfFeatures = 200, numOfRecords = 270000;
        int k = 0;
        while(k < gb){
           ArrayList<ArrayList<Double>> testArrays = new ArrayList<>();
           Random r = new Random();
 
           for(int i = 0; i < numOfRecords; i ++){
             ArrayList<Double> testArray = new ArrayList<>();
             for(int j = 0; j < numOfFeatures; j++){
               testArray.add(r.nextDouble());
            }
 
             testArrays.add(testArray);
           }

   

      try(
           FileWriter writer = new FileWriter("kmeansData1gb.csv", true)
      ){
           for(int i = 0; i < numOfRecords; i ++){
    
              for(int j = 0; j < numOfFeatures; j++){
                  if(j != numOfFeatures - 1){
                    writer.write((Double)testArrays.get(i).get(j) + ",");
                 }else{
                    writer.write((Double)testArrays.get(i).get(j) + "");
              }
          
            }
            writer.write("\n");
           } 
          writer.close();
       }catch(Exception e){
          e.printStackTrace();
       }

       k++;
     } 
  }

    

    public static void main(String[] args){
        //for 10gb input 100 million
        generateSortByKeyTestFile(100000000);

        //for 5gb file input 5
        //generateKMeansTestFile(1);
       
    }
}
