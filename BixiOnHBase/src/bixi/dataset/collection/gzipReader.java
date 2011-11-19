package bixi.dataset.collection;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

/**
 * Read some data from a gzip file.
 * 
 * @author Ian F. Darwin, http://www.darwinsys.com/
 * @version $Id: ReadGZIP.java,v 1.4 2004/03/06 20:54:38 ian Exp $
 */
public class gzipReader {
  public static void main(String[] argv) throws IOException {
	  String filename = "/home/dan/Downloads/gdb-7.3a.tar.gz";
	  readGZip(filename);
  }
  
  public static void readGZip(String fileName){
      // use BufferedReader to get one line at a time
      BufferedReader gzipReader = null;
      try {
         // simple loop to dump the contents to the console
         gzipReader = new BufferedReader(
               new InputStreamReader(
                new GZIPInputStream(
                new FileInputStream(fileName))));
         
         while (gzipReader.ready()) {
            System.out.println(gzipReader.readLine());
         }
         gzipReader.close();
         
     } catch (FileNotFoundException fnfe) {
         System.out.println("The file was not found: " + fnfe.getMessage());
      } catch (IOException ioe) {
         System.out.println("An IOException occurred: " + ioe.getMessage());
      } finally {
         if (gzipReader != null) {
            try { gzipReader.close(); } catch (IOException ioe) {}
         }
      }	  
  }
  
  public static void readGZip2(String fileName) throws IOException{

	    // Since there are 4 constructor calls here, I wrote them out in full.
	    // In real life you would probably nest these constructor calls.
	    FileInputStream fin = new FileInputStream(fileName);
	    GZIPInputStream gzis = new GZIPInputStream(fin);
	    InputStreamReader xover = new InputStreamReader(gzis);
	    BufferedReader is = new BufferedReader(xover);

	    String line;
	    // Now read lines of text: the BufferedReader puts them in lines,
	    // the InputStreamReader does Unicode conversion, and the
	    // GZipInputStream "gunzip"s the data from the FileInputStream.
	    while ((line = is.readLine()) != null)
	      System.out.println("Read: " + line);	  
  }
  
  
}