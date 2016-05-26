import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LinkCountSeq {

  public static void main(String[] args) throws IOException {
    Map<Integer, Integer> map = new HashMap<>();
    BufferedReader br = new BufferedReader(new FileReader("web-Google.txt"));
    String line = br.readLine();
    PrintWriter out = new PrintWriter("out.txt", "UTF-8");
    while(line != null){
   
      String[] values = line.split("\t");
      Integer from, to;
      from = Integer.parseInt(values[0]);
      to = Integer.parseInt(values[1]);
      
      int previous = 0;
      if (map.containsKey(to)) {
        previous = map.get(to).intValue();
      }
      
      map.put(to, new Integer(previous + 1));
      line = br.readLine();
    }

    for (Integer key : map.keySet()) {
      out.println(key + " " + map.get(key).intValue());
    }
  }
}