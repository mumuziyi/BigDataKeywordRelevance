package bin.bigdata;

import bin.bigdata.providedutilities.TextDistanceCalculator;

public class Test {
    public static void main(String[] args) {
        String str1 = "Book World: ‘Fug You,’ by Ed Sanders, a look back at the ’60s band";
        String str2 = "Book World: In ‘Bond Girl’ by Erin Duffy, a woman takes on high finance";

        if((str1.charAt(0) == 't' || str1.charAt(0) == 'T') && (str1.charAt(1) == 'h' || str1.charAt(1) == 'H')){

        }

        System.out.println(TextDistanceCalculator.similarity(str1,str2));
    }
}
