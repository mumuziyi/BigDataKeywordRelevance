package uk.ac.gla.dcs.bigdata.MyMaps;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MyFunctions {
    public static Set<String> getTermsSet(List<Query> list){
        Set<String> set = new HashSet<>();
        for (Query query: list){
            List<String> terms = query.getQueryTerms();
            set.addAll(terms);
        }
        return set;
    }
}
