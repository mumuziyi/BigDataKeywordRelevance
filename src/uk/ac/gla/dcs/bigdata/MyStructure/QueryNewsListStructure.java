package uk.ac.gla.dcs.bigdata.MyStructure;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class QueryNewsListStructure implements Serializable {

    private static final long serialVersionUID = 125686797741279609L;

    Map<Query,List<RankedResult>> queryListMap;

    public QueryNewsListStructure(Map<Query, List<RankedResult>> queryListMap) {
        this.queryListMap = queryListMap;
    }

    public QueryNewsListStructure() {
    }

    public Map<Query, List<RankedResult>> getQueryListMap() {
        return queryListMap;
    }

    public void setQueryListMap(Map<Query, List<RankedResult>> queryListMap) {
        this.queryListMap = queryListMap;
    }
}
