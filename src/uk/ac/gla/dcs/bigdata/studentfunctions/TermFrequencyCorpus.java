package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsEssential;


public class TermFrequencyCorpus {
    public static int getTermFrequencyCorpus(String term, Dataset<NewsEssential> newsEssentialDataset, LongAccumulator accumulator){
//        Iterate through the dataset and get the term frequency for each news article, then sum them up to get the term frequency in the corpus
        var accum = accumulator;
        newsEssentialDataset.foreach(newsEssential -> {
            int termFrequency = newsEssential.getQueryFrequency(term);
            accum.add(termFrequency);
        });
        var freqInCorpus = accum.value();
//        Convert it to int
        return (int) (long)freqInCorpus;
    }

    public static short getTermFrequencyArticle(String term, NewsEssential newsEssential) {
        return newsEssential.getQueryFrequency(term);
    }
}
