package aliyun.sls.chuzhi;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRef;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class TestMerge {

    public static void main(String[] args) throws Exception {
        Directory dir = new MMapDirectory(new File("/tmp/lucene").toPath());

        IndexWriter indexWriter = new IndexWriter(dir, getWriterConfig(null));
        for (int idx = 0; idx < 3; idx++) {
            Document doc = new Document();
            doc.add(new StringField("docName", "document#" + idx, Field.Store.YES));
            doc.add(new StringField("value", "v" + idx, Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("dv" + idx)));
            indexWriter.addDocument(doc);

            if (idx == 1) {
                indexWriter.updateBinaryDocValue(new Term("docName", "document#0"), "sqlCol", new BytesRef("dv0-new"));
            }

            indexWriter.flush();
        }

//        new Thread() {
//            @Override
//            public void run() {
//                try {
//                    TimeUnit.SECONDS.sleep(5);
//                    System.out.println("thread started");
//                    indexWriter.updateBinaryDocValue(new Term("docName", "document#0"), "sqlCol", new BytesRef("dv0-new2"));
//                    indexWriter.flush();
//                } catch (Throwable e) {
//                    e.printStackTrace();
//                }
//                System.out.println("thread ended");
//            }
//        }.start();

        System.out.println("kicked off force merge now");
        indexWriter.forceMerge(1);

        indexWriter.commit();
        indexWriter.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        System.out.println("leaf readers: " + reader.leaves().size());
    }

    private static IndexWriterConfig getWriterConfig(IndexDeletionPolicy indexDeletionPolicy) {
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
        indexWriterConfig.setUseCompoundFile(false);
        indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        indexWriterConfig.setMergePolicy(new LogDocMergePolicy());
        if (indexDeletionPolicy != null) {
            indexWriterConfig.setIndexDeletionPolicy(indexDeletionPolicy);
        }
        return indexWriterConfig;
    }

    private static void search(DirectoryReader reader, String tip) throws IOException {
        System.out.println("start search: " + tip);
        ScoreDoc[] scoreDocs = (new IndexSearcher(reader)).search(new MatchAllDocsQuery(), 100).scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            System.out.println("docId: #" + scoreDoc.doc
                    + ", docName: " + reader.document(scoreDoc.doc).get("docName")
                    + ", value: " + reader.document(scoreDoc.doc).get("value"));
        }
    }
}
