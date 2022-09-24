package aliyun.sls.chuzhi;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class TestSort {

    public static void main(String[] args) throws Exception {
        Directory dir = new ByteBuffersDirectory();

        {
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
            indexWriterConfig.setUseCompoundFile(false);

            IndexWriter indexWriter = new IndexWriter(dir, indexWriterConfig);
            Document doc;

            // 文档0
            doc = new Document();
            doc.add(new StringField("docName", "document0", Field.Store.YES));
            doc.add(new NumericDocValuesField("value", 100));
            indexWriter.addDocument(doc);

            // 文档1
            doc = new Document();
            doc.add(new StringField("docName", "document1", Field.Store.YES));
            doc.add(new NumericDocValuesField("value", 20));
            indexWriter.addDocument(doc);

            // 文档2
            doc = new Document();
            doc.add(new StringField("docName", "document2", Field.Store.YES));
            doc.add(new NumericDocValuesField("value", 5));
            indexWriter.addDocument(doc);

            // 文档3
            doc = new Document();
            doc.add(new StringField("docName", "document3", Field.Store.YES));
            doc.add(new NumericDocValuesField("value", 50));
            indexWriter.addDocument(doc);

            // 文档4
            doc = new Document();
            doc.add(new StringField("docName", "document4", Field.Store.YES));
            doc.add(new NumericDocValuesField("value", 200));
            indexWriter.addDocument(doc);

            indexWriter.commit();
            indexWriter.close();
        }

        {
            DirectoryReader reader = DirectoryReader.open(dir);

            Sort sort = new Sort(new SortField("value", SortField.Type.LONG, false));
            ScoreDoc[] scoreDocs = (new IndexSearcher(reader)).search(
                    new MatchAllDocsQuery(), 100, sort, false).scoreDocs;
            for (ScoreDoc scoreDoc : scoreDocs) {
                System.out.println("docId: #" + scoreDoc.doc
                        + ", docName: " + reader.document(scoreDoc.doc).get("docName"));
            }

            reader.close();
        }
    }
}
