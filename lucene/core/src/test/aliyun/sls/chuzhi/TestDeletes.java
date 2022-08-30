package aliyun.sls.chuzhi;


import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import java.util.function.Supplier;

public class TestDeletes {

    public static void main(String[] args) throws Exception{
        Directory dir = new ByteBuffersDirectory();
        String softDeletesField  = "deleted";

        Supplier<IndexWriterConfig> configSupplier = new Supplier<IndexWriterConfig>() {
            @Override
            public IndexWriterConfig get() {
                IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
                indexWriterConfig.setUseCompoundFile(false);
                indexWriterConfig.setSoftDeletesField(softDeletesField);
                indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
                return indexWriterConfig;
            }
        };

        {
            IndexWriter indexWriter = new IndexWriter(dir, configSupplier.get());
            Document doc;

            // 文档0
            doc = new Document();
            doc.add(new StringField("docName", "document0", Field.Store.YES));
            doc.add(new StringField("value", "v0", Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("dv0")));
            indexWriter.addDocument(doc);

            // 文档1 - 1
            doc = new Document();
            doc.add(new StringField("docName", "document1", Field.Store.YES));
            doc.add(new StringField("value", "v1", Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("dv1")));
            indexWriter.addDocument(doc);

            // 文档1 - 2 （已经软删除）
            doc = new Document();
            doc.add(new StringField("docName", "document1", Field.Store.YES));
            doc.add(new StringField("value", "v1-soft-deleted", Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("v1-soft-deleted")));
            doc.add(new NumericDocValuesField(softDeletesField, 1));
            indexWriter.addDocument(doc);

            // 文档2
            doc = new Document();
            doc.add(new StringField("docName", "document2", Field.Store.YES));
            doc.add(new StringField("value", "v2", Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("dv2")));
            indexWriter.addDocument(doc);

            // 文档3 (已经软删除)
            doc = new Document();
            doc.add(new StringField("docName", "document3", Field.Store.YES));
            doc.add(new StringField("value", "v3", Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("dv3")));
            doc.add(new NumericDocValuesField(softDeletesField, 110));
            indexWriter.addDocument(doc);

            // 硬删除
            doc = new Document();
            doc.add(new StringField("docName", "document1", Field.Store.YES));
            doc.add(new StringField("value", "v1-new", Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("dv1-new")));
            indexWriter.updateDocument(new Term("docName", "document1"), doc);

            // 软删除
            doc = new Document();
            doc.add(new StringField("docName", "document2", Field.Store.YES));
            doc.add(new StringField("value", "v2-new", Field.Store.YES));
            doc.add(new BinaryDocValuesField("sqlCol", new BytesRef("dv2-new")));
            indexWriter.softUpdateDocument(new Term("docName", "document2"), doc, new NumericDocValuesField(softDeletesField, 10));

            // 更新DV
            indexWriter.updateBinaryDocValue(new Term("docName", "document0"), "sqlCol", new BytesRef("dv0-new"));

            indexWriter.flush();
            // 生成一个段
            indexWriter.commit();

            {
                DirectoryReader reader = DirectoryReader.open(indexWriter);

                ScoreDoc[] scoreDocs = (new IndexSearcher(reader)).search(new MatchAllDocsQuery(), 100).scoreDocs;
                for (ScoreDoc scoreDoc : scoreDocs) {
                    System.out.println("docId: 文档" + scoreDoc.doc
                            + ", docName: " + reader.document(scoreDoc.doc).get("docName")
                            + ", value: " + reader.document(scoreDoc.doc).get("value"));
                }
                System.out.println("---------");

                LeafReader leafReader = reader.leaves().get(0).reader();
                BinaryDocValues binaryDVs = leafReader.getBinaryDocValues("sqlCol");
                while (binaryDVs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    System.out.println("docId:" + binaryDVs.docID() + ", value: " + binaryDVs.binaryValue().utf8ToString());
                }
                System.out.println("---------");

                NumericDocValues numericDVs = leafReader.getNumericDocValues(softDeletesField);
                while (numericDVs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    System.out.println("docId:" + numericDVs.docID() + ", value: " + numericDVs.longValue());
                }
                System.out.println("---------");
            }

            indexWriter.close();
        }

        {
            IndexWriter indexWriter = new IndexWriter(dir, configSupplier.get());

            indexWriter.updateBinaryDocValue(new Term("docName", "document0"), "sqlCol", new BytesRef("dv0-new2"));
            indexWriter.flush();

            indexWriter.updateBinaryDocValue(new Term("docName", "document0"), "sqlCol", new BytesRef("dv0-new3"));
            indexWriter.flush();
        }

    }

}
