//import java.io.File
//
//import cn.pandadb.blob.Blob
//import org.apache.commons.io.FileUtils
//import org.neo4j.graphdb.GraphDatabaseService
//
///**
//  * Created by bluejoe on 2019/4/13.
//  */
//trait TestBase {
//  val testDbDir = new File("./output/testdb");
//  val testConfPath = new File("./testdata/neo4j.conf").getPath;
//
//  def setupNewDatabase(dbdir: File = testDbDir, conf: String = testConfPath): Unit = {
//    FileUtils.deleteDirectory(dbdir);
//    //create a new database
//    val db = openDatabase(dbdir, conf);
//    val tx = db.beginTx();
//    //create a node
//    val node1 = db.createNode();
//
//    node1.setProperty("name", "bob");
//    node1.setProperty("age", 40);
//
//    //with a blob property
//    node1.setProperty("photo", Blob.fromFile(new File("./testdata/test.png")));
//    //blob array
//    node1.setProperty("album", (0 to 5).map(x => Blob.fromFile(new File("./testdata/test.png"))).toArray);
//
//    val node2 = db.createNode();
//    node2.setProperty("name", "alex");
//    //with a blob property
//    node2.setProperty("photo", Blob.fromFile(new File("./testdata/test1.png")));
//    node2.setProperty("age", 10);
//
//    //node2.createRelationshipTo(node1, RelationshipType.withName("dad"));
//
//    tx.success();
//    tx.close();
//    db.shutdown();
//  }
//}
