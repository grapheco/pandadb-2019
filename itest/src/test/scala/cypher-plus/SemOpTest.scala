import java.io.File

import org.apache.commons.io.FileUtils
import org.junit.{Assert, Test}

class SemOpTest extends TestBase {
  @Test
  def testLike(): Unit = {
    //create a new database
    val db = openDatabase();
    val tx = db.beginTx();

    Assert.assertEquals(true, db.execute("return Blob.empty() ~:0.5 Blob.empty() as r").next().get("r").asInstanceOf[Boolean]);
    Assert.assertEquals(true, db.execute("return Blob.empty() ~:0.5 Blob.empty() as r").next().get("r").asInstanceOf[Boolean]);
    Assert.assertEquals(true, db.execute("return Blob.empty() ~:1.0 Blob.empty() as r").next().get("r").asInstanceOf[Boolean]);

    Assert.assertEquals(true, db.execute("return Blob.empty() ~: Blob.empty() as r").next().get("r").asInstanceOf[Boolean]);

    Assert.assertEquals(true, db.execute(
      """return Blob.fromFile('./testdata/mayun1.jpeg')
      ~: Blob.fromFile('./testdata/mayun2.jpeg') as r""")
      .next().get("r").asInstanceOf[Boolean]);

    Assert.assertEquals(false, db.execute(
      """return Blob.fromFile('./testdata/mayun1.jpeg')
      ~: Blob.fromFile('./testdata/lqd.jpeg') as r""")
      .next().get("r").asInstanceOf[Boolean]);

    Assert.assertEquals(true, db.execute("""return Blob.fromFile('./testdata/car1.jpg') ~: '.*NB666.*' as r""")
      .next().get("r").asInstanceOf[Boolean]);

    tx.success();
    tx.close();
    db.shutdown();
  }

  @Test
  def testCompare(): Unit = {
    //create a new database
    val db = openDatabase();
    val tx = db.beginTx();

    try {
      Assert.assertEquals(1.toLong, db.execute("return 1 :: 2 as r").next().get("r"));
      Assert.assertTrue(false);
    }
    catch {
      case _: Throwable => Assert.assertTrue(true);
    }

    Assert.assertEquals(true, db.execute("return <file://./testdata/mayun1.jpeg> :: <file://./testdata/mayun1.jpeg> as r").next().get("r").asInstanceOf[Double] > 0.7);
    Assert.assertEquals(true, db.execute("return <file://./testdata/mayun1.jpeg> :: <file://./testdata/mayun2.jpeg> as r").next().get("r").asInstanceOf[Double] > 0.6);
    Assert.assertEquals(true, db.execute("return '杜 一' :: '杜一' > 0.6 as r").next().get("r"));
    Assert.assertEquals(true, db.execute("return '杜 一' ::jaro '杜一' > 0.6 as r").next().get("r"));

    db.execute("return '杜 一' ::jaro '杜一','Zhihong SHEN' ::levenshtein 'SHEN Z.H'");

    tx.success();
    tx.close();
    db.shutdown();
  }

  @Test
  def testCustomProperty1(): Unit = {
    //create a new database
    val db = openDatabase();
    val tx = db.beginTx();

    Assert.assertEquals(new File("./testdata/car1.jpg").length(),
      db.execute("""return Blob.fromFile('./testdata/car1.jpg')->length as x""")
        .next().get("x"));

    Assert.assertEquals("image/jpeg", db.execute("""return Blob.fromFile('./testdata/car1.jpg')->mime as x""")
      .next().get("x"));

    Assert.assertEquals(500, db.execute("""return Blob.fromFile('./testdata/car1.jpg')->width as x""")
      .next().get("x"));

    Assert.assertEquals(333, db.execute("""return Blob.fromFile('./testdata/car1.jpg')->height as x""")
      .next().get("x"));

    Assert.assertEquals(333, db.execute("""return <file://./testdata/car1.jpg>->height as x""")
      .next().get("x"));

    Assert.assertEquals(null, db.execute("""return Blob.fromFile('./testdata/car1.jpg')->notExist as x""")
      .next().get("x"));

    tx.success();
    tx.close();
    db.shutdown();
  }

  @Test
  def testCustomProperty2(): Unit = {
    //create a new database
    val db = openDatabase();
    val tx = db.beginTx();

    Assert.assertEquals("苏E730V7", db.execute("""return Blob.fromFile('./testdata/car1.jpg')->plateNumber as r""")
      .next().get("r"));

    Assert.assertEquals("我今天早上吃了两个包子", db.execute("""return Blob.fromFile('./testdata/test.wav')->message as r""")
      .next().get("r").asInstanceOf[Boolean]);

    tx.success();
    tx.close();
    db.shutdown();
  }
}