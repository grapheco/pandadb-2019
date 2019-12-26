package cn.pandadb.tool;

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:56 2019/12/26
 * @Modified By:
 */
public class JUnsafePNodeLauncher {
    public static void main(String[] args) {
        String num = args[0];
        String dbPathStr = "../itest/output/testdb/db" + num;
        String confFilePathStr = "../itest/testdata/localnode" + num + ".conf";
        String[] startArgs = {dbPathStr, confFilePathStr};
        PNodeServerStarter.main(startArgs);
    }
}
