package cn.pandadb.tool;

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 19:52 2019/12/25
 * @Modified By:
 */
public class UnsafePNodeLauncher {

    public static void main(String[] args) {
        String num = args[0];
        String dbPathStr = "../itest/output/testdb/db" + num;
        String confFilePathStr = "../itest/testdata/localnode" + num + ".conf";
        String[] startArgs = {dbPathStr, confFilePathStr};
        PNodeServerStarter.main(startArgs);
    }
}
