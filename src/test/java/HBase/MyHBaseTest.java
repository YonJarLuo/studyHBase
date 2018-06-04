package HBase;

import common.HBaseHelp;
import org.junit.Test;

/**
 * Created by LuoYJ on 2018/6/1.
 */
public class MyHBaseTest {

    //创建表
    @Test
    public void test01(){
        HBaseHelp hBaseHelp = new HBaseHelp();
        String[] columns = {"food","motion","star"};
        boolean mylike = hBaseHelp.createTable("mylike", columns);
        System.out.println(mylike);
    }

    //删除表
    @Test
    public void test02(){
        HBaseHelp hBaseHelp = new HBaseHelp();
        boolean mylike = hBaseHelp.deleteTable("mylike");
        System.out.println(mylike);
    }

    //查看已有表
    @Test
    public void test03(){
        HBaseHelp hBaseHelp = new HBaseHelp();
        hBaseHelp.listTable();
    }

    //插入数据
    @Test
    public void test04(){
        HBaseHelp hBaseHelp = new HBaseHelp();
//        boolean b = hBaseHelp.putDate("mylike", "nan", "star", "music", "Jay");
//        boolean b = hBaseHelp.putDate("mylike", "man", "star", "who", "Jay");
        boolean b = hBaseHelp.putDate("mylike", "man", "motion", "basketball", "kebi");
        System.out.println(b);
    }

    //删除
    @Test
    public void test5(){
        HBaseHelp hBaseHelp = new HBaseHelp();
        boolean b = hBaseHelp.deleteDate("mylike", "man", "", "");
        System.out.println(b);
    }

    //查询数据 get
    @Test
    public void test6(){
        HBaseHelp hBaseHelp = new HBaseHelp();
//        hBaseHelp.getDate("mylike","man","star","dancer");
        hBaseHelp.getDate("mylike","man","","");

    }

    //查询数据 scan
    @Test
    public void test7(){
        HBaseHelp hBaseHelp = new HBaseHelp();
//        hBaseHelp.getDate("mylike","man","star","dancer");
        String[] columns = {"star","food"};  //列族
        hBaseHelp.scanDate("mylike","","",columns,"");

    }

    //增加列族
    @Test
    public void test08(){
        HBaseHelp hBaseHelp = new HBaseHelp();
        String[] columns = {"newWorld"};
        boolean mylike = hBaseHelp.addColumn("mylike", columns);
        System.out.println(mylike);
    }
}
