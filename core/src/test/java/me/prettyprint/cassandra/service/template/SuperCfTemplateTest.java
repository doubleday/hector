package me.prettyprint.cassandra.service.template;

import static org.junit.Assert.*;

import java.util.Arrays;

import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Test;


public class SuperCfTemplateTest extends BaseColumnFamilyTemplateTest {

  @Test
  public void testSuperCfInsertReadTemplate() {
    SuperCfTemplate<String, String, String> sTemplate = 
      new ThriftSuperCfTemplate<String, String, String>(keyspace, "Super1", se, se, se);
    SuperCfUpdater sUpdater = sTemplate.createUpdater("skey1","super1");
    sUpdater.setString("sub_col_1", "sub_val_1");
    sTemplate.update(sUpdater);

    SuperCfResult<String,String,String> result = sTemplate.querySuperColumn("skey1", "super1");
    
    assertEquals("sub_val_1",result.getString("super1","sub_col_1"));
  }
  
  
  @Test
  public void testSuperCfMultiSc() {
    SuperCfTemplate<String, String, String> sTemplate = 
      new ThriftSuperCfTemplate<String, String, String>(keyspace, "Super1", se, se, se);
    SuperCfUpdater sUpdater = sTemplate.createUpdater("skey2","super1");
    sUpdater.setString("sub1_col_1", "sub1_val_1");
    sUpdater.addSuperColumn("super2");
    sUpdater.setString("sub2_col_1", "sub2_val_1");
    sTemplate.update(sUpdater);

    SuperCfResult<String,String,String> result = sTemplate.querySuperColumns("skey2", Arrays.asList("super1","super2"));
    assertEquals(2,result.getSuperColumns().size());
    /*for (String sName : result.getSuperColumns() ) {
      result.getString(sName,"sub1_col_1");
    }*/
    
    //assertEquals("sub1_val_1",result.getString("sub1_col_1"));
    //assertEquals("sub2_val_1",result.next().getString("sub2_col_1"));
    
  }
  
  @Test
  public void testQuerySingleSubColumn() {
    SuperCfTemplate<String, String, String> sTemplate = 
      new ThriftSuperCfTemplate<String, String, String>(keyspace, "Super1", se, se, se);
    SuperCfUpdater sUpdater = sTemplate.createUpdater("skey3","super1");
    sUpdater.setString("sub1_col_1", "sub1_val_1");
    sTemplate.update(sUpdater);
    
    HColumn<String,String> myCol = sTemplate.querySingleSubColumn("skey3", "super1", "sub1_col_1", se);
    assertEquals("sub1_val_1", myCol.getValue());
  }
  
  
  @Test
  public void testQuerySingleSubColumnEmpty() {
    SuperCfTemplate<String, String, String> sTemplate = 
      new ThriftSuperCfTemplate<String, String, String>(keyspace, "Super1", se, se, se);
    SuperCfUpdater sUpdater = sTemplate.createUpdater("skey3","super1");
    sUpdater.setString("sub1_col_1", "sub1_val_1");
    sTemplate.update(sUpdater);
    
    HColumn<String,String> myCol = sTemplate.querySingleSubColumn("skey3", "super2", "sub1_col_1", se);
    assertNull(myCol);
  }
  
  @Test
  public void testSuperCfInsertReadMultiKey() {
    SuperCfTemplate<String, String, String> sTemplate = 
      new ThriftSuperCfTemplate<String, String, String>(keyspace, "Super1", se, se, se);
    SuperCfUpdater sUpdater = sTemplate.createUpdater("skey1","super1");
    sUpdater.setString("sub_col_1", "sub_val_1");
    sUpdater.addKey("skey2");
    sUpdater.addSuperColumn("super1");
    sUpdater.setString("sub_col_1", "sub_val_2");
    sTemplate.update(sUpdater);

    SuperCfResult<String,String,String> result = sTemplate.querySuperColumns(Arrays.asList("skey1","skey2"), Arrays.asList("super1"));
    
    assertEquals("sub_val_1",result.getString("super1","sub_col_1"));
    assertEquals("sub_val_2",result.next().getString("super1","sub_col_1"));
    
  }
  
  @Test
  public void testSuperCfInsertReadMultiKeyNoSc() {
    SuperCfTemplate<String, String, String> sTemplate = 
      new ThriftSuperCfTemplate<String, String, String>(keyspace, "Super1", se, se, se);
    SuperCfUpdater sUpdater = sTemplate.createUpdater("skey1","super1");
    sUpdater.setString("sub_col_1", "sub_val_1");
    sUpdater.addKey("skey2");
    sUpdater.addSuperColumn("super1");
    sUpdater.setString("sub_col_1", "sub_val_2");
    sTemplate.update(sUpdater);

    SuperCfResult<String,String,String> result = sTemplate.querySuperColumns(Arrays.asList("skey1","skey2"));
    
    assertEquals("sub_val_1",result.getString("super1","sub_col_1"));
    assertEquals("sub_val_2",result.next().getString("super1","sub_col_1"));
    
  }
  
}
