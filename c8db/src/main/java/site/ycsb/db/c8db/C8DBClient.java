/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db.c8db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDBException;
import com.arangodb.model.TransactionOptions;
import com.arangodb.velocypack.VPackBuilder;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocypack.ValueType;
import com.c8db.C8Collection;
import com.c8db.C8Cursor;
import com.c8db.C8DB;
import com.c8db.C8DBException;
import com.c8db.C8Database;
import com.c8db.Protocol;
import com.c8db.entity.BaseDocument;
import com.c8db.entity.BaseEdgeDocument;
import com.c8db.entity.CollectionType;
import com.c8db.entity.GeoFabricEntity;
import com.c8db.entity.TraversalEntity;
import com.c8db.model.C8TransactionOptions;
import com.c8db.model.CollectionCreateOptions;
import com.c8db.model.DocumentCreateOptions;
import com.c8db.model.DocumentUpdateOptions;
import com.c8db.model.TraversalOptions;
import com.c8db.model.TraversalOptions.Direction;
import com.c8db.util.MapBuilder;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * C8 binding for YCSB framework
 */
public class C8DBClient extends DB {

  private static Logger logger = LoggerFactory.getLogger(C8DBClient.class);

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** C8DB Driver related, Singleton. */
  private C8DB c8db;
  private String databaseName;
  private String tenantName;
  private String collectionName;

  private String collectionType;

  private Boolean dropDBBeforeRun;
  private Boolean waitForSync = false;
  private Boolean transactionUpdate = false;

  public static final String JOIN_PROPORTION_PROPERTY = "joinproportion";

  public static final String JOIN_PROPORTION_PROPERTY_DEFAULT = "0.0";


  public static final String ARRAYSCAN_PROPORTION_PROPERTY = "arrayscanproportion";

  public static final String ARRAYSCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";

  public static final String SEARCH_PROPORTION_PROPERTY = "searchproportion";

  public static final String SEARCH_PROPORTION_PROPERTY_DEFAULT = "0.0";


  public static final String GROUP_PROPORTION_PROPERTY = "groupproportion";


  public static final String GROUP_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * Field name prefix.
   */
  public static final String FIELD_NAME_PREFIX = "fieldnameprefix";

  /**
   * Default value of the field name prefix.
   */
  public static final String FIELD_NAME_PREFIX_DEFAULT = "field";

  private String fieldnameprefix;

  private Double joinoperation;

  private Double arrayscanoperation;

  private Double searchoperation;
  private Double groupoperation;

  private static String[] groupValues = {"groupValue1","groupValue2","groupValue3","groupValue4","groupValue5","groupValue6","groupValue7","groupValue8","groupValue9","groupValue10"};
  private static int groupCounter = 1;
  /**

  /**
   * Initialize any state for this DB. Called once per DB instance; there is
   * one DB instance per client thread.
   *
   * Actually, one client process will share one DB instance here.(Coincide to
   * mongoDB driver)
   */
  @Override
  public void init() throws DBException {
    synchronized (C8DBClient.class) {
      Properties props = getProperties();

      System.out.println("Inside Init Method -->");
      tenantName = props.getProperty("c8db.tenantName", "ycsb-tenant_test.com");
      databaseName = props.getProperty("c8db.databaseName", "ycsb");
      collectionName = props.getProperty("table", "usertable");

      collectionType = props.getProperty("c8db.collectionType", "document");

      fieldnameprefix = props.getProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);

      // Set the DB address
      //String ip = props.getProperty("arangodb.ip", "localhost");

      String ip = props.getProperty("c8db.ip", "api-tejinder.eng.macrometa.io");
      String host = props.getProperty("c8db.host", "api-tejinder-ap-west.eng.macrometa.io");
      String portStr = props.getProperty("b8db.port", "8529");
      int port = Integer.parseInt(portStr);

      String protocolStr = props.getProperty("c8db.protocol", "HTTP_JSON");
      Protocol protocol = Protocol.valueOf(protocolStr);

      // If clear db before run
      String dropDBBeforeRunStr = props.getProperty("c8db.dropDBBeforeRun", "false");
      dropDBBeforeRun = Boolean.parseBoolean(dropDBBeforeRunStr);

      // Set the sync mode
      String waitForSyncStr = props.getProperty("c8db.waitForSync", "false");
      waitForSync = Boolean.parseBoolean(waitForSyncStr);

      // Set if transaction for update
      String transactionUpdateStr = props.getProperty("c8db.transactionUpdate", "false");
      transactionUpdate = Boolean.parseBoolean(transactionUpdateStr);

      //User Credentials

      String user = props.getProperty("c8db.user", "mm@macrometa.io");
      String password = props.getProperty("c8db.password", "");

      joinoperation = Double.parseDouble(props.getProperty(JOIN_PROPORTION_PROPERTY, JOIN_PROPORTION_PROPERTY_DEFAULT));

      arrayscanoperation = Double.parseDouble(props.getProperty(ARRAYSCAN_PROPORTION_PROPERTY, ARRAYSCAN_PROPORTION_PROPERTY_DEFAULT));

      searchoperation = Double.parseDouble(props.getProperty(SEARCH_PROPORTION_PROPERTY, SEARCH_PROPORTION_PROPERTY_DEFAULT));
      groupoperation = Double.parseDouble(props.getProperty(GROUP_PROPORTION_PROPERTY, GROUP_PROPORTION_PROPERTY_DEFAULT));

      // Init C8DB connection
      try {
         c8db = new C8DB.Builder()
                  // .host("api-stoyan.eng.macrometa.io", 443)
                         .host(host, 443)
                   .useSsl(true)
                   .email(user)
                   .password(password).build();
      } catch (Exception e) {
        logger.error("Failed to initialize C8DB", e);
        System.exit(-1);
      }

      if(INIT_COUNT.getAndIncrement() == 0) {
        // Init the database
        if (dropDBBeforeRun) {
          // Try delete first
          try {
         //   arangoDB.db(databaseName).drop();
                //  c8db.db(tenantName,databaseName).drop();
          } catch (C8DBException e) {
            logger.info("Fail to delete DB: {}", databaseName);
          }
        }
        try {
      //    arangoDB.createDatabase(databaseName);
         // c8db.createGeoFabric("demo", databaseName, null, null, null);
                GeoFabricEntity geoFabricEntity = null;
                try
                {
                        geoFabricEntity = c8db.getGeoFabricInformation(tenantName, databaseName);
                        System.out.println("geoFabricEntity Name"+ geoFabricEntity.getName());
                }
                catch(Exception e)
                {
                        System.out.println("geoFabricEntity doesnt exist");
                }

                System.out.println("tenantName"+ tenantName);

                System.out.println("databaseName-->"+databaseName);
                if(geoFabricEntity!=null && geoFabricEntity.getName().equals(databaseName))
                {
                        System.out.println("GeoFabric already exist: " + databaseName);
                }
                else
                {
                        c8db.createGeoFabric(tenantName, "_system", null, null, databaseName);
                        System.out.println("GeoFabric created: " + databaseName);
                }

        } catch (C8DBException e) {
          //logger.error("Failed to create database: {} with ex: {}", databaseName, e.toString());
        }
        try {
      //    arangoDB.db(databaseName).createCollection(collectionName);
        //  c8db.db().createCollection(collectionName);

                if(collectionType.equalsIgnoreCase("graph"))
                {
                        // c8db.db(tenantName,databaseName).createCollection(collectionName+"1", null);
                        // c8db.db(tenantName,databaseName).createCollection(collectionName, new CollectionCreateOptions().type(CollectionType.EDGES));

                        if(joinoperation > 0)
                {
                                C8Collection c8Edgecol = c8db.db(tenantName,databaseName).collection(collectionName);
                                if(!c8Edgecol.exists())
                                {
                                        c8db.db(tenantName,databaseName).createCollection(collectionName, new CollectionCreateOptions().type(CollectionType.EDGES));

                                }

                                C8Collection c8col = c8db.db(tenantName,databaseName).collection(collectionName+"1");
                            if(!c8col.exists())
                            {
                                c8db.db(tenantName,databaseName).createCollection(collectionName+"1");

                            }
                                C8Collection c8childCol = c8db.db(tenantName,databaseName).collection(collectionName+"child");
                                if(!c8childCol.exists())
                                {
                                        c8db.db(tenantName,databaseName).createCollection(collectionName+"child");

                                }

                        }
                        else
                        {
                                 C8Collection c8Edgecol = c8db.db(tenantName,databaseName).collection(collectionName);
                             if(!c8Edgecol.exists())
                             {
                                c8db.db(tenantName,databaseName).createCollection(collectionName, new CollectionCreateOptions().type(CollectionType.EDGES));

                             }
                             else
                             {
                                 System.out.println("Collection already exist: " + databaseName);
                             }

                            C8Collection c8col = c8db.db(tenantName,databaseName).collection(collectionName+"1");
                            if(!c8col.exists())
                            {
                                c8db.db(tenantName,databaseName).createCollection(collectionName+"1");

                            }
                        }

                }
                else
                {
                        if(joinoperation > 0)
                        {
                                C8Collection c8col = c8db.db(tenantName,databaseName).collection(collectionName);
                                if(!c8col.exists())
                                {
                                 c8db.db(tenantName,databaseName).createCollection(collectionName);

                                }
                                C8Collection c8childCol = c8db.db(tenantName,databaseName).collection(collectionName+"child");
                                if(!c8childCol.exists())
                                {
                                        c8db.db(tenantName,databaseName).createCollection(collectionName+"child");

                                }

                        }
                        else
                        {
                                C8Collection c8col = c8db.db(tenantName,databaseName).collection(collectionName);
                                if(!c8col.exists())
                                {
                                 c8db.db(tenantName,databaseName).createCollection(collectionName);

                                }
                        }
                }

                 //c8db.db().createCollection(collectionName,new CollectionCreateOptions().type(CollectionType.EDGES));
          logger.info("Collection created: " + collectionName);
        } catch (C8DBException e) {
          logger.error("Failed to create collection: {} with ex: {}", collectionName, e.toString());
        }
        logger.info("C8DB client connection created to {}:{}", host, user);

        // Log the configuration
        logger.info("C8DB Configuration: dropDBBeforeRun: {}; address: {}:, user: {}; databaseName: {};"
                    + " waitForSync: {}; transactionUpdate: {};",
                    dropDBBeforeRun, host, user, databaseName, waitForSync, transactionUpdate);
      }
    }
  }



  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   *
   * Actually, one client process will share one DB instance here.(Coincide to
   * mongoDB driver)
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
   //   arangoDB.shutdown();
    //  arangoDB = null;

      c8db.shutdown();
      c8db = null;
      logger.info("Local cleaned up.");
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *      The name of the table
   * @param key
   *      The record key of the record to insert.
   * @param values
   *      A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the
   *     {@link DB} class's description for a discussion of error codes.
   */
  //@Override
  public Status insertOLD(String table, String key, Map<String, ByteIterator> values) {
          try {
              BaseDocument toInsert = new BaseDocument(key);
              for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                toInsert.addAttribute(entry.getKey(), byteIteratorToString(entry.getValue()));
              }
              DocumentCreateOptions options = new DocumentCreateOptions().waitForSync(waitForSync);
              c8db.db(tenantName,databaseName).collection(table).insertDocument(toInsert, options);
              return Status.OK;
            } catch (ArangoDBException e) {
              logger.error("Exception while trying insert {} {} with ex {}", table, key, e.toString());
            }
          return Status.ERROR;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

          C8Database c8database = c8db.db(tenantName,databaseName);


        try
        {
                if(collectionType.equalsIgnoreCase("graph"))
        {
                        final BaseDocument doc = new BaseDocument();
                    doc.setKey(key);


                    BaseDocument joinDocument = null;


                    c8database.collection(collectionName+"1").insertDocument(doc, null);

                    doc.setKey(key+"Tejinder");
                    c8database.collection(collectionName+"1").insertDocument(doc, null);

                    String edgeKey = key + "_plus_"+ "Tejinder";
                    String from = collectionName+"1/"+key;
                    String to = collectionName+"1/"+key+"Tejinder";

                        if(joinoperation > 0)
                        {
                                  joinDocument = new BaseDocument(edgeKey);
                        }

                    final BaseEdgeDocument edge = new BaseEdgeDocument();
                    edge.setKey(edgeKey);
                    edge.setFrom(from);
                    edge.setTo(to);



                    List<String> arrays = new ArrayList<>();
                arrays.add(edgeKey);
                for (Map.Entry<String, ByteIterator> entry : values.entrySet())
                {
                        edge.addAttribute(entry.getKey(), byteIteratorToString(entry.getValue()));
                        arrays.add(entry.getKey());
                        if(joinoperation > 0)
                        {
                                joinDocument.addAttribute(entry.getKey(), byteIteratorToString(entry.getValue()));
                        }
                     }

                  if(arrayscanoperation > 0)
                      {
                          edge.addAttribute("arrayField", arrays);
                      }
                  if(searchoperation > 0)
                  {
                          edge.addAttribute("searchviewfield", edgeKey);
                  }

                  if(groupoperation > 0)
                  {
                          edge.addAttribute("groupbyfield", groupValues[groupCounter]);
                          if(groupCounter > 0)
                          {
                                  groupCounter = 1;
                          }
                          groupCounter = groupCounter + 1;
                  }

                  DocumentCreateOptions options = new DocumentCreateOptions().waitForSync(waitForSync);
            /*   try
                 {*/
                         c8database.collection(collectionName).insertDocument(edge, options);



                          if(joinoperation > 0)
                                  {
                                   //   options = new DocumentCreateOptions().waitForSync(waitForSync);
                                      joinDocument.addAttribute("childkey", edgeKey);
                                      c8database.collection(table+"child").insertDocument(joinDocument, options);
                                  }
                /* }
                 catch(Exception e)
                 {
                         logger.error("Exception while trying insert {} {} with ex {}", table, key, e.toString());
                 }*/

                  return Status.OK;
        }
                else
                {

                          BaseDocument toInsert = new BaseDocument(key);
                          BaseDocument joinDocument = null;
                          if(joinoperation > 0)
                          {
                                  joinDocument = new BaseDocument("child"+key);
                          }

                         /*
                       * Adding expireAt attribute for KeyValue documents
                       */
                      if(collectionType.equalsIgnoreCase("keyvalue"))
                      {
                          toInsert.addAttribute("value", key);
                          toInsert.addAttribute("expireAt", "1729311400");
                      }
                      else
                      {
                          List<String> arrays = new ArrayList<>();
                          arrays.add(key);
                          for (Map.Entry<String, ByteIterator> entry : values.entrySet())
                              {
                                  String fieldValue = byteIteratorToString(entry.getValue());
                                  toInsert.addAttribute(entry.getKey(), fieldValue);
                                  arrays.add(entry.getKey());
                                  if(joinoperation > 0)
                                  {
                                          joinDocument.addAttribute(entry.getKey(), fieldValue);
                                  }
                              }

                          if(arrayscanoperation > 0)
                              {
                                  toInsert.addAttribute("arrayField", arrays);
                              }
                          if(searchoperation > 0)
                          {
                                  toInsert.addAttribute("searchviewfield", key);
                          }
                          if(groupoperation > 0)
                          {
                                  toInsert.addAttribute("groupbyfield", groupValues[groupCounter]);
                                  if(groupCounter > 0)
                                  {
                                          groupCounter = 1;
                                  }
                                  groupCounter = groupCounter + 1;
                          }
                      }

                      DocumentCreateOptions options = new DocumentCreateOptions().waitForSync(waitForSync);
                   /*  try
                     {
                        */
                         c8database.collection(table).insertDocument(toInsert, options);


                              if(joinoperation > 0)
                                  {
                                    //  options = new DocumentCreateOptions().waitForSync(waitForSync);
                                      joinDocument.addAttribute("childkey", key);

                                      c8database.collection(table+"child").insertDocument(joinDocument, options);
                                  }
                    /* }
                     catch(Exception e)
                     {
                         logger.error("Exception while trying insert {} {} with ex {}", table, key, e.toString());
                     }*/
                      return Status.OK;
                        }

        }
        catch (C8DBException e) {
        logger.error("C8DBException while trying insert {} {} with ex {}", table, key, e.toString());
        e.printStackTrace();


        }
        return Status.ERROR;

  }
  /**
   * Read a record from the database. Each field/value pair from the result
   * will be stored in a HashMap.
   *
   * @param table
   *      The name of the table
   * @param key
   *      The record key of the record to read.
   * @param fields
   *      The list of fields to read, or null for all of them
   * @param result
   *      A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
   //   VPackSlice document = arangoDB.db(databaseName).collection(table).getDocument(key, VPackSlice.class, null);

      if(collectionType.equalsIgnoreCase("graph"))
      {
          key = key + "_plus_"+ "Tejinder";
      }

      VPackSlice document1 = c8db.db(tenantName,databaseName).collection(table).getDocument(key, VPackSlice.class, null);

   /*   System.out.println("Checking Read System--");
      System.out.println("Checking table --"+table);
      System.out.println("Checking key --"+key);
      System.out.println("Checking fields --"+fields);

      System.out.println("Checking document1 --"+document1);
     */

      if (document1!=null && !this.fillMap(result, document1, fields)) {
        return Status.ERROR;
      }
      return Status.OK;
    } catch (C8DBException e) {
      logger.error("Exception while trying read {} {} with ex {}", table, key, e.toString());
    }
    return Status.ERROR;
  }


  @Override
  public Status join(String table, String startkey, int recordcount, Set<String> fields,
              Vector<HashMap<String, ByteIterator>> result) {

          C8Cursor<VPackSlice> c8cursor = null;
          try {
   //   VPackSlice document = arangoDB.db(databaseName).collection(table).getDocument(key, VPackSlice.class, null);


      //String joinQuery = "FOR u IN "+table+"  FOR c IN "+table+"child"+"  FILTER u._key == c.childkey FILTER u._key == '"+startkey+"'  RETURN { "+table+": u, "+table+"child"+": c }";
                  String joinQuery = "FOR u IN "+table+"  FOR c IN "+table+"child"+"  FILTER u._key == c.childkey FILTER u._key == '"+startkey+"'  RETURN { "+table+": u._key }";

      //System.out.println("joinQuery -->"+ joinQuery);

       c8cursor  = c8db.db(tenantName,databaseName).query(joinQuery, null, null, VPackSlice.class);

      while (c8cursor.hasNext()) {
          VPackSlice aDocument = c8cursor.next();
          HashMap<String, ByteIterator> aMap = new HashMap<String, ByteIterator>(aDocument.size());
          if (!this.fillMap(aMap, aDocument)) {
            return Status.ERROR;
          }
          result.add(aMap);
        }
        return Status.OK;
      } catch (Exception e) {
        logger.error("Exception while trying doing join {} {} {} with ex {}", table, startkey, recordcount, e.toString());
      } finally {
        if (c8cursor != null) {
          try {
                c8cursor.close();
          } catch (IOException e) {
            logger.error("Fail to close cursor", e);
          }
        }
      }
      return Status.ERROR;
  }


  @Override
  public Status group(String table, String startkey, int recordcount, Set<String> fields,
              Vector<HashMap<String, ByteIterator>> result) {

          C8Cursor<VPackSlice> c8cursor = null;


          try {
   //   VPackSlice document = arangoDB.db(databaseName).collection(table).getDocument(key, VPackSlice.class, null);


      //String joinQuery = "FOR u IN "+table+"  FOR c IN "+table+"child"+"  FILTER u._key == c.childkey FILTER u._key == '"+startkey+"'  RETURN { "+table+": u, "+table+"child"+": c }";
                //String groupQuery = "FOR u IN "+table + " COLLECT field0 = u.field0 RETURN  {field0}";
                StringBuilder groupQuery = new StringBuilder("");
                //groupQuery.append("FOR u IN ").append(table).append(" COLLECT ").append(fieldnameprefix).append("0 = u.").append(fieldnameprefix).append("0 LIMIT ").append(recordcount).append(" RETURN {").append(fieldnameprefix).append("0}");


                // Added LIMIT 100 for multinode environments, as with Multinode it was not returing data for more than 100 records.
                //groupQuery.append("FOR u IN ").append(table).append(" COLLECT key=u._key, groupbyfield = u.groupbyfield INTO groups RETURN { 'groupbyfield' : groupbyfield, 'key' : key  }");
                groupQuery.append("FOR u IN ").append(table).append(" COLLECT key=u._key, groupbyfield = u.groupbyfield INTO groups LIMIT 100  RETURN { 'groupbyfield' : groupbyfield, 'key' : key  }");

                //System.out.println("GroupQuery -->"+ groupQuery);

                c8cursor  = c8db.db(tenantName,databaseName).query(groupQuery.toString(), null, null, VPackSlice.class);


       while (c8cursor.hasNext()) {
          VPackSlice aDocument = c8cursor.next();

          //HashMap<String, ByteIterator> aMap = new HashMap<String, ByteIterator>(aDocument.size());
          HashMap<String, ByteIterator> aMap = new HashMap<String, ByteIterator>();
          if (!this.fillMap(aMap, aDocument)) {
            return Status.ERROR;
          }
          result.add(aMap);
        }
        return Status.OK;
      } catch (Exception e) {
        logger.error("Exception while trying doing groupBy {} {} {} with ex {}", table, startkey, recordcount, e.toString());
      } finally {
        if (c8cursor != null) {
          try {
                c8cursor.close();
          } catch (IOException e) {
            logger.error("Fail to close cursor", e);
          }
        }
      }
      return Status.ERROR;
  }

  @Override
  public Status aggregate(String table, String startkey, int recordcount, Set<String> fields,
              Vector<HashMap<String, ByteIterator>> result) {

          C8Cursor<VPackSlice> c8cursor = null;


          try {

                StringBuilder aggregateQuery = new StringBuilder("");
                aggregateQuery.append("FOR u IN ").append(table).append(" COLLECT AGGREGATE totalCount = LENGTH(1)  return { count: totalCount } ");



                c8cursor  = c8db.db(tenantName,databaseName).query(aggregateQuery.toString(), null, null, VPackSlice.class);


       while (c8cursor.hasNext()) {
          VPackSlice aDocument = c8cursor.next();

          //HashMap<String, ByteIterator> aMap = new HashMap<String, ByteIterator>(aDocument.size());
          HashMap<String, ByteIterator> aMap = new HashMap<String, ByteIterator>();
          if (!this.fillMap(aMap, aDocument)) {
            return Status.ERROR;
          }
          result.add(aMap);
        }
        return Status.OK;
      } catch (Exception e) {
        logger.error("Exception while trying doing aggregate {} {} {} with ex {}", table, startkey, recordcount, e.toString());
      } finally {
        if (c8cursor != null) {
          try {
                c8cursor.close();
          } catch (IOException e) {
            logger.error("Fail to close cursor", e);
          }
        }
      }
      return Status.ERROR;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *      The name of the table
   * @param key
   *      The record key of the record to write.
   * @param values
   *      A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *     description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {

        if(collectionType.equalsIgnoreCase("graph"))
        {
          key = key + "_plus_"+ "Tejinder";
        }

        /*System.out.println("key in update -->"+ key);
        System.out.println("transactionUpdate in update -->"+ transactionUpdate);

        System.out.println("values in update -->"+ values);
        */
        if (!transactionUpdate) {
        BaseDocument updateDoc = new BaseDocument();
        for (Entry<String, ByteIterator> field : values.entrySet()) {
          updateDoc.addAttribute(field.getKey(), byteIteratorToString(field.getValue()));
        }
  //      arangoDB.db(databaseName).collection(table).updateDocument(key, updateDoc);
    /*    try
        {
                c8db.db(tenantName,databaseName).collection(table).updateDocument(key, updateDoc);
        }
        catch (Exception e) {
            logger.error("Exception while trying update {} {} with ex {}", table, key, e.toString());
          }*/
       // c8db.db(tenantName,databaseName).collection(table).updateDocument(key, updateDoc);
        DocumentUpdateOptions docUpdateOptions = new DocumentUpdateOptions();
        c8db.db(tenantName,databaseName).collection(table).updateDocument(key, updateDoc, docUpdateOptions);
        return Status.OK;
      } else {
        // id for documentHandle
        String transactionAction = "function (id) {"
               // use internal database functions
            + "var db = require('internal').db;"
              // collection.update(document, data, overwrite, keepNull, waitForSync)
            + String.format("db._update(id, %s, true, false, %s);}",
                mapToJson(values), Boolean.toString(waitForSync).toLowerCase());
        TransactionOptions options = new TransactionOptions();
        options.writeCollections(table);
        options.params(createDocumentHandle(table, key));

        C8TransactionOptions c8options = new C8TransactionOptions();
        c8options.writeCollections(table);
        c8options.params(createDocumentHandle(table, key));

     //   arangoDB.db(databaseName).transaction(transactionAction, Void.class, options);
        c8db.db(tenantName,databaseName).transaction(transactionAction, String.class, c8options);
        return Status.OK;
      }
    } catch (C8DBException e) {
      logger.error("C8DBException while trying update {} {} with ex {}", table, key, e.toString());
    }
    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *      The name of the table
   * @param key
   *      The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the
   *     {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
   //   arangoDB.db(databaseName).collection(table).deleteDocument(key);
        c8db.db(tenantName,databaseName).collection(table).deleteDocument(key);
      return Status.OK;
    } catch (C8DBException e) {
      logger.error("Exception while trying delete {} {} with ex {}", table, key, e.toString());
    }
    return Status.ERROR;
  }

  /**
   * Perform a range scan for a set of records in the database. Each
   * field/value pair from the result will be stored in a HashMap.
   *
   * @param table
   *      The name of the table
   * @param startkey
   *      The record key of the first record to read.
   * @param recordcount
   *      The number of records to read
   * @param fields
   *      The list of fields to read, or null for all of them
   * @param result
   *      A Vector of HashMaps, where each HashMap is a set field/value
   *      pairs for one record
   * @return Zero on success, a non-zero error code on error. See the
   *     {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    ArangoCursor<VPackSlice> cursor = null;

    C8Cursor<VPackSlice> c8cursor = null;

    /*
    System.out.println("Inside scan in C8DB Client startkey "+ startkey);
          System.out.println("Inside scan in C8DB Client recordcount "+ recordcount);
          System.out.println("Inside scan in C8DB Client result "+ result);

          System.out.println("Inside scan in C8DB Client fields"+ fields);
        */

    try {
     /* String aqlQuery = String.format(
          "FOR target IN %s FILTER target._key >= @key SORT target._key ASC LIMIT %d RETURN %s ", table,
          recordcount, constructReturnForAQL(fields, "target"));

      System.out.println("Inside scan in C8DB Client aqlQuery"+ aqlQuery);
      */
        /*
         * TOD : This is the limit been applied for Multinode for returning only upto 200 records. Need to check why?
         */
        if(recordcount > 100)
        {
                //recordcount = 100;

                recordcount = 50;
        }
      String returnFields = "{"+ fieldnameprefix + "0:target."+fieldnameprefix + "0,"+ fieldnameprefix + "2:target."+fieldnameprefix + "2 }";
      String scanQuery = String.format(
              "FOR target IN %s FILTER target._key >= @key SORT target._key ASC LIMIT %d RETURN %s ", table,
              recordcount, returnFields);

 //     System.out.println("Inside scan in C8DB Client scanQuery "+ scanQuery);
      Map<String, Object> bindVars = new MapBuilder().put("key", startkey).get();

      c8cursor = c8db.db(tenantName,databaseName).query(scanQuery, bindVars, null, VPackSlice.class);

      while (c8cursor.hasNext()) {
        VPackSlice aDocument = c8cursor.next();
        HashMap<String, ByteIterator> aMap = new HashMap<String, ByteIterator>(aDocument.size());
        if (!this.fillMap(aMap, aDocument)) {
          return Status.ERROR;
        }
        result.add(aMap);
      }
   //   System.out.println("Result Size ->"+ result.size());
      return Status.OK;
    } catch (Exception e) {
      logger.error("Exception while trying scan {} {} {} with ex {}", table, startkey, recordcount, e.toString());
    } finally {
      if (c8cursor != null) {
        try {
                c8cursor.close();
        } catch (IOException e) {
          logger.error("Fail to close cursor", e);
        }
      }
    }
    return Status.ERROR;
  }

  /**
   * Perform a range scan for a set of records in the database. Each
   * field/value pair from the result will be stored in a HashMap.
   *
   * @param table
   *      The name of the table
   * @param startkey
   *      The record key of the first record to read.
   * @param recordcount
   *      The number of records to read
   * @param fields
   *      The list of fields to read, or null for all of them
   * @param result
   *      A Vector of HashMaps, where each HashMap is a set field/value
   *      pairs for one record
   * @return Zero on success, a non-zero error code on error. See the
   *     {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status arrayscan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    ArangoCursor<VPackSlice> cursor = null;

    C8Cursor<VPackSlice> c8cursor = null;

    /*
    System.out.println("Inside arrayscan in C8DB Client startkey "+ startkey);
          System.out.println("Inside arrayscan in C8DB Client recordcount "+ recordcount);
          System.out.println("Inside arrayscan in C8DB Client result "+ result);

          System.out.println("Inside arrayscan in C8DB Client fields"+ fields);
          // FOR target IN %s FILTER CONTAINS(target.traits, @key) == true SORT target._key DESC LIMIT 880 RETURN target
    */
    try {
     /* String aqlQuery = String.format(
          "FOR target IN %s FILTER target._key >= @key SORT target._key ASC LIMIT %d RETURN %s ", table,
          recordcount, constructReturnForAQL(fields, "target"));
    */

    /*  String arrayscanQuery = String.format(
              "FOR target IN %s FILTER CONTAINS(target.arrayField, @key) == true  SORT target._key ASC LIMIT %d RETURN %s ", table,
              recordcount, constructReturnForAQL(fields, "target"));
      */
      String arrayscanQuery = String.format(
              "FOR target IN %s FILTER  @key IN target.arrayField[*] RETURN %s ", table,
              constructReturnForAQL(fields, "target"));

    //  System.out.println("Inside arrayscan in C8DB Client arrayscanQuery"+ arrayscanQuery);

      Map<String, Object> bindVars = new MapBuilder().put("key", startkey).get();
    //  cursor = arangoDB.db(databaseName).query(aqlQuery, bindVars, null, VPackSlice.class);

      c8cursor = c8db.db(tenantName,databaseName).query(arrayscanQuery, bindVars, null, VPackSlice.class);

      while (c8cursor.hasNext()) {
        VPackSlice aDocument = c8cursor.next();
        HashMap<String, ByteIterator> aMap = new HashMap<String, ByteIterator>(aDocument.size());
        if (!this.fillMap(aMap, aDocument)) {
          return Status.ERROR;
        }
        result.add(aMap);
      }
     // System.out.println("Result Size ->"+ result.size());
      return Status.OK;
    } catch (Exception e) {
      logger.error("Exception while trying arrayscan {} {} {} with ex {}", table, startkey, recordcount, e.toString());
    } finally {
      if (c8cursor != null) {
        try {
                c8cursor.close();
        } catch (IOException e) {
          logger.error("Fail to close cursor", e);
        }
      }
    }
    return Status.ERROR;
  }

  @Override
  public Status search(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {

    C8Cursor<VPackSlice> c8cursor = null;

    /*
    System.out.println("Inside search in C8DB Client startkey "+ startkey);
          System.out.println("Inside search in C8DB Client recordcount "+ recordcount);
          System.out.println("Inside search in C8DB Client result "+ result);

          System.out.println("Inside search in C8DB Client fields"+ fields);
        */
    String tableview = table+"view";

    try {
      String searchQuery = String.format( "FOR doc IN %s SEARCH doc.searchviewfield == @key RETURN doc ", tableview);

      //System.out.println("Inside search in C8DB Client searchQuery"+ searchQuery);

      Map<String, Object> bindVars = new MapBuilder().put("key", startkey).get();
    //  cursor = arangoDB.db(databaseName).query(aqlQuery, bindVars, null, VPackSlice.class);

      c8cursor = c8db.db(tenantName,databaseName).query(searchQuery, bindVars, null, VPackSlice.class);

      while (c8cursor.hasNext()) {
        VPackSlice aDocument = c8cursor.next();
        HashMap<String, ByteIterator> aMap = new HashMap<String, ByteIterator>(aDocument.size());
        if (!this.fillMap(aMap, aDocument)) {
          return Status.ERROR;
        }
        result.add(aMap);
      }
     // System.out.println("Result Size ->"+ result.size());
      return Status.OK;
    } catch (Exception e) {
      logger.error("Exception while trying search {} {} {} with ex {}", table, startkey, recordcount, e.toString());
    } finally {
      if (c8cursor != null) {
        try {
                c8cursor.close();
        } catch (IOException e) {
          logger.error("Fail to close cursor", e);
        }
      }
    }
    return Status.ERROR;
  }

  @Override
  public Status graphTraversal(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {

        /*  System.out.println("Inside graphTraversal in C8DB Client startkey "+ startkey);
          System.out.println("Inside graphTraversal in C8DB Client recordcount "+ recordcount);
          System.out.println("Inside graphTraversal in C8DB Client result "+ result);

          System.out.println("Inside graphTraversal in C8DB Client fields"+ fields);
          if(fields!=null)
          {
                  for(String s: fields)
                  {
                          System.out.println("field s -->"+ s);
                  }
          }
         */

      try
      {
                  final TraversalOptions options = new TraversalOptions().edgeCollection(collectionName).startVertex(collectionName+"1/"+startkey)
                      .direction(Direction.outbound);
              final TraversalEntity<BaseDocument, BaseEdgeDocument> traversal = c8db.db(tenantName,databaseName).executeTraversal(BaseDocument.class,
                      BaseEdgeDocument.class, options);

              Collection<BaseDocument> vertices = traversal.getVertices();
              List<String> traversalResults = new ArrayList<String>();
              for(BaseDocument vertex: vertices)
              {
                  //System.out.println("traversal vertex.getKey()-->"+vertex.getKey());
                  traversalResults.add(vertex.getKey());
              }
              return Status.OK;
      }
      catch(Exception e)
      {
          logger.error("Exception while graphTraversal {} {} with ex {}", table, startkey, e.toString());
      }
      return Status.ERROR;
  }

  @Override
  public Status graphShortestPath(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {



        //  String queryString = "FOR v, e IN OUTBOUND SHORTEST_PATH 'circles/A' TO 'circles/D' GRAPH 'traversalGraph' RETURN {'vertex': v._key, 'edge': e._key}";

          StringBuilder query = new StringBuilder("");
          query.append("WITH ").append(collectionName).append("1 ");
          query.append("FOR v, e IN OUTBOUND SHORTEST_PATH '");
          query.append(collectionName).append("1/").append(startkey).append("' TO '");
          query.append(collectionName).append("1/").append(startkey).append("Tejinder").append("' ");
          query.append(collectionName).append(" RETURN {'vertex': v._key, 'edge': e._key}");

          String queryString = query.toString();

          //System.out.println("Graph Shortest Query-->"+ queryString);

      C8Cursor<Pair> cursor = c8db.db(tenantName,databaseName).query(queryString, null, null, Pair.class);
    //  final Collection<String> collection = toVertexCollection(cursor);
      List<String> shortestPathResults = new ArrayList<String>();
      for (; cursor.hasNext();) {
          final Pair pair = cursor.next();

         // System.out.println("pair.getVertext()-->"+ pair.getVertex());
         // System.out.println("pair.getEdge()-->"+ pair.getEdge());
          shortestPathResults.add(pair.getVertex());
      }


      return Status.OK;

  }
  private String createDocumentHandle(String collection, String documentKey) throws C8DBException {
    validateCollectionName(collection);
    return collection + "/" + documentKey;
  }

  private void validateCollectionName(String name) throws C8DBException {
    if (name.indexOf('/') != -1) {
      throw new C8DBException("does not allow '/' in name.");
    }
  }


  private String constructReturnForAQL(Set<String> fields, String targetName) {
    // Construct the AQL query string.
    String resultDes = targetName;
    if (fields != null && fields.size() != 0) {
      StringBuilder builder = new StringBuilder("{");
      for (String field : fields) {
        builder.append(String.format("\n\"%s\" : %s.%s,", field, targetName, field));
      }
      //Replace last ',' to newline.
      builder.setCharAt(builder.length() - 1, '\n');
      builder.append("}");
      resultDes = builder.toString();
    }
    return resultDes;
  }

  private boolean fillMap(Map<String, ByteIterator> resultMap, VPackSlice document) {
    return fillMap(resultMap, document, null);
  }

  /**
   * Fills the map with the properties from the BaseDocument.
   *
   * @param resultMap
   *      The map to fill/
   * @param document
   *      The record to read from
   * @param fields
   *      The list of fields to read, or null for all of them
   * @return isSuccess
   */
  private boolean fillMap(Map<String, ByteIterator> resultMap, VPackSlice document, Set<String> fields) {

          if (fields == null || fields.size() == 0) {
      for (Iterator<Entry<String, VPackSlice>> iterator = document.objectIterator(); iterator.hasNext();) {
        Entry<String, VPackSlice> next = iterator.next();
        VPackSlice value = next.getValue();

        if (value.isString()) {
          resultMap.put(next.getKey(), stringToByteIterator(value.getAsString()));
        }
        else if (value.isInt() || value.isInteger()) {
            resultMap.put(next.getKey(), stringToByteIterator(value.getAsInt()+""));
          }
        else if (value.isArray()) {
            resultMap.put(next.getKey(), stringToByteIterator(value.toString()));
          }else if (!value.isCustom()) {
          logger.error("Error! Not the format expected! Actually is {}",
              value.getClass().getName());
          return false;
        }
      }
    } else {
      for (String field : fields) {
        VPackSlice value = document.get(field);

        if (value.isString()) {
          resultMap.put(field, stringToByteIterator(value.getAsString()));
        } else if (!value.isCustom()) {
          logger.error("Error! Not the format expected! Actually is {}",
              value.getClass().getName());
          return false;
        }
      }
    }
    return true;
  }

  private String byteIteratorToString(ByteIterator byteIter) {
    return new String(byteIter.toArray());
  }

  private ByteIterator stringToByteIterator(String content) {
    return new StringByteIterator(content);
  }

  private String mapToJson(Map<String, ByteIterator> values) {
    VPackBuilder builder = new VPackBuilder().add(ValueType.OBJECT);
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      builder.add(entry.getKey(), byteIteratorToString(entry.getValue()));
    }
    builder.close();
   // return arangoDB.util().deserialize(builder.slice(), String.class);
    return c8db.util().deserialize(builder.slice(), String.class);
  }

  public static class Pair {

      private String vertex;
      private String edge;

      public String getVertex() {
          return vertex;
      }

      public void setVertex(final String vertex) {
          this.vertex = vertex;
      }

      public String getEdge() {
          return edge;
      }

      public void setEdge(final String edge) {
          this.edge = edge;
      }

  }
}
