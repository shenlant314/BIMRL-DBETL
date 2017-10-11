//
// BIMRL (BIM Rule Language) Simplified Schema ETL (Extract, Transform, Load) library: this library transforms IFC data into BIMRL Simplified Schema for RDBMS. 
// This work is part of the original author's Ph.D. thesis work on the automated rule checking in Georgia Institute of Technology
// Copyright (C) 2013 Wawan Solihin (borobudurws@hotmail.com)
// 
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 3 of the License, or any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Oracle.DataAccess.Types;
#if ORACLE
using Oracle.DataAccess.Client;
using NetSdoGeometry;
#endif
#if POSTGRES
using Npgsql;
using NpgsqlTypes;
#endif
using BIMRL.Common;

namespace BIMRL.BIMRLGraph
{
    public class GraphData
    {
      public static BIMRLCommon refBimrlCommon;
      List<int> nodeIdList = new List<int>();
      List<string> nodeNameList;
      List<string> nodeTypeList;
      List<string> activeList;
      List<int> hierarchyLevelList;
      List<int> parentIdList;

      List<int> linkIdList;
      List<string> linkNameList;
      List<int> startNodeList;
      List<int> endNodeList;
      List<string> linkTypeList;
      List<string> linkActive;
      List<int> linkParentID;
#if ORACLE
      List<OracleParameterStatus> linkParentStatus;
#endif

      public GraphData()
      {
         refBimrlCommon = new BIMRLCommon();
         try { 
            DBOperation.ExistingOrDefaultConnection();
         }
         catch
         {
            if (DBOperation.UIMode)
            {
               //BIMRLErrorDialog erroDlg = new BIMRLErrorDialog(refBimrlCommon);
               //erroDlg.ShowDialog();
               Console.Write(refBimrlCommon.ErrorMessages);
            }
            else
               Console.Write(refBimrlCommon.ErrorMessages);
            return;
         }

         nodeIdList = new List<int>();
         nodeNameList = new List<string>();
         nodeTypeList = new List<string>();
         activeList = new List<string>();
         hierarchyLevelList = new List<int>();
         parentIdList = new List<int>();

         linkIdList = new List<int>();
         linkNameList = new List<string>();
         startNodeList = new List<int>();
         endNodeList = new List<int>();
         linkTypeList = new List<string>();
         linkActive = new List<string>();
      }

      void resetLists()
      {
         nodeIdList.Clear();
         nodeNameList.Clear();
         nodeTypeList.Clear();
         activeList.Clear();
         hierarchyLevelList.Clear();
         parentIdList.Clear();

         linkIdList.Clear();
         linkNameList.Clear();
         startNodeList.Clear();
         endNodeList.Clear();
         linkTypeList.Clear();
         linkActive.Clear();
         if (linkParentID != null)
               linkParentID.Clear();
#if ORACLE
         if (linkParentStatus != null)
               linkParentStatus.Clear();
#endif
      }

      public bool createCirculationGraph(int FedID)
      {
         bool status = true;
         string sqlStmt = null;
         int nodeID = 1;
         int linkID = 1;
         string containerQuery;
         Dictionary<string, int> nodeProcessed = new Dictionary<string, int>();
         Dictionary<string, int> parentNodeIdDict = new Dictionary<string, int>();
         Dictionary<string, Tuple<string, string>> dependencyDict = new Dictionary<string, Tuple<string, string>>();
#if ORACLE
         OracleCommand command = new OracleCommand("", DBOperation.DBConn);
         OracleCommand command2 = new OracleCommand("", DBOperation.DBConn);
         OracleCommand commandPlSql = new OracleCommand("", DBOperation.DBConn);
#endif
#if POSTGRES
         NpgsqlCommand command = new NpgsqlCommand("", DBOperation.DBConn);
         NpgsqlCommand command2 = new NpgsqlCommand("", DBOperation.DBConn);
#endif
         string networkName = "CIRCULATION_" + FedID.ToString("X4");
         try
         {
            // Create network tables
            dropNetwork(networkName);   // Drop it first if already existing

#if ORACLE
            sqlStmt = "SDO_NET.CREATE_LOGICAL_NETWORK";
            commandPlSql.CommandText = sqlStmt;
            commandPlSql.CommandType = CommandType.StoredProcedure;
            commandPlSql.BindByName = true;
            int noHierarchy = 2;
            bool isDirected = false;
            commandPlSql.Parameters.Add("network", OracleDbType.Varchar2, networkName, ParameterDirection.Input);
            commandPlSql.Parameters.Add("no_of_hierarchy_levels", OracleDbType.Int32, noHierarchy, ParameterDirection.Input);
            commandPlSql.Parameters.Add("is_directed", OracleDbType.Boolean, isDirected, ParameterDirection.Input);
            commandPlSql.ExecuteNonQuery();
            DBOperation.commitTransaction();
            commandPlSql.Dispose();
#endif
#if POSTGRES
            //var location = new Uri(Assembly.GetEntryAssembly().GetName().CodeBase);
            //string exePath = new FileInfo(location.AbsolutePath).Directory.FullName.Replace("%20"," ");
            //// Create tables for storing the graph
            //DBOperation.executeScript(Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_graphtab_cr.sql"), DBOperation.currFedModel.FederatedID);
            DBOperation.ExecuteSystemScript("BIMRL_graphtab_cr.sql");
#endif
            // 
            string elemList = "'IFCSPACE','IFCDOOR','IFCOPENINGELEMENT','IFCSTAIR','IFCSTAIRFLIGHT','IFCRAMP','IFCRAMPFLIGHT','IFCTRANSPORTELEMENT'";    // IFC source
            elemList = elemList + ",'OST_ROOMS','OST_AREAS','OST_MEPSPACES','OST_DOORS','OST_SWALLRECTOPENING','OST_STAIRS','OST_STAIRSRUNS','OST_RAMPS'";
            sqlStmt = "SELECT ELEMENTID,CONTAINER FROM " + DBOperation.formatTabName("BIMRL_ELEMENT", FedID) + " WHERE UPPER(ELEMENTTYPE) IN (" + elemList +")"
                     + " OR ((UPPER(NAME) LIKE '%LIFT%' OR UPPER(NAME) LIKE '%ELEVATOR%'))";
            //+" OR (ELEMENTTYPE='IFCBUILDINGELEMENTPROXY' AND (UPPER(NAME) LIKE '%LIFT%' OR UPPER(NAME) LIKE '%ELEVATOR%'))";
            command.CommandText = sqlStmt;
#if ORACLE
            OracleDataReader reader = command.ExecuteReader();
            OracleDataReader reader2 = null;
#endif
#if POSTGRES
            NpgsqlDataReader reader = command.ExecuteReader();
            NpgsqlDataReader reader2 = null;
#endif
            while (reader.Read())
            {
               string elemID = reader.GetString(0);
               string container = string.Empty;
               if (reader.IsDBNull(1))
               {
                  container = containerFromDetail(FedID, elemID);

                  if (string.IsNullOrEmpty(container))
                  {
                     container = containerFromHost(FedID, elemID);
                     if (string.IsNullOrEmpty(container))
                     {
                        // If still cannot identify the container even from the host, skip
                        refBimrlCommon.StackPushError("%% Warning: Can't find container for ElementID: " + elemID);
                        continue;               // Can't seem to find the appropriate container
                     }
                  }
               }
               else
               {
                  container = reader.GetString(1);
               }
               string storeyID = storeyContainer(FedID, container);

               if (string.IsNullOrEmpty(storeyID))
               {
                  refBimrlCommon.StackPushError("%% Warning: Can't find the appropriate Storey for ElementID: " + elemID);
                  continue;               // Can't seem to find the appropriate container
               }

               if (!nodeProcessed.ContainsKey(storeyID))
               {
#if ORACLE
                  string insStorey = "Insert into " + networkName + "_NODE$ (NODE_ID, NODE_NAME, NODE_TYPE, ACTIVE, HIERARCHY_LEVEL) "
                                          + "VALUES (" + nodeID.ToString() + ",'" + storeyID + "','IFCBUILDINGSTOREY','Y',1)";
                  command2.CommandText = insStorey;
                  command2.ExecuteNonQuery();
#endif
#if POSTGRES
                  nodeIdList.Add(nodeID);
                  nodeNameList.Add(storeyID);
                  nodeTypeList.Add("IFCBUILDINGSTOREY");
                  activeList.Add("Y");
                  hierarchyLevelList.Add(1);
#endif
                  nodeProcessed.Add(storeyID, nodeID);
                  parentNodeIdDict.Add(elemID, nodeID);
                  nodeID++;
               }
               else
               {
                  parentNodeIdDict.Add(elemID, nodeProcessed[storeyID]);
               }
            }
            reader.Dispose();

#if POSTGRES
            if (nodeIdList.Count > 0)
               insertNode(networkName, nodeIdList, nodeNameList, nodeTypeList, activeList, hierarchyLevelList, null);
#endif

            string boundList = "'IFCSPACE','IFCDOOR','IFCOPENINGELEMENT'";    // IFC source
            boundList = boundList + ",'OST_ROOMS','OST_AREAS','OST_MEPSPACES','OST_DOORS','OST_SWALLRECTOPENING'";

            sqlStmt = "SELECT SPACEELEMENTID, BOUNDARYELEMENTID, BOUNDARYELEMENTTYPE FROM " + DBOperation.formatTabName("BIMRL_SPACEBOUNDARYV", FedID)
                        + " WHERE upper(BOUNDARYELEMENTTYPE) IN (" + boundList + ") order by spaceelementid";
            command.CommandText = sqlStmt;
            reader2 = command.ExecuteReader();
            while (reader2.Read())
            {
               string spaceID = reader2.GetString(0);
               string boundID = reader2.GetString(1);
               string boundType = reader2.GetString(2);
               if (boundType.Length > 24)
                  boundType = boundType.Substring(0, 24);         // Oracle nodetype only accept 24 chars long!
               int spaceNodeID = 0;
               int boundNodeID = 0;

               if (!nodeProcessed.ContainsKey(spaceID))
               {
                  int parentNodeId = parentNodeIdDict[spaceID];
                  spaceNodeID = nodeID++;
                  nodeIdList.Add(spaceNodeID);
                  nodeNameList.Add(spaceID);
                  nodeTypeList.Add("IFCSPACE");
                  activeList.Add("Y");
                  hierarchyLevelList.Add(2);
                  parentIdList.Add(parentNodeId);
                  nodeProcessed.Add(spaceID, spaceNodeID);
               }
               else
               {
                  spaceNodeID = nodeProcessed[spaceID];
               }

               if (string.Compare(boundType, "IFCOPENINGELEMENT") == 0 || string.Compare(boundType, "IFCOPENINGELEMENTSTANDARDCASE") == 0
                     || string.Compare(boundType, "OST_SWALLRECTOPENING", ignoreCase:true) == 0)
               {
                  if (!dependencyDict.ContainsKey(boundID))
                  {
                     // If opening element, need to check whether there is any infill, if yes, use the Door as the node
                     command2.CommandText = "SELECT DEPENDENTELEMENTID,DEPENDENTELEMENTTYPE FROM " + DBOperation.formatTabName("BIMRL_ELEMENTDEPENDENCY", FedID) + " WHERE ELEMENTID='" + boundID
                                             + "' AND upper(DEPENDENTELEMENTTYPE) IN ('IFCDOOR','OST_DOORS')";
#if ORACLE
                     OracleDataReader depReader = command2.ExecuteReader();
                     if (depReader.HasRows)
                     {
                        depReader.Read();
                        // If the query returns value, replace the boundary information with the infill
                        string origBoundID = boundID;

                        boundID = depReader.GetString(0);
                        boundType = depReader.GetString(1);
                        if (boundType.Length > 24)
                           boundType = boundType.Substring(0, 24);         // Oracle nodetype only accept 24 chars long!
#endif
#if POSTGRES
                     DataTable dt = DBOperation.ExecuteToDataTableWithTrans2(command2.CommandText);
                     if (dt != null)
                     {
                        string origBoundID = null;
                        if (dt.Rows.Count > 0)
                        {
                           boundID = dt.Rows[0][0].ToString();
                           boundType = dt.Rows[0][1].ToString();
                           origBoundID = boundID;
                        }
#endif

                        dependencyDict.Add(origBoundID, new Tuple<string, string>(boundID, boundType));
                     }
                     else
                     {
                        // when there is no dependency, insert itself into the Dict to avoid the query to be invoked again in future
                        dependencyDict.Add(boundID, new Tuple<string, string>(boundID, boundType));
                     }
#if ORACLE
                     depReader.Dispose();
#endif
                  }
                  else
                  {
                     Tuple<string, string> boundTuple = dependencyDict[boundID];
                     boundID = boundTuple.Item1;
                     boundType = boundTuple.Item2;
                  }
               }

               if (!nodeProcessed.ContainsKey(boundID))
               {
                  // for the node
                  int parentNodeId = 0;
                  if (parentNodeIdDict.ContainsKey(boundID))
                        parentNodeId = parentNodeIdDict[boundID];
                  else
                  {
                        refBimrlCommon.StackPushError("%%Warning: can't find the parent node id for " + boundType + "'" + boundID + "'");
                        continue;     // missing information, skip
                  }
                  boundNodeID = nodeID++;
                  nodeIdList.Add(boundNodeID);
                  nodeNameList.Add(boundID);
                  nodeTypeList.Add(boundType);
                  activeList.Add("Y");
                  hierarchyLevelList.Add(2);
                  parentIdList.Add(parentNodeId);
                  nodeProcessed.Add(boundID, boundNodeID);

                  // for the link
                  string linkName = spaceID + " - " + boundID;
                  string linkType = "IFCSPACE - " + boundType;
                  linkIdList.Add(linkID++);
                  linkNameList.Add(linkName);
                  startNodeList.Add(spaceNodeID);
                  endNodeList.Add(boundNodeID);
                  linkTypeList.Add(linkType);
                  linkActive.Add("Y");
               }
               else
               {
                  boundNodeID = nodeProcessed[boundID];
                  //// if the object already processed before, add link only if it is not an IfcSpace
                  //if (string.Compare(boundType, "IFCSPACE") != 0)
                  //{
                        string linkName = spaceID + " - " + boundID;
                        string linkType = "IFCSPACE - " + boundType;
                        linkIdList.Add(linkID++);
                        linkNameList.Add(linkName);
                        startNodeList.Add(spaceNodeID);
                        endNodeList.Add(boundNodeID);
                        linkTypeList.Add(linkType);
                        linkActive.Add("Y");
                  //}
               }
            }
            reader2.Dispose();

            insertNode(networkName, nodeIdList, nodeNameList, nodeTypeList, activeList, hierarchyLevelList, parentIdList);
            insertLink(networkName, linkIdList, linkNameList, startNodeList, endNodeList, linkTypeList, linkActive);
            resetLists();

            HashSet<Tuple<string, string>> processedVertPair = new HashSet<Tuple<string, string>>();    // To track the unique pair of the elemID - SpaceAbove

            // Connect between stories: collect means to connect via Stairs or Ramp
            linkParentID = new List<int>();
#if ORACLE
            linkParentStatus = new List<OracleParameterStatus>();
#endif
            sqlStmt = "SELECT ELEMENTID, ELEMENTTYPE, CONTAINER, NAME FROM " + DBOperation.formatTabName("BIMRL_ELEMENT", FedID) 
               + " WHERE UPPER(ELEMENTTYPE) IN ('IFCSTAIR','IFCSTAIRFLIGHT','IFCRAMP','IFCRAMPFLIGHT','OST_STAIRS','OST_STAIRSRUNS','OST_RAMPS') order by elementid";
            command.CommandText = sqlStmt;
            reader = command.ExecuteReader();
            while (reader.Read())
            {
               string elemID = reader.GetString(0);
               string elemType = reader.GetString(1);
               string containerId = string.Empty;
               if (!reader.IsDBNull(2))
                  containerId = reader.GetString(2);
               else
               {
                  containerId = containerFromDetail(FedID, elemID);
                  if (string.IsNullOrEmpty(containerId))
                  {
                        containerId = spaceContainer(FedID, elemID);
                        if (string.IsNullOrEmpty(containerId))
                        {
                           // If even after using geometry to get the space container it cannot get any, give it up
                           refBimrlCommon.StackPushError("%%Warning: Can't find container for " + elemType + " '" + elemID + "'");
                           continue;
                        }
                  }
               }
               string elemName = string.Empty;
               if (!reader.IsDBNull(3))
                  elemName = reader.GetString(3);

               string storeyID = storeyContainer(FedID, containerId);
               string elemIDList = "'" + elemID + "'";
               List<string> aggrList = new List<string>() { elemID };

               sqlStmt = "select aggregateelementid,aggregateelementtype from " + DBOperation.formatTabName("bimrl_relaggregation", FedID) + " where masterelementid='" + elemID + "'";
#if ORACLE
               command2.CommandText = sqlStmt;
               reader2 = command2.ExecuteReader();
               while (reader2.Read())
               {
                  string aggrId = reader2.GetString(0);
                  string aggrType = reader2.GetString(1);
                  BIMRLCommon.appendToString("'" + aggrId + "'", ",", ref elemIDList);
                  aggrList.Add(aggrId);
               }
               reader2.Close();
#endif
#if POSTGRES
               // Npgsql does not allow nested command. Must use second connection to perform a query inside an active command
               DataTable dt = DBOperation.ExecuteToDataTableWithTrans2(sqlStmt);
               if (dt != null)
               {
                  foreach (DataRow row in dt.Rows)
                  {
                     string aggrId = row[0].ToString();
                     string aggrType = row[1].ToString();
                     BIMRLCommon.appendToString("'" + aggrId + "'", ",", ref elemIDList);
                     aggrList.Add(aggrId);
                  }
               }
#endif
               // Getting objects (space or the same object) that intersect cells that are at the top of the object
               sqlStmt = "select a.elementid, a.elementtype, a.name, a.container, count(b.cellid) cellCount from " + DBOperation.formatTabName("bimrl_element", FedID) + " a, " + DBOperation.formatTabName("bimrl_spatialindex", FedID)
                           + " b where a.elementid=b.elementid and b.cellid  in (select cellid from " + DBOperation.formatTabName("bimrl_spatialindex)", FedID)
                           + " where elementid in(" + elemIDList + ") and zmaxbound = (select max(zmaxbound) from " + DBOperation.formatTabName("bimrl_spatialindex", FedID)
                           + " where elementid in (" + elemIDList + "))) and upper(elementtype) in ('IFCSPACE','OST_ROOMS','OST_AREAS','OST_MEPSPACES')"
                           + " GROUP BY a.elementid, a.elementtype, a.name, a.container order by cellCount desc";
#if ORACLE
               command2.CommandText = sqlStmt;
               reader2 = command2.ExecuteReader();
               if (reader2.HasRows)
               {
                  while (reader2.Read())
                  {
                     string spaceAbove = reader2.GetString(0);
                     string container = string.Empty;
                     string spaceType = reader2.GetString(1);
                     string spaceName = string.Empty;
                     if (!reader2.IsDBNull(2))
                        spaceName = reader2.GetString(2);
                     if (!reader2.IsDBNull(3))
                        container = reader2.GetString(3);
                     else
                     {
                        refBimrlCommon.StackPushError("%%Warning: Can't find container for space '" + spaceAbove + "'");
                        continue;
                     }

                     int cellCount = reader2.GetInt32(4);
#endif
#if POSTGRES
               dt = DBOperation.ExecuteToDataTableWithTrans2(sqlStmt);
               if (dt != null)
               { 
                  foreach (DataRow row in dt.Rows)
                  {
                     string spaceAbove = row[0].ToString();
                     string container = string.Empty;
                     string spaceType = row[1].ToString();
                     string spaceName = string.Empty;
                     if (row[2] != null && row[2] != DBNull.Value)
                        spaceName = row[2].ToString();
                     if (row[3] != null && row[3] != DBNull.Value)
                        container = row[3].ToString();
                     else
                     {
                        refBimrlCommon.StackPushError("%%Warning: Can't find container for space '" + spaceAbove + "'");
                        continue;
                     }

                     int cellCount = (int) row[4];
#endif
                     string nextStoreyId = storeyContainer(FedID, container);
                     if (string.Compare(storeyID, nextStoreyId) == 0 || string.IsNullOrEmpty(nextStoreyId))
                     {
                        continue;           // The space is at the same storey, skip
                     }

                     // add now a link between the storey
                     int storeyNode = 0;
                     int storeyAbove = 0;
                     if (nodeProcessed.ContainsKey(storeyID))
                        storeyNode = nodeProcessed[storeyID];
                     if (nodeProcessed.ContainsKey(nextStoreyId))
                        storeyAbove = nodeProcessed[nextStoreyId];
                     if (storeyNode == 0 || storeyAbove == 0)
                     {
                        refBimrlCommon.StackPushError("%%Warning: can't find the corresponding storey node ids (current or above) for '" + elemID + "'");
                        continue;     // missing information, skip
                     }

                     string linkName = storeyNode + " - " + storeyAbove;
                     string linkType = "IFCBUILDINGSTOREY - IFCBUILDINGSTOREY";
                     int parentLinkID = linkID++;
                     linkIdList.Add(parentLinkID);
                     linkNameList.Add(linkName);
                     startNodeList.Add(storeyNode);
                     endNodeList.Add(storeyAbove);
                     linkTypeList.Add(linkType);
                     linkActive.Add("Y");
                     linkParentID.Add(0);
#if ORACLE
                     linkParentStatus.Add(OracleParameterStatus.NullInsert);
#endif
                     if (!nodeProcessed.ContainsKey(elemID))
                     {
                        int parentNodeId = 0;
                        // for the node (this object)
                        if (parentNodeIdDict.ContainsKey(elemID))
                           parentNodeId = parentNodeIdDict[elemID];
                        else
                        {
                           // Check whether the information is available through the dependent element
                           foreach (string aggrId in aggrList)
                           {
                              if (parentNodeIdDict.ContainsKey(aggrId))
                              {
                                 parentNodeId = parentNodeIdDict[aggrId];
                                 break;
                              }
                           }

                           // if still cannot find the information, continue with 0
                           if (parentNodeId == 0)
                           {
                              refBimrlCommon.StackPushError("%%Warning: can't find the corresponding parent node id for '" + elemID + "'");
                              //continue;     // missing information, skip
                           }
                        }

                        int boundNodeID = nodeID++;
                        nodeIdList.Add(boundNodeID);
                        nodeNameList.Add(elemID);
                        nodeTypeList.Add(elemType);
                        activeList.Add("Y");
                        hierarchyLevelList.Add(2);
                        parentIdList.Add(parentNodeId);
                        nodeProcessed.Add(elemID, boundNodeID);

                        // for the link between the object and the space that contains it
                        // string spaceID = spaceContainer(FedID, elemID); 
                        string spaceID = spaceContainer(FedID, aggrList);
                        if (!string.IsNullOrEmpty(spaceID))
                        {
                           linkName = spaceID + " - " + elemID;
                           linkType = "IFCSPACE - " + elemType;
                           linkIdList.Add(linkID++);
                           linkNameList.Add(linkName);
                           int spaceNodeID = nodeProcessed[spaceID];
                           startNodeList.Add(spaceNodeID);
                           endNodeList.Add(boundNodeID);
                           linkTypeList.Add(linkType);
                           linkActive.Add("Y");
                           linkParentID.Add(parentLinkID);
#if ORACLE
                           linkParentStatus.Add(OracleParameterStatus.Success);
#endif

                           processedVertPair.Add(new Tuple<string, string>(spaceID, elemID));
                        }
                     }

                     // Here, we only consider the main element (elemID) for pairing and not the details therefore we need to filter them by the processed pair
                     Tuple<string, string> vPair = new Tuple<string, string>(elemID, spaceAbove);
                     if (!processedVertPair.Contains(vPair))
                     {
                        // Element has been inserted to the Node table before, now we just need to link this element to the space above
                        linkName = spaceAbove + " - " + elemID;
                        linkType = "IFCSPACE - " + elemType;
                        linkIdList.Add(linkID++);
                        linkNameList.Add(linkName);
                        int spaceAboveNode = nodeProcessed[spaceAbove];
                        startNodeList.Add(spaceAboveNode);
                        int elemIDNode = nodeProcessed[elemID];
                        endNodeList.Add(elemIDNode);
                        linkTypeList.Add(linkType);
                        linkActive.Add("Y");
                        linkParentID.Add(parentLinkID);
#if ORACLE
                        linkParentStatus.Add(OracleParameterStatus.Success);
#endif
                        processedVertPair.Add(new Tuple<string, string>(elemID, spaceAbove));
                     }
                  }
#if ORACLE
                  reader2.Close();
#endif
               }
               else
               {
                  reader2.Close();
                  // Missing space above, cannot determine the vertical circulation connectivity
                  refBimrlCommon.StackPushError("%%Warning: Elementid '" + elemID + "' does not have a space above that it can connect to!");
                  continue;
               }
            }
            reader.Close();

            projectUnit projUnit = DBOperation.getProjectUnitLength(FedID);
            double maxHeight = 2.5; // default in Meter
            // Model now is always in Meter

            // To set MaxOctreeLevel correctly, do this first
            Point3D llb, urt;
            DBOperation.getWorldBB(FedID, out llb, out urt);

            // Now look for connection through elevator or elevator space
            sqlStmt = "SELECT ELEMENTID, ELEMENTTYPE, CONTAINER, BODY_MAJOR_AXIS_CENTROID FROM " + DBOperation.formatTabName("BIMRL_ELEMENT", FedID) 
                        + " WHERE (UPPER(NAME) LIKE '%ELEVATOR$' OR UPPER(NAME) LIKE '%LIFT%')"
                        + " AND upper(ELEMENTTYPE) IN ('IFCBUILDINGELEMENTPROXY','IFCSPACE','OST_ROOMS','OST_AREAS','OST_MEPSPACES')";
            command.CommandText = sqlStmt;
            reader = command.ExecuteReader();
            while (reader.Read())
            {
               string elemID = reader.GetString(0);
               string elemType = reader.GetString(1);
               string containerId = reader.GetString(2);
#if ORACLE
               SdoGeometry geom = reader.GetValue(3) as SdoGeometry;
               double geomZValue = geom.SdoPoint.ZD.Value;
#endif
#if POSTGRES
               Point3D centroid = reader.GetFieldValue<Point3D>(2);
               double geomZValue = centroid.Z;
#endif
               string storeyID = storeyContainer(FedID, containerId);

               sqlStmt = "SELECT MIN(XMINBOUND), MAX(XMAXBOUND), MIN(YMINBOUND), MAX(YMAXBOUND), MIN(ZMINBOUND), MAX(ZMAXBOUND) FROM " + DBOperation.formatTabName("BIMRL_SPATIALINDEX", FedID)
                           + " WHERE ELEMENTID='" + elemID + "'";
#if ORACLE
               command2.CommandText = sqlStmt;
               reader2 = command2.ExecuteReader();
               reader2.Read();
               int xmin = reader2.GetInt32(0);
               int xmax = reader2.GetInt32(1);
               int ymin = reader2.GetInt32(2);
               int ymax = reader2.GetInt32(3);
               int zmin = reader2.GetInt32(4);
               int zmax = reader2.GetInt32(5);
               reader2.Close();
               Point3D maxHeightLoc = new Point3D(geom.SdoPoint.XD.Value, geom.SdoPoint.YD.Value, (geomZValue + maxHeight));
#endif
#if POSTGRES
               DataTable dt = DBOperation.ExecuteToDataTableWithTrans2(sqlStmt);
               int xmin = (int) dt.Rows[0][0];
               int xmax = (int) dt.Rows[0][1];
               int ymin = (int) dt.Rows[0][2];
               int ymax = (int) dt.Rows[0][3];
               int zmin = (int) dt.Rows[0][4];
               int zmax = (int) dt.Rows[0][5];
               Point3D maxHeightLoc = new Point3D(centroid.X, centroid.Y, (geomZValue + maxHeight));
#endif
               CellID64 enclCellAtMaxHeight = CellID64.cellAtMaxDepth(maxHeightLoc);
               int xmin2, xmax2, ymin2, ymax2, zmin2, zmax2;
               CellID64.getCellIDComponents(enclCellAtMaxHeight, out xmin2, out ymin2, out zmin2, out xmax2, out ymax2, out zmax2);

               HashSet<Tuple<string, string>> processedEPair = new HashSet<Tuple<string, string>>();

               // Need to limit the max height to avoid getting other relevant object on the higher storeys
               // Getting objects (space or the same object) that have cells that are above of the object
               sqlStmt = "select a.elementid, a.elementtype, a.name, a.container, b.zminbound, count(b.cellid) cellCount from " + DBOperation.formatTabName("bimrl_element", FedID) + " a, " + DBOperation.formatTabName("bimrl_spatialindex", FedID)
                           + " b where a.elementid=b.elementid and b.cellid  in (select cellid from " + DBOperation.formatTabName("bimrl_spatialindex", FedID)
                           + " where (zminbound >= " + zmax.ToString() + " and zminbound < " + zmax2.ToString() + ") "
                           + " and xminbound between " + xmin.ToString() + " and " + xmax.ToString() 
                           + " and yminbound between " + ymin.ToString() + " and " + ymax.ToString() + ") and (upper(a.elementtype) in ('IFCSPACE','IFCTRANSPORTELEMENT','OST_ROOMS','OST_AREAS','OST_MEPSPACES','OST_ELEVS')"
                           //+ " or (a.elementtype='IFCBUILDINGELEMENTPROXY' and (upper(a.name) like '%LIFT%' OR upper(a.name) like '%ELEVATOR%')))"
                           + " or ((upper(a.name) like '%LIFT%' OR upper(a.name) like '%ELEVATOR%')))"
                           + " GROUP BY a.elementid, a.elementtype, a.name, a.container, b.zminbound order by b.zminbound asc, cellCount desc";
#if ORACLE
               command2.CommandText = sqlStmt;
               reader2 = command2.ExecuteReader();
               if (reader2.HasRows)
               {
                  while (reader2.Read())
                  {
                     string objectAbove = reader2.GetString(0);
                     string objectType = reader2.GetString(1);
                     string container = string.Empty;
                     if (!reader2.IsDBNull(3))
                     {
                        container = reader2.GetString(3);
                     }
                     else
                     {
                        container = containerFromDetail(FedID, objectAbove);
                        if (string.IsNullOrEmpty(container))
                           container = containerFromHost(FedID, objectAbove);
                        if (string.IsNullOrEmpty(container))
                        {
                           refBimrlCommon.StackPushError("%%Warning: can't find the container of '" + objectAbove + "' even from its host or details");
                           continue;     // missing information, skip
                        }
                     }

                     double zminbound = reader2.GetDouble(4);
                     int cellCount = reader2.GetInt32(5);
#endif
#if POSTGRES
               dt = DBOperation.ExecuteToDataTableWithTrans2(sqlStmt);
               if (dt != null)
               { 
                  foreach (DataRow row in dt.Rows)
                  {
                     string objectAbove = row[0].ToString();
                     string objectType = row[1].ToString();
                     string container = string.Empty;
                     if (row[3] != null && row[3] != DBNull.Value)
                     {
                        container = row[3].ToString();
                     }
                     else
                     {
                        container = containerFromDetail(FedID, objectAbove);
                        if (string.IsNullOrEmpty(container))
                           container = containerFromHost(FedID, objectAbove);
                        if (string.IsNullOrEmpty(container))
                        {
                           refBimrlCommon.StackPushError("%%Warning: can't find the container of '" + objectAbove + "' even from its host or details");
                           continue;     // missing information, skip
                        }
                     }

                     double zminbound = (double) row[4];
                     int cellCount = (int) row[5];
#endif
                     string nextStoreyId = storeyContainer(FedID, container);
                     if (string.Compare(storeyID, nextStoreyId) == 0)
                        continue;           // The space is at the same storey, skip

                     // add now a link between the storey
                     int storeyNode = 0;
                     int storeyAbove = 0;
                     if (nodeProcessed.ContainsKey(storeyID))
                        storeyNode = nodeProcessed[storeyID];
                     if (nodeProcessed.ContainsKey(nextStoreyId))
                        storeyAbove = nodeProcessed[nextStoreyId];
                     if (storeyNode == 0 || storeyAbove == 0)
                     {
                        refBimrlCommon.StackPushError("%%Warning: can't find the corresponding storey node ids (current or above) for '" + elemID + "'");
                        continue;     // missing information, skip
                     }

                     string linkName = storeyNode + " - " + storeyAbove;
                     string linkType = "IFCBUILDINGSTOREY - IFCBUILDINGSTOREY";
                     int parentLinkID = linkID++;
                     linkIdList.Add(parentLinkID);
                     linkNameList.Add(linkName);
                     startNodeList.Add(storeyNode);
                     endNodeList.Add(storeyAbove);
                     linkTypeList.Add(linkType);
                     linkActive.Add("Y");
                     linkParentID.Add(0);
#if ORACLE
                     linkParentStatus.Add(OracleParameterStatus.NullInsert);
#endif
                     if (!nodeProcessed.ContainsKey(elemID))
                     {
                        // for the node (this object)
                        int parentNodeId = 0;
                        if (parentNodeIdDict.ContainsKey(elemID))
                           parentNodeId = parentNodeIdDict[elemID];

                        int boundNodeID = nodeID++;
                        nodeIdList.Add(boundNodeID);
                        nodeNameList.Add(elemID);
                        nodeTypeList.Add(elemType);
                        activeList.Add("Y");
                        hierarchyLevelList.Add(2);
                        parentIdList.Add(parentNodeId);
                        nodeProcessed.Add(elemID, boundNodeID);

                        // for the link between the object and the space that contains it
                        string spaceID = spaceContainer(FedID, elemID);
                        if (!string.IsNullOrEmpty(spaceID))
                        {
                           linkName = spaceID + " - " + elemID;
                           linkType = "IFCSPACE - " + elemType;
                           linkIdList.Add(linkID++);
                           linkNameList.Add(linkName);
                           int spaceNodeID = nodeProcessed[spaceID];
                           startNodeList.Add(spaceNodeID);
                           endNodeList.Add(boundNodeID);
                           linkTypeList.Add(linkType);
                           linkActive.Add("Y");
                           linkParentID.Add(parentLinkID);
#if ORACLE
                           linkParentStatus.Add(OracleParameterStatus.Success);
#endif
                           processedVertPair.Add(new Tuple<string, string>(spaceID, elemID));
                        }

                     }
                     // for the object above
                     if (!nodeProcessed.ContainsKey(objectAbove))
                     {
                        // for the node (object above)
                        int parentNodeId = 0;
                        if (parentNodeIdDict.ContainsKey(objectAbove))
                           parentNodeId = parentNodeIdDict[objectAbove];

                        int boundNodeID = nodeID++;
                        nodeIdList.Add(boundNodeID);
                        nodeNameList.Add(objectAbove);
                        nodeTypeList.Add(objectType);
                        activeList.Add("Y");
                        hierarchyLevelList.Add(2);
                        parentIdList.Add(storeyAbove);
                        nodeProcessed.Add(objectAbove, boundNodeID);
                     }

                     Tuple<string,string> ePair = new Tuple<string,string>(objectAbove, elemID);
                     if (!processedVertPair.Contains(ePair))
                     {
                        // for the link between the object and the object above
                        linkName = objectAbove + " - " + elemID;
                        linkType = objectType + " - " + elemType;
                        linkIdList.Add(linkID++);
                        linkNameList.Add(linkName);
                        int elemNode = nodeProcessed[elemID];
                        int boundNodeID = nodeProcessed[objectAbove];
                        startNodeList.Add(boundNodeID);
                        endNodeList.Add(elemNode);
                        linkTypeList.Add(linkType);
                        linkActive.Add("Y");
                        linkParentID.Add(parentLinkID);
#if ORACLE
                        linkParentStatus.Add(OracleParameterStatus.Success);
#endif
                        processedVertPair.Add(ePair);
                     }
                  }
               }
               reader2.Close();
            }
            reader.Close();
            insertNode(networkName, nodeIdList, nodeNameList, nodeTypeList, activeList, hierarchyLevelList, parentIdList);
#if ORACLE
            insertLink(networkName, linkIdList, linkNameList, startNodeList, endNodeList, linkTypeList, linkActive, linkParentID, linkParentStatus);
         }
         catch (OracleException e)
#endif
#if POSTGRES
            insertLink(networkName, linkIdList, linkNameList, startNodeList, endNodeList, linkTypeList, linkActive, linkParentID);
         }
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
            refBimrlCommon.StackPushError(excStr);
         }
         catch (SystemException e)
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
            refBimrlCommon.StackPushError(excStr);
            throw;
         }

         command.Dispose();
         command2.Dispose();

         return status;
      }

      public bool createSpaceAdjacencyGraph(int FedID)
      {
         bool status = true;
         string sqlStmt = "";
#if ORACLE
         OracleCommand command = new OracleCommand("", DBOperation.DBConn);

         try
         {

         }
         catch (OracleException e)
#endif
#if POSTGRES
         NpgsqlCommand command = new NpgsqlCommand("", DBOperation.DBConn);
         try
         {

         }
         catch (NpgsqlException e)
#endif
         {
               string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
               refBimrlCommon.StackPushError(excStr);
         }
         catch (SystemException e)
         {
               string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
               refBimrlCommon.StackPushError(excStr);
               throw;
         }
         return status;
      }

      public bool dropNetwork(string networkName)
      {
         bool status = true;
#if ORACLE
         string sqlStmt = "";
         OracleCommand commandPlSql = new OracleCommand("", DBOperation.DBConn);
         try
         {
               // Create network tables
               sqlStmt = "SDO_NET.DROP_NETWORK";
               commandPlSql.CommandText = sqlStmt;
               commandPlSql.CommandType = CommandType.StoredProcedure;
               commandPlSql.BindByName = true;
               commandPlSql.Parameters.Add("network", OracleDbType.Varchar2, networkName, ParameterDirection.Input);
               commandPlSql.ExecuteNonQuery();
               DBOperation.commitTransaction();
               commandPlSql.Dispose();
         }
         catch (OracleException e)
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
            refBimrlCommon.StackPushError(excStr);
            commandPlSql.Dispose();
         }
         catch (SystemException e)
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
            refBimrlCommon.StackPushError(excStr);
            throw;
         }
#endif
#if POSTGRES
         //var location = new Uri(Assembly.GetEntryAssembly().GetName().CodeBase);
         //string exePath = new FileInfo(location.AbsolutePath).Directory.FullName.Replace("%20", " ");
         //// Create tables for storing the graph
         //int ret = DBOperation.executeScript(Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_graphtab_dr.sql"), DBOperation.currFedModel.FederatedID);
         DBOperation.ExecuteSystemScript("BIMRL_graphtab_dr.sql");
#endif

         return status;
      }

      string containerFromDetail(int FedID, string elemID)
      {
         string container = null;
#if ORACLE
         string sqlStmt = "select container from " + DBOperation.formatTabName("bimrl_element", FedID)
                     + " where elementid in (select aggregateelementid  from " + DBOperation.formatTabName("bimrl_relaggregation", FedID)
                     + " where masterelementid='" + elemID + "' union all select dependentelementid from " + DBOperation.formatTabName("bimrl_elementdependency", FedID)
                     + " where elementid='" + elemID + "')";
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
         try
         {
               OracleDataReader reader = command.ExecuteReader();
               while (reader.Read())
               {
                  if (reader.IsDBNull(0))
                     continue;
                  container = reader.GetString(0);
               }
               reader.Dispose();
         }
         catch (OracleException e)
         {
               string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
               refBimrlCommon.StackPushError(excStr);
               command.Dispose();
         }
         catch (SystemException e)
         {
               string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
               refBimrlCommon.StackPushError(excStr);
               throw;
         }

         command.Dispose();
#endif
#if POSTGRES
         string sqlStmt = "select container from " + DBOperation.formatTabName("bimrl_element", FedID)
                     + " where elementid in (select aggregateelementid  from " + DBOperation.formatTabName("bimrl_relaggregation", FedID)
                     + " where masterelementid='" + elemID + "' union all select dependentelementid from " + DBOperation.formatTabName("bimrl_elementdependency", FedID)
                     + " where elementid='" + elemID + "')";
         object contObj = DBOperation.ExecuteScalarWithTrans2(sqlStmt);
         if (contObj != null)
            container = contObj.ToString();
#endif
         return container;
      }

      string containerFromHost(int FedID, string elemID)
      {
         string container = null;
#if ORACLE
         string sqlStmt = "select container from " + DBOperation.formatTabName("bimrl_element", FedID)
                     + " where elementid in (select masterelementid  from " + DBOperation.formatTabName("bimrl_relaggregation", FedID)
                     + " where aggregateelementid='" + elemID + "' union all select elementid from " + DBOperation.formatTabName("bimrl_elementdependency", FedID)
                     + " where dependentelementid='" + elemID + "')";
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
         try
         {
               OracleDataReader reader = command.ExecuteReader();
               while (reader.Read())
               {
                  if (reader.IsDBNull(0))
                     continue;
                  container = reader.GetString(0);
               }
               reader.Dispose();
         }
         catch (OracleException e)
         {
               string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
               refBimrlCommon.StackPushError(excStr);
               command.Dispose();
         }
         catch (SystemException e)
         {
               string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
               refBimrlCommon.StackPushError(excStr);
               throw;
         }

         command.Dispose();
#endif
#if POSTGRES
         string sqlStmt = "select container from " + DBOperation.formatTabName("bimrl_element", FedID)
            + " where elementid in (select masterelementid  from " + DBOperation.formatTabName("bimrl_relaggregation", FedID)
            + " where aggregateelementid='" + elemID + "' union all select elementid from " + DBOperation.formatTabName("bimrl_elementdependency", FedID)
            + " where dependentelementid='" + elemID + "')";
         object contObj = DBOperation.ExecuteScalarWithTrans2(sqlStmt);
         if (contObj != null)
            container = contObj.ToString();
#endif
         return container;
      }


      string storeyContainer(int FedID, string containerID)
      {
         string storeyID = null;
#if ORACLE
         OracleCommand command = new OracleCommand("", DBOperation.DBConn);
         string sqlStmt = "select parentid from " + DBOperation.formatTabName("bimrl_spatialstructure", FedID) + " where spatialelementid='" + containerID + "' "
                           + " and parenttype='IFCBUILDINGSTOREY' union select spatialelementid from " + DBOperation.formatTabName("bimrl_spatialstructure", FedID)
                           + " where spatialelementid='" + containerID + "' and spatialelementtype='IFCBUILDINGSTOREY'";
         command.CommandText = sqlStmt;
         try
         {
               object retC = command.ExecuteScalar();
               if (retC != null)
                  storeyID = retC.ToString();
         }
         catch (OracleException e)
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
               refBimrlCommon.StackPushError(excStr);
               command.Dispose();
         }
         catch (SystemException e)
         {
               string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
               refBimrlCommon.StackPushError(excStr);
               throw;
         }
         command.Dispose();
#endif
#if POSTGRES
         string sqlStmt = "select parentid from " + DBOperation.formatTabName("bimrl_spatialstructure", FedID) + " where spatialelementid='" + containerID + "' "
                           + " and upper(parenttype) in ('IFCBUILDINGSTOREY','OST_LEVELS') union select spatialelementid from " + DBOperation.formatTabName("bimrl_spatialstructure", FedID)
                           + " where spatialelementid='" + containerID + "' and upper(spatialelementtype) in ('IFCBUILDINGSTOREY','OST_LEVELS')";
         object retC = DBOperation.ExecuteScalarWithTrans2(sqlStmt);
         if (retC != null)
            storeyID = retC.ToString();
#endif
         return storeyID;
      }

      string spaceContainer(int FedID, string elementID)
      {
         List<string> elemIDList = new List<string>();
         elemIDList.Add(elementID);
         return spaceContainer(FedID, elemIDList);
      }

      string spaceContainer(int FedID, List<string> elementIDList)
      {
         string elemIDLis = "";
         foreach (string elem in elementIDList)
               BIMRLCommon.appendToString("'" + elem + "'", ",", ref elemIDLis);

         string container = null;
#if ORACLE
         string sqlStmt = "select a.elementid, count(b.cellid) cellCount from " + DBOperation.formatTabName("bimrl_element", FedID) + " a, " + DBOperation.formatTabName("bimrl_spatialindex", FedID) + " b, "
                     + DBOperation.formatTabName("bimrl_element", FedID) + " c, " + DBOperation.formatTabName("bimrl_spatialindex", FedID) + " d "
                     + "where a.elementid=b.elementid and c.elementid=d.elementid and b.cellid=d.cellid and c.elementid in (" + elemIDLis + ") and a.elementtype='IFCSPACE' "
                     + "group by a.elementid order by cellCount desc";
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
         OracleDataReader reader;
         try
         {
            reader = command.ExecuteReader();
            if (reader.HasRows)
            {
               // We will get the first one, which has the most cells in common
               reader.Read();
               container = reader.GetString(0);
            }
            reader.Dispose();
         }
         catch (OracleException e)
#endif
#if POSTGRES
         string sqlStmt = "select a.elementid, count(b.cellid) cellCount from " + DBOperation.formatTabName("bimrl_element", FedID) + " a, " + DBOperation.formatTabName("bimrl_spatialindex", FedID) + " b, "
            + DBOperation.formatTabName("bimrl_element", FedID) + " c, " + DBOperation.formatTabName("bimrl_spatialindex", FedID) + " d "
            + "where a.elementid=b.elementid and c.elementid=d.elementid and b.cellid=d.cellid and c.elementid in (" + elemIDLis + ") "
            + "and upper(a.elementtype) in ('IFCSPACE','OST_ROOMS','OST_AREAS','OSTMEPSPACES') "
            + "group by a.elementid order by cellCount desc";
         NpgsqlConnection arbConn = DBOperation.arbitraryConnection();
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, arbConn);
         NpgsqlTransaction arbTrans = arbConn.BeginTransaction();
         NpgsqlDataReader reader;
         try
         {
            reader = command.ExecuteReader();
            if (reader.HasRows)
            {
               // We will get the first one, which has the most cells in common
               reader.Read();
               container = reader.GetString(0);
            }
            reader.Dispose();
         }
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
            refBimrlCommon.StackPushError(excStr);
            command.Dispose();
         }
         catch (SystemException e)
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
            refBimrlCommon.StackPushError(excStr);
#if POSTGRES
            arbConn.Close();
#endif
            throw;
         }
         command.Dispose();
#if POSTGRES
         arbTrans.Rollback();
         arbConn.Close();
#endif
         return container;
      }


#if ORACLE
      void insertNode(string networkName, List<int> nodeIdList, List<string> nodeNameList, List<string> nodeTypeList, List<string> activeList, List<int> hierarchyLevelList, List<int> parentIdList = null, List<OracleParameterStatus> parentIdListStatus = null)
      {
         string sqlStmt = "Insert into " + networkName + "_NODE$ (NODE_ID, NODE_NAME, NODE_TYPE, ACTIVE, HIERARCHY_LEVEL, PARENT_NODE_ID) "
                                 + "VALUES (:1, :2, :3, :4, :5, :6)";
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
         OracleParameter[] pars = new OracleParameter[6];
         pars[0] = command.Parameters.Add("1", OracleDbType.Int32);
         pars[1] = command.Parameters.Add("2", OracleDbType.Varchar2);
         pars[2] = command.Parameters.Add("3", OracleDbType.Varchar2);
         pars[3] = command.Parameters.Add("4", OracleDbType.Varchar2);
         pars[4] = command.Parameters.Add("5", OracleDbType.Int32);
         pars[5] = command.Parameters.Add("6", OracleDbType.Int32);
         for (int i = 0; i < 6; i++)
         {
            pars[i].Direction = ParameterDirection.Input;
         }
         if (nodeIdList.Count > 0)
         {
            pars[0].Value = nodeIdList.ToArray();
            pars[1].Value = nodeNameList.ToArray();
            pars[2].Value = nodeTypeList.ToArray();
            pars[3].Value = activeList.ToArray();
            pars[4].Value = hierarchyLevelList.ToArray();
            if (parentIdList == null)
            {
               parentIdList = new List<int>();
               parentIdListStatus = new List<OracleParameterStatus>();
               for (int i=0; i< nodeIdList.Count; ++i)
               {
                  parentIdList.Add(0);
                  parentIdListStatus.Add(OracleParameterStatus.NullInsert);
               }
               pars[5].Value = parentIdList.ToArray();
               pars[5].ArrayBindStatus = parentIdListStatus.ToArray();
            }
            else
            {
               pars[5].Value = parentIdList.ToArray();
            }
            command.ArrayBindCount = nodeIdList.Count;

            try
            {
               command.ExecuteNonQuery();
               DBOperation.commitTransaction();
            }
            catch (OracleException e)
            {
               string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
               refBimrlCommon.StackPushError(excStr);
               command.Dispose();
            }
            catch (SystemException e)
            {
               string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
               refBimrlCommon.StackPushError(excStr);
               throw;
            }
         }

         command.Dispose();
#endif
#if POSTGRES
      void insertNode(string networkName, List<int> nodeIdList, List<string> nodeNameList, List<string> nodeTypeList, List<string> activeList, List<int> hierarchyLevelList, List<int> parentIdList = null)
      {
         string sqlStmt = "Insert into " + networkName + "_NODE$ (NODE_ID, NODE_NAME, NODE_TYPE, ACTIVE, HIERARCHY_LEVEL, PARENT_NODE_ID) "
                     + "VALUES (@nid, @nname, @ntyp, @act, @hier, @par)";

         NpgsqlConnection arbConn = DBOperation.arbitraryConnection();
         NpgsqlCommand commandIns = new NpgsqlCommand(sqlStmt, arbConn);
         NpgsqlTransaction arbTrans = arbConn.BeginTransaction();
         commandIns.Parameters.Add("@nid", NpgsqlDbType.Integer);
         commandIns.Parameters.Add("@nname", NpgsqlDbType.Text);
         commandIns.Parameters.Add("@ntyp", NpgsqlDbType.Text);
         commandIns.Parameters.Add("@act", NpgsqlDbType.Text);
         commandIns.Parameters.Add("@hier", NpgsqlDbType.Integer);
         commandIns.Parameters.Add("@par", NpgsqlDbType.Integer);
         commandIns.Prepare();

         for (int i = 0; i < nodeIdList.Count; ++i)
         {
            try
            {
               commandIns.Parameters["@nid"].Value = nodeIdList[i];
               commandIns.Parameters["@nname"].Value = nodeNameList[i];
               commandIns.Parameters["@ntyp"].Value = nodeTypeList[i];
               commandIns.Parameters["@act"].Value = activeList[i];
               commandIns.Parameters["@hier"].Value = hierarchyLevelList[i];
               if (parentIdList == null)
                  commandIns.Parameters["@par"].Value = DBNull.Value;
               else
                  commandIns.Parameters["@par"].Value = parentIdList[i];

               arbTrans.Save("insSavePoint");
               int commandStatus = commandIns.ExecuteNonQuery();
               arbTrans.Release("insSavePoint");
            }
            catch (NpgsqlException e)
            {
               string excStr = "%%Insert Spatial Index Error - " + e.Message + "\n\t";
               refBimrlCommon.StackPushIgnorableError(excStr);
               arbTrans.Rollback("insSavePoint");
            }
            catch (SystemException e)
            {
               string excStr = "%%Insert Spatial Index Error - " + e.Message + "\n\t";
               refBimrlCommon.StackPushError(excStr);
               throw;
            }
         }
         arbTrans.Commit();
         commandIns.Dispose();
         arbConn.Close();
#endif
      }

#if ORACLE
      void insertLink(string networkName, List<int> linkIdList, List<string> linkNameList, List<int> startNodeList, List<int> endNodeList, List<string> linkTypeList,
                     List<string> linkActive, List<int> parentLinkID = null, List<OracleParameterStatus> parentLinkStatus = null)
      {
         string sqlStmt = "Insert into " + networkName + "_LINK$ (LINK_ID, LINK_NAME, START_NODE_ID, END_NODE_ID, LINK_TYPE, ACTIVE, PARENT_LINK_ID) VALUES (:1, :2, :3, :4, :5, :6, :7)";
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
         OracleParameter[] parsLink = new OracleParameter[7];
         command.Parameters.Clear();
         parsLink[0] = command.Parameters.Add("1", OracleDbType.Int32);
         parsLink[1] = command.Parameters.Add("2", OracleDbType.Varchar2);
         parsLink[2] = command.Parameters.Add("3", OracleDbType.Int32);
         parsLink[3] = command.Parameters.Add("4", OracleDbType.Int32);
         parsLink[4] = command.Parameters.Add("5", OracleDbType.Varchar2);
         parsLink[5] = command.Parameters.Add("6", OracleDbType.Varchar2);
         parsLink[6] = command.Parameters.Add("7", OracleDbType.Int32);
         for (int i = 0; i < 7; i++)
         {
            parsLink[i].Direction = ParameterDirection.Input;
         }
         if (linkIdList.Count > 0)
         {
            parsLink[0].Value = linkIdList.ToArray();
            parsLink[1].Value = linkNameList.ToArray();
            parsLink[2].Value = startNodeList.ToArray();
            parsLink[3].Value = endNodeList.ToArray();
            parsLink[4].Value = linkTypeList.ToArray();
            parsLink[5].Value = linkActive.ToArray();
            if (parentLinkID == null)
            {
               parentLinkID = new List<int>();
               parentLinkStatus = new List<OracleParameterStatus>();
               for (int i=0; i<linkIdList.Count; ++i)
               {
                  parentLinkID.Add(0);
                  parentLinkStatus.Add(OracleParameterStatus.NullInsert);
               }
               parsLink[6].Value = parentLinkID.ToArray();
               parsLink[6].ArrayBindStatus = parentLinkStatus.ToArray();
            }
            else
            {
               parsLink[6].Value = parentLinkID.ToArray();
               parsLink[6].ArrayBindStatus = parentLinkStatus.ToArray();
            }

            command.ArrayBindCount = linkIdList.Count;

            try
            {
               command.ExecuteNonQuery();
               DBOperation.commitTransaction();
            }
            catch (OracleException e)
            {
               string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
               refBimrlCommon.StackPushError(excStr);
               command.Dispose();
            }
            catch (SystemException e)
            {
               string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
               refBimrlCommon.StackPushError(excStr);
               throw;
            }
         }
         command.Dispose();
#endif
#if POSTGRES
      void insertLink(string networkName, List<int> linkIdList, List<string> linkNameList, List<int> startNodeList, List<int> endNodeList, List<string> linkTypeList,
               List<string> linkActive, List<int> parentLinkID = null)
      {
         string sqlStmt = "Insert into " + networkName + "_LINK$ (LINK_ID, LINK_NAME, START_NODE_ID, END_NODE_ID, LINK_TYPE, ACTIVE, PARENT_LINK_ID) "
                           + "VALUES (@lid, @lname, @stid, @endid, @ltyp, @act, @par)";
         NpgsqlConnection arbConn = DBOperation.arbitraryConnection();
         NpgsqlCommand commandIns = new NpgsqlCommand(sqlStmt, arbConn);
         NpgsqlTransaction arbTrans = arbConn.BeginTransaction();
         commandIns.Parameters.Add("@lid", NpgsqlDbType.Integer);
         commandIns.Parameters.Add("@lname", NpgsqlDbType.Text);
         commandIns.Parameters.Add("@stid", NpgsqlDbType.Integer);
         commandIns.Parameters.Add("@endid", NpgsqlDbType.Integer);
         commandIns.Parameters.Add("@ltyp", NpgsqlDbType.Text);
         commandIns.Parameters.Add("@act", NpgsqlDbType.Text);
         commandIns.Parameters.Add("@par", NpgsqlDbType.Integer);
         commandIns.Prepare();

         if (linkIdList.Count > 0)
         {
            for (int i = 1; i < linkIdList.Count; ++i)
            {
               commandIns.Parameters["@lid"].Value = linkIdList[i];
               commandIns.Parameters["@lname"].Value = linkNameList[i];
               commandIns.Parameters["@stid"].Value = startNodeList[i];
               commandIns.Parameters["@endid"].Value = endNodeList[i];
               commandIns.Parameters["@ltyp"].Value = linkTypeList[i];

               if (parentLinkID == null)
               {
                  parentLinkID = new List<int>();
                  commandIns.Parameters["@par"].Value = DBNull.Value;
               }
               else
               {
                  commandIns.Parameters["@par"].Value = parentLinkID[i];
               }

               try
               {
                  arbTrans.Save("insSavePoint");
                  commandIns.ExecuteNonQuery();
                  arbTrans.Release("insSavePoint");
               }
               catch (NpgsqlException e)
               {
                  string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
                  refBimrlCommon.StackPushError(excStr);
                  arbTrans.Rollback("insSavePoint");
               }
               catch (SystemException e)
               {
                  string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
                  refBimrlCommon.StackPushError(excStr);
                  arbTrans.Rollback();
                  throw;
               }
            }
         }
         arbTrans.Commit();
         commandIns.Dispose();
         arbConn.Close();
#endif
      }
   }
}
