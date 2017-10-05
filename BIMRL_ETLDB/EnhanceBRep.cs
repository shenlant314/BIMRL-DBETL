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
using System.Threading.Tasks;
using System.Data;
#if ORACLE
using Oracle.DataAccess.Types;
using Oracle.DataAccess.Client;
using NetSdoGeometry;
#endif
#if POSTGRES
using Npgsql;
using NpgsqlTypes;
#endif
using Xbim.Common.Geometry;
using BIMRL.Common;
using Newtonsoft.Json;

namespace BIMRL
{
   public class EnhanceBRep
   {
      struct IDnType
      {
         public string elementId;
         public string elementType;
      }

      struct TopoFace
      {
         public string ID;
         public string type;
         public Vector3D normal;
         public Vector3D reversedNormal;
         public double angleFromNorth;
         public Point3D centroid;
#if ORACLE
         public SdoGeometry polygon;
         public SdoGeometry sdoNormal;
         public SdoGeometry sdoCentroid;
#endif
#if POSTGRES
         public string polygon;
#endif
      }

      BIMRLCommon _refBIMRLCommon = new BIMRLCommon();

      public EnhanceBRep()
      {
         // If the dictionary is empty, initialize with default settings
         if (DBOperation.objectForSpaceBoundary.Count == 0)
         {
            DBOperation.objectForSpaceBoundary.Add("IFCBEAM", false);
            DBOperation.objectForSpaceBoundary.Add("IFCCOLUMN", false);
            DBOperation.objectForSpaceBoundary.Add("IFCCOVERING", true);
            DBOperation.objectForSpaceBoundary.Add("IFCCURTAINWALL", true);
            DBOperation.objectForSpaceBoundary.Add("IFCDOOR", true);
            DBOperation.objectForSpaceBoundary.Add("IFCMEMBER", false);
            DBOperation.objectForSpaceBoundary.Add("IFCOPENINGELEMENT", true);
            DBOperation.objectForSpaceBoundary.Add("IFCPLATE", false);
            DBOperation.objectForSpaceBoundary.Add("IFCRAILING", false);
            DBOperation.objectForSpaceBoundary.Add("IFCRAMP", false);
            DBOperation.objectForSpaceBoundary.Add("IFCROOF", true);
            DBOperation.objectForSpaceBoundary.Add("IFCSLAB", true);
            DBOperation.objectForSpaceBoundary.Add("IFCSPACE", true);
            DBOperation.objectForSpaceBoundary.Add("IFCSTAIR", false);
            DBOperation.objectForSpaceBoundary.Add("IFCWALL", true);
            DBOperation.objectForSpaceBoundary.Add("IFCWALLSTANDARDCASE", true);
            DBOperation.objectForSpaceBoundary.Add("IFCWINDOW", true);
#if POSTGRES
            // Add equivalent objects from Revit model to be processed the same way
            DBOperation.objectForSpaceBoundary.Add("OST_Windows", DBOperation.objectForSpaceBoundary["IFCWINDOW"]);
            DBOperation.objectForSpaceBoundary.Add("OST_CurtainWallPanels", DBOperation.objectForSpaceBoundary["IFCCURTAINWALL"]);
            DBOperation.objectForSpaceBoundary.Add("OST_Doors", DBOperation.objectForSpaceBoundary["IFCDOOR"]);
            DBOperation.objectForSpaceBoundary.Add("OST_CurtainGridsRoof", DBOperation.objectForSpaceBoundary["IFCROOF"]);
            DBOperation.objectForSpaceBoundary.Add("OST_Roofs", DBOperation.objectForSpaceBoundary["IFCROOF"]);
            DBOperation.objectForSpaceBoundary.Add("OST_Walls", DBOperation.objectForSpaceBoundary["IFCWALL"]);
            DBOperation.objectForSpaceBoundary.Add("OST_Floors", DBOperation.objectForSpaceBoundary["IFCSLAB"]);
            DBOperation.objectForSpaceBoundary.Add("OST_StairsLandings", DBOperation.objectForSpaceBoundary["IFCSLAB"]);
            DBOperation.objectForSpaceBoundary.Add("OST_Ramps", DBOperation.objectForSpaceBoundary["IFCRAMP"]);
            DBOperation.objectForSpaceBoundary.Add("OST_Stairs", DBOperation.objectForSpaceBoundary["IFCSTAIR"]);
            DBOperation.objectForSpaceBoundary.Add("OST_StairsRuns", DBOperation.objectForSpaceBoundary["IFCSTAIR"]);
            DBOperation.objectForSpaceBoundary.Add("OST_Areas", DBOperation.objectForSpaceBoundary["IFCSPACE"]);
            DBOperation.objectForSpaceBoundary.Add("OST_Rooms", DBOperation.objectForSpaceBoundary["IFCSPACE"]);
            DBOperation.objectForSpaceBoundary.Add("OST_MEPSpaces", DBOperation.objectForSpaceBoundary["IFCSPACE"]);
            DBOperation.objectForSpaceBoundary.Add("OST_SWallRectOpening", DBOperation.objectForSpaceBoundary["IFCOPENINGELEMENT"]);
            DBOperation.objectForSpaceBoundary.Add("OST_RoofOpening", DBOperation.objectForSpaceBoundary["IFCOPENINGELEMENT"]);
            DBOperation.objectForSpaceBoundary.Add("OST_Cornices", DBOperation.objectForSpaceBoundary["IFCBEAM"]);
            DBOperation.objectForSpaceBoundary.Add("OST_CurtainWallMullions", DBOperation.objectForSpaceBoundary["IFCMEMBER"]);
            DBOperation.objectForSpaceBoundary.Add("OST_StairsStringerCarriage", DBOperation.objectForSpaceBoundary["IFCMEMBER"]);
            DBOperation.objectForSpaceBoundary.Add("OST_Railings", DBOperation.objectForSpaceBoundary["IFCRAILING"]);
            DBOperation.objectForSpaceBoundary.Add("OST_RailingHandRail", DBOperation.objectForSpaceBoundary["IFCRAILING"]);
            DBOperation.objectForSpaceBoundary.Add("OST_RailingSupport", DBOperation.objectForSpaceBoundary["IFCRAILING"]);
            DBOperation.objectForSpaceBoundary.Add("OST_RailingTopRail", DBOperation.objectForSpaceBoundary["IFCRAILING"]);
            DBOperation.objectForSpaceBoundary.Add("OST_StairsRailing", DBOperation.objectForSpaceBoundary["IFCRAILING"]);
            DBOperation.objectForSpaceBoundary.Add("OST_Ceilings", DBOperation.objectForSpaceBoundary["IFCCOVERING"]);
            DBOperation.objectForSpaceBoundary.Add("OST_StructuralColumns", DBOperation.objectForSpaceBoundary["IFCCOLUMN"]);
            DBOperation.objectForSpaceBoundary.Add("OST_StructuralFraming", DBOperation.objectForSpaceBoundary["IFCCOLUMN"]);
            DBOperation.objectForSpaceBoundary.Add("OST_StructuralFoundation", DBOperation.objectForSpaceBoundary["IFCCOLUMN"]);
#endif
         }
      }

      public void enhanceSpaceBoundary(string addCondition)
      {
         Dictionary<string, List<IDnType>> spaceNBoundary = new Dictionary<string, List<IDnType>>();
         string spaceColl = "('IFCSPACE', 'OST_ROOMS', 'OST_AREAS',' OST_MEPSPACES')";

         List<string> spaceEID = new List<string>();
         List<string> sFaceBoundID = new List<string>();
         List<string> boundaryEID = new List<string>();
         List<string> bFaceBoundID = new List<string>();
         List<string> boundaryType = new List<string>();
         List<string> InternalOrExternal = new List<string>();
         List<double> SFanglefromnorthList = new List<double>();
         List<double> BFanglefromnorthList = new List<double>();
#if ORACLE
         List<SdoGeometry> commonPointAtS = new List<SdoGeometry>();
         List<OracleParameterStatus> cPointAtSPS = new List<OracleParameterStatus>();
         List<SdoGeometry> commonPointAtB = new List<SdoGeometry>();
         List<OracleParameterStatus> cPointAtBPS = new List<OracleParameterStatus>();
         List<SdoGeometry> SFpolygonList = new List<SdoGeometry>();
         List<SdoGeometry> SFnormalList = new List<SdoGeometry>();
         List<OracleParameterStatus> SFanglefromnorthStatus = new List<OracleParameterStatus>();
         List<SdoGeometry> SFcentroidList = new List<SdoGeometry>();
         List<SdoGeometry> BFpolygonList = new List<SdoGeometry>();
         List<SdoGeometry> BFnormalList = new List<SdoGeometry>();
         List<OracleParameterStatus> BFanglefromnorthStatus = new List<OracleParameterStatus>();
         List<SdoGeometry> BFcentroidList = new List<SdoGeometry>();
#endif
#if POSTGRES
         List<Point3D> commonPointAtS = new List<Point3D>();
         List<Point3D> commonPointAtB = new List<Point3D>();
         List<string> SFpolygonList = new List<string>();
         List<Point3D> SFnormalList = new List<Point3D>();
         List<Point3D> SFcentroidList = new List<Point3D>();
         List<string> BFpolygonList = new List<string>();
         List<Point3D> BFnormalList = new List<Point3D>();
         List<Point3D> BFcentroidList = new List<Point3D>();
#endif
         string currStep = "";
         try
         {
            string elemList = "";
            foreach (KeyValuePair<string, bool> elems in DBOperation.objectForSpaceBoundary)
            {
               if (elems.Value)
               {
                  if (!string.IsNullOrEmpty(elemList))
                     elemList += ", ";
                  elemList += "'" + elems.Key.ToUpper() + "'";
               }
            }
            // expand elementtype that has aggregation
            if (!string.IsNullOrEmpty(elemList))
            {
               string sqlStmt = "SELECT DISTINCT AGGREGATEELEMENTTYPE FROM " + DBOperation.formatTabName("BIMRL_RELAGGREGATION") + " WHERE UPPER(MASTERELEMENTTYPE) IN (" + elemList + ")";
               currStep = sqlStmt;
#if ORACLE
               OracleCommand cmdAT = new OracleCommand(sqlStmt, DBOperation.DBConn);
               cmdAT.FetchSize = 200;
               OracleDataReader reader = cmdAT.ExecuteReader();
#endif
#if POSTGRES
               NpgsqlCommand cmdAT = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
               cmdAT.Prepare();
               NpgsqlDataReader reader = cmdAT.ExecuteReader();
#endif
               while (reader.Read())
               {
                  string etype = reader.GetString(0).ToUpper();
                  if (elemList.Contains("'" + etype + "'"))
                        continue;
                  elemList += ",'" + etype + "'";
               }
               reader.Dispose();
               cmdAT.Dispose();
               elemList = "(" + elemList + ")";
            }

            string elemListCond = "";
            if (!string.IsNullOrEmpty(elemList))
               elemListCond = "SELECT ELEMENTID FROM BIMRL_ELEMENT_" +  DBOperation.currSelFedID.ToString("X4") + " WHERE upper(ELEMENTTYPE) IN " + elemList;
                
            string sqlStmt1 = "SELECT ELEMENTID FROM " + DBOperation.formatTabName("BIMRL_ELEMENT") + " WHERE upper(ELEMENTTYPE) in " + spaceColl;
            BIMRLCommon.appendToString(addCondition, " AND ", ref sqlStmt1);

#if ORACLE
            OracleCommand cmdETyp = new OracleCommand(sqlStmt1, DBOperation.DBConn);
            cmdETyp.FetchSize = 500;
            OracleDataReader readerEID = cmdETyp.ExecuteReader();
#endif
#if POSTGRES
            NpgsqlCommand cmdETyp = new NpgsqlCommand(sqlStmt1, DBOperation.DBConn);
            cmdETyp.Prepare();
            NpgsqlDataReader readerEID = cmdETyp.ExecuteReader();
#endif
            currStep = sqlStmt1;

            while (readerEID.Read())
            {
               List<IDnType> boundaries = new List<IDnType>();
               string eid = readerEID.GetString(0);

               string sqlStmt2 = "SELECT DISTINCT A.ELEMENTID FROM " + DBOperation.formatTabName("BIMRL_SPATIALINDEX")
                                    + " A, " + DBOperation.formatTabName("BIMRL_SPATIALINDEX") + " B "
                                    + " WHERE A.CELLID = B.CELLID AND B.ELEMENTID='" + eid + "' INTERSECT " + elemListCond;
#if ORACLE
               OracleCommand cmd = new OracleCommand(sqlStmt2, DBOperation.DBConn);
               cmd.FetchSize = 500;
               OracleDataReader reader2;
               currStep = sqlStmt2;
               reader2 = cmd.ExecuteReader();
               while (reader2.Read())
               {
                  string bid = reader2.GetString(0);
                  string etype = "";
                  IDnType boundaryInfo = new IDnType { elementId = bid, elementType = etype };
                  boundaries.Add(boundaryInfo);
               }
               spaceNBoundary.Add(eid,boundaries);
               reader2.Dispose();
               cmd.Dispose();
#endif
#if POSTGRES
               DataTable dt = DBOperation.ExecuteToDataTableWithTrans2(sqlStmt2);
               if (dt != null)
               {
                  foreach (DataRow row in dt.Rows)
                  {
                     string bid = (string)row[0];
                     string etype = "";
                     IDnType boundaryInfo = new IDnType { elementId = bid, elementType = etype };
                     boundaries.Add(boundaryInfo);
                  }
                  //NpgsqlCommand cmd = new NpgsqlCommand(sqlStmt2, DBOperation.DBConn);
                  //cmd.Prepare();
                  //NpgsqlDataReader reader2;
                  //currStep = sqlStmt2;
                  //reader2 = cmd.ExecuteReader();
                  //while (reader2.Read())
                  //{
                  //   string bid = reader2.GetString(0);
                  //   string etype = "";
                  //   IDnType boundaryInfo = new IDnType { elementId = bid, elementType = etype };
                  //   boundaries.Add(boundaryInfo);
                  //}
                  spaceNBoundary.Add(eid, boundaries);
                  //reader2.Dispose();
                  //cmd.Dispose();
               }
#endif
            }

            readerEID.Dispose();
            cmdETyp.Dispose();

            List<TopoFace> theSpaceF = new List<TopoFace>();
            Dictionary<string, List<TopoFace>> faceColl = new Dictionary<string, List<TopoFace>>();

            // Now we have collected the spaces and their boundaries. We will go through one by one and update the space boundary information linking it with the faces
            // In this process, we will ignore faces from OBB or PROJOBB and only process BODY and HOLE for space boundaries
            foreach (KeyValuePair<string, List<IDnType>> spb in spaceNBoundary)
            {
               string sqlStmt3 = "";
               string eidList = "";
               eidList += "'" + spb.Key + "'";
               if (spb.Value.Count > 900)
               {
                  // Oracle limits the number of expressions in a query to 1000 only. If the list is long, we need to use a different approach using a temporary table

                  DBOperation.executeSingleStmt("DELETE FROM BIMRLQUERYTEMP");
#if ORACLE
                  sqlStmt3 = "INSERT INTO BIMRLQUERYTEMP (ID1) VALUES (:1)";
                  OracleCommand addCmd = new OracleCommand(sqlStmt3, DBOperation.DBConn);
                  OracleParameter par = new OracleParameter();
                  par = addCmd.Parameters.Add("1", OracleDbType.Varchar2);
                  List<string> eids = new List<string>();
                  foreach (IDnType it in spb.Value)
                  {
                     eids.Add(it.elementId);
                  }
                  par.Value = eids.ToArray();
                  par.Size = eids.Count();
                  par.Direction = ParameterDirection.Input;
                  cmdETyp.ArrayBindCount = eids.Count;
                  currStep = sqlStmt3;
                  addCmd.ExecuteNonQuery();
#endif
#if POSTGRES
                  sqlStmt3 = "INSERT INTO BIMRLQUERYTEMP (ID1) VALUES (@id1)";
                  NpgsqlCommand addCmd = new NpgsqlCommand(sqlStmt3, DBOperation.DBConn);
                  currStep = sqlStmt3;
                  foreach (IDnType it in spb.Value)
                  {
                     addCmd.Parameters.Clear();
                     addCmd.Parameters.AddWithValue("@id1", NpgsqlDbType.Varchar, it.elementId);
                     addCmd.Prepare();

                     addCmd.ExecuteNonQuery();
                  }
#endif

                  sqlStmt3 = "SELECT ELEMENTID, ID, TYPE, POLYGON, NORMAL, ANGLEFROMNORTH, CENTROID FROM " + DBOperation.formatTabName("BIMRL_TOPO_FACE")
                              + " WHERE ELEMENTID IN (SELECT ID1 FROM BIMRLQUERYTEMP) AND TYPE NOT IN ('OBB',' PROJOBB') ORDER BY ELEMENTID";
               }
               else
               {
                  foreach (IDnType it in spb.Value)
                  {
                        eidList += ",'" + it.elementId + "'";
                  }
                  eidList = "(" + eidList + ")";
                  sqlStmt3 = "SELECT ELEMENTID, ID, TYPE, POLYGON, NORMAL, ANGLEFROMNORTH, CENTROID FROM " + DBOperation.formatTabName("BIMRL_TOPO_FACE")
                              + " WHERE ELEMENTID IN " + eidList + " AND TYPE NOT IN ('OBB','PROJOBB') ORDER BY ELEMENTID";
               }
               currStep = sqlStmt3;
#if ORACLE
               OracleCommand cmdF = new OracleCommand(sqlStmt3, DBOperation.DBConn);
               cmdF.FetchSize = 500;
               OracleDataReader readerF = cmdF.ExecuteReader();
#endif
#if POSTGRES
               NpgsqlCommand cmdF = new NpgsqlCommand(sqlStmt3, DBOperation.DBConn);
               cmdF.Prepare();
               NpgsqlDataReader readerF = cmdF.ExecuteReader();
#endif
               string currentEid = "";
               List<TopoFace> tfList = new List<TopoFace>();
               while (readerF.Read())
               {
                  string elemid = readerF.GetString(0);
                  if (string.IsNullOrEmpty(currentEid))
                  {
                        currentEid = elemid;
                  }
                  else if (string.Compare(currentEid, elemid)!=0 && !string.IsNullOrEmpty(currentEid))
                  {
                        if (string.Compare(currentEid, spb.Key) == 0)
                           theSpaceF = tfList;
                        else
                           faceColl.Add(currentEid, tfList);

                        // a new set of faces from different eid
                        tfList = new List<TopoFace>();
                        currentEid = elemid;
                  }

                  TopoFace tfData = new TopoFace();
                  tfData.ID = readerF.GetString(1);
                  tfData.type = readerF.GetString(2);
#if ORACLE
                  tfData.polygon = readerF.GetValue(3) as SdoGeometry;
                  tfData.sdoNormal = readerF.GetValue(4) as SdoGeometry;
                  tfData.normal = new Vector3D(tfData.sdoNormal.SdoPoint.XD.Value, tfData.sdoNormal.SdoPoint.YD.Value, tfData.sdoNormal.SdoPoint.ZD.Value);
                  tfData.reversedNormal = new Vector3D(-tfData.sdoNormal.SdoPoint.XD.Value, -tfData.sdoNormal.SdoPoint.YD.Value, -tfData.sdoNormal.SdoPoint.ZD.Value);
                  tfData.angleFromNorth = readerF.GetDouble(5);
                  tfData.sdoCentroid = readerF.GetValue(6) as SdoGeometry;
                  tfData.centroid = new Point3D(tfData.sdoCentroid.SdoPoint.XD.Value, tfData.sdoCentroid.SdoPoint.YD.Value, tfData.sdoCentroid.SdoPoint.ZD.Value);
#endif
#if POSTGRES
                  tfData.polygon = readerF.GetString(3);
                  Point3D norm = readerF.GetFieldValue<Point3D>(4);
                  tfData.normal = new Vector3D(norm.X, norm.Y, norm.Z);
                  tfData.reversedNormal = new Vector3D(-tfData.normal.X, -tfData.normal.Y, -tfData.normal.Z);
                  tfData.angleFromNorth = readerF.GetDouble(5);
                  tfData.centroid = readerF.GetFieldValue<Point3D>(6);
#endif
                  tfList.Add(tfData);
               }
               if (string.Compare(currentEid, spb.Key) == 0)
                  theSpaceF = tfList;
               else
                  faceColl.Add(currentEid, tfList);
               readerF.Dispose();
               cmdF.Dispose();

               // pair the space with all other boundaries
               HashSet<Tuple<string, string>> processedFace = new HashSet<Tuple<string, string>>();    // keep set of face(s) that has been paired
               foreach (TopoFace tf in theSpaceF)
               {
#if ORACLE
                  Face3D tfFace = SDOToFace3D(tf.polygon);
#endif
#if POSTGRES
                  dynamic polyg = JsonConvert.DeserializeObject(tf.polygon);
                  Face3D tfFace = JsonGeomUtils.Generate_Face3D(polyg);
#endif
                  int axisAlg = normalAxisAligned(tf.normal);
                  foreach (KeyValuePair<string, List<TopoFace>> boundTF in faceColl)
                  {
                     foreach(TopoFace bTF in boundTF.Value)
                     {
                        if (processedFace.Contains(new Tuple<string, string>(boundTF.Key, bTF.ID)))
                           continue;   // This face has been processed and paired previously, skip

                        Vector3D SnormalToCompare = tf.normal;
                        Vector3D BnormalToCompare = bTF.normal;
                        SnormalToCompare = tf.reversedNormal;

                        if (SnormalToCompare != BnormalToCompare)
                           continue;

                                
                        if ((axisAlg == 0) && (SnormalToCompare == BnormalToCompare) && !MathUtils.equalTol(tf.centroid.X, bTF.centroid.X))
                           continue;   // these 2 faces are not on the same YZ plane, not real boundary face

                        if ((axisAlg == 1) && (SnormalToCompare == BnormalToCompare) && !MathUtils.equalTol(tf.centroid.Y, bTF.centroid.Y))
                           continue;   // these 2 faces are not on the same XZ plane, not real boundary face

                        if ((axisAlg == 2) && (SnormalToCompare == BnormalToCompare) && !MathUtils.equalTol(tf.centroid.Z, bTF.centroid.Z))
                           continue;   // these 2 faces are not on the same XY plane, not real boundary face

#if ORACLE
                        Face3D btfFace = SDOToFace3D(bTF.polygon);
#endif
#if POSTGRES
                        dynamic polygB = JsonConvert.DeserializeObject(bTF.polygon);
                        Face3D btfFace = JsonGeomUtils.Generate_Face3D(polygB);
#endif

                        if (!Face3D.touch(tfFace, btfFace))
                           continue;       // not touching each other, skip

                        bool cPAtS = Face3D.inside(tfFace, bTF.centroid);
                        bool cPAtBPS = Face3D.inside(btfFace, tf.centroid);

                        if (cPAtS && cPAtBPS)
                        {
#if ORACLE
                           commonPointAtS.Add(Point3DToSdoGeometry(bTF.centroid));
                           cPointAtSPS.Add(OracleParameterStatus.Success);
                           commonPointAtB.Add(Point3DToSdoGeometry(tf.centroid));
                           cPointAtBPS.Add(OracleParameterStatus.Success);
#endif
#if POSTGRES
                           commonPointAtS.Add(bTF.centroid);
                           commonPointAtB.Add(tf.centroid);
#endif
                        }
                        else if (cPAtS && !cPAtBPS)
                        {
                           // if centroid of the other boundary face is not in the face, set both the same value
#if ORACLE
                           commonPointAtS.Add(Point3DToSdoGeometry(bTF.centroid));
                           cPointAtSPS.Add(OracleParameterStatus.Success);
                           commonPointAtB.Add(Point3DToSdoGeometry(bTF.centroid));
                           cPointAtBPS.Add(OracleParameterStatus.Success);
#endif
#if POSTGRES
                           commonPointAtS.Add(bTF.centroid);
                           commonPointAtB.Add(bTF.centroid);
#endif
                        }
                        else if (!cPAtS && cPAtBPS)
                        {
#if ORACLE
                           commonPointAtS.Add(Point3DToSdoGeometry(tf.centroid));
                           cPointAtSPS.Add(OracleParameterStatus.Success);
                           commonPointAtB.Add(Point3DToSdoGeometry(tf.centroid));
                           cPointAtBPS.Add(OracleParameterStatus.Success);
#endif
#if POSTGRES
                           commonPointAtS.Add(tf.centroid);
                           commonPointAtB.Add(tf.centroid);
#endif
                        }
                        else
                        {
#if ORACLE
                           commonPointAtS.Add(new SdoGeometry());
                           cPointAtSPS.Add(OracleParameterStatus.NullInsert);
                           commonPointAtB.Add(new SdoGeometry());
                           cPointAtBPS.Add(OracleParameterStatus.NullInsert);
#endif
#if POSTGRES
                           commonPointAtS.Add(null);
                           commonPointAtB.Add(null);
#endif
                        }

                        // They are pair. Create DB entry for it
                        spaceEID.Add(spb.Key);
                        sFaceBoundID.Add(tf.ID);
                        boundaryEID.Add(boundTF.Key);
                        bFaceBoundID.Add(bTF.ID);
                        SFpolygonList.Add(tf.polygon);
                        SFanglefromnorthList.Add(tf.angleFromNorth);
                        BFpolygonList.Add(bTF.polygon);
                        BFanglefromnorthList.Add(bTF.angleFromNorth);
#if ORACLE
                        SFnormalList.Add(tf.sdoNormal);
                        SFanglefromnorthStatus.Add(OracleParameterStatus.Success);
                        SFcentroidList.Add(tf.sdoCentroid);
                        BFnormalList.Add(bTF.sdoNormal);
                        BFanglefromnorthStatus.Add(OracleParameterStatus.Success);
                        BFcentroidList.Add(bTF.sdoCentroid);
#endif
#if POSTGRES
                        SFnormalList.Add(tf.normal.ToPoint3D());
                        SFcentroidList.Add(tf.centroid);
                        BFnormalList.Add(bTF.normal.ToPoint3D());
                        BFcentroidList.Add(bTF.centroid);
#endif
                        processedFace.Add(new Tuple<string, string>(boundTF.Key, bTF.ID));
                     }
                  }
               }
               faceColl.Clear();
            }

            // Insert record into a new table
#if ORACLE
            string insStmt = "INSERT INTO " + DBOperation.formatTabName("BIMRL_RELSPACEB_DETAIL") + "(SPACEELEMENTID,SFACEBOUNDID,COMMONPOINTATS,"
                              + "BOUNDARYELEMENTID,BFACEBOUNDID,COMMONPOINTATB,SFACEPOLYGON,SFACENORMAL,SFACEANGLEFROMNORTH,SFACECENTROID,"
                              + "BFACEPOLYGON,BFACENORMAL,BFACEANGLEFROMNORTH,BFACECENTROID) "
                              + " VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)";
            currStep = insStmt;
            OracleCommand insCmd = new OracleCommand(insStmt, DBOperation.DBConn);
            OracleParameter[] pars = new OracleParameter[14];
            pars[0] = insCmd.Parameters.Add("1", OracleDbType.Varchar2);
            pars[1] = insCmd.Parameters.Add("2", OracleDbType.Varchar2);
            pars[2] = insCmd.Parameters.Add("3", OracleDbType.Object);
            pars[2].UdtTypeName = "MDSYS.SDO_GEOMETRY";
            pars[3] = insCmd.Parameters.Add("4", OracleDbType.Varchar2);
            pars[4] = insCmd.Parameters.Add("5", OracleDbType.Varchar2);
            pars[5] = insCmd.Parameters.Add("6", OracleDbType.Object);
            pars[5].UdtTypeName = "MDSYS.SDO_GEOMETRY";
            pars[6] = insCmd.Parameters.Add("7", OracleDbType.Object);
            pars[6].UdtTypeName = "MDSYS.SDO_GEOMETRY";
            pars[7] = insCmd.Parameters.Add("8", OracleDbType.Object);
            pars[7].UdtTypeName = "MDSYS.SDO_GEOMETRY";
            pars[8] = insCmd.Parameters.Add("9", OracleDbType.Double);
            pars[9] = insCmd.Parameters.Add("10", OracleDbType.Object);
            pars[9].UdtTypeName = "MDSYS.SDO_GEOMETRY";
            pars[10] = insCmd.Parameters.Add("11", OracleDbType.Object);
            pars[10].UdtTypeName = "MDSYS.SDO_GEOMETRY";
            pars[11] = insCmd.Parameters.Add("12", OracleDbType.Object);
            pars[11].UdtTypeName = "MDSYS.SDO_GEOMETRY";
            pars[12] = insCmd.Parameters.Add("13", OracleDbType.Double);
            pars[13] = insCmd.Parameters.Add("14", OracleDbType.Object);
            pars[13].UdtTypeName = "MDSYS.SDO_GEOMETRY";

            for (int i = 0; i < pars.Count(); ++i)
               pars[i].Direction = ParameterDirection.Input;

            while (spaceEID.Count > 0)
            {
               int recCount = DBOperation.commitInterval;
               if (spaceEID.Count < recCount)
                  recCount = spaceEID.Count;

               pars[0].Value = spaceEID.GetRange(0, recCount).ToArray();
               spaceEID.RemoveRange(0, recCount);
               pars[0].Size = recCount;

               pars[1].Value = sFaceBoundID.GetRange(0, recCount).ToArray();
               sFaceBoundID.RemoveRange(0, recCount);
               pars[1].Size = recCount;

               pars[2].Value = commonPointAtS.GetRange(0, recCount).ToArray();
               commonPointAtS.RemoveRange(0, recCount);
               pars[2].Size = recCount;
               pars[2].ArrayBindStatus = cPointAtSPS.GetRange(0, recCount).ToArray();
               cPointAtSPS.RemoveRange(0, recCount);

               pars[3].Value = boundaryEID.GetRange(0, recCount).ToArray();
               boundaryEID.RemoveRange(0, recCount);
               pars[3].Size = recCount;

               pars[4].Value = bFaceBoundID.GetRange(0, recCount).ToArray();
               bFaceBoundID.RemoveRange(0, recCount);
               pars[4].Size = recCount;

               pars[5].Value = commonPointAtB.GetRange(0, recCount).ToArray();
               commonPointAtB.RemoveRange(0, recCount);
               pars[5].Size = recCount;
               pars[5].ArrayBindStatus = cPointAtBPS.GetRange(0, recCount).ToArray();
               cPointAtBPS.RemoveRange(0, recCount);

               pars[6].Value = SFpolygonList.GetRange(0, recCount).ToArray();
               SFpolygonList.RemoveRange(0, recCount);
               pars[6].Size = recCount;

               pars[7].Value = SFnormalList.GetRange(0, recCount).ToArray();
               SFnormalList.RemoveRange(0, recCount);
               pars[7].Size = recCount;

               pars[8].Value = SFanglefromnorthList.GetRange(0, recCount).ToArray();
               SFanglefromnorthList.RemoveRange(0, recCount);
               pars[8].Size = recCount;
               pars[8].ArrayBindStatus = SFanglefromnorthStatus.GetRange(0, recCount).ToArray();
               SFanglefromnorthStatus.RemoveRange(0, recCount);

               pars[9].Value = SFcentroidList.GetRange(0, recCount).ToArray();
               SFcentroidList.RemoveRange(0, recCount);
               pars[9].Size = recCount;

               pars[10].Value = BFpolygonList.GetRange(0, recCount).ToArray();
               BFpolygonList.RemoveRange(0, recCount);
               pars[10].Size = recCount;

               pars[11].Value = BFnormalList.GetRange(0, recCount).ToArray();
               BFnormalList.RemoveRange(0, recCount);
               pars[11].Size = recCount;

               pars[12].Value = BFanglefromnorthList.GetRange(0, recCount).ToArray();
               BFanglefromnorthList.RemoveRange(0, recCount);
               pars[12].Size = recCount;
               pars[12].ArrayBindStatus = BFanglefromnorthStatus.GetRange(0, recCount).ToArray();
               BFanglefromnorthStatus.RemoveRange(0, recCount);

               pars[13].Value = BFcentroidList.GetRange(0, recCount).ToArray();
               BFcentroidList.RemoveRange(0, recCount);
               pars[13].Size = recCount;

               insCmd.ArrayBindCount = recCount;
               insCmd.ExecuteNonQuery();
               DBOperation.commitTransaction();
            }
#endif
#if POSTGRES
            string insStmt = "INSERT INTO " + DBOperation.formatTabName("BIMRL_RELSPACEB_DETAIL") + "(SPACEELEMENTID,SFACEBOUNDID,COMMONPOINTATS,"
                  + "BOUNDARYELEMENTID,BFACEBOUNDID,COMMONPOINTATB,SFACEPOLYGON,SFACENORMAL,SFACEANGLEFROMNORTH,SFACECENTROID,"
                  + "BFACEPOLYGON,BFACENORMAL,BFACEANGLEFROMNORTH,BFACECENTROID) "
                  + " VALUES (@spid, @sface, @scpt, @bid, @bface, @bcpt, @spoly, @snorm, @san, @sfctr, @bpoly, @bnorm, @ban, @bctr)";
            currStep = insStmt;
            NpgsqlCommand insCmd = new NpgsqlCommand(insStmt, DBOperation.DBConn);

            for (int i = 0; i < spaceEID.Count; ++i)
            {
               insCmd.Parameters.Clear();
               insCmd.Parameters.AddWithValue("@spid", spaceEID[i]);
               insCmd.Parameters.AddWithValue("@sface", sFaceBoundID[i]);
               if (BIMRLUtils.IsNull(commonPointAtS[i]))
                  insCmd.Parameters.AddWithValue("@scpt", DBNull.Value);
               else
                  insCmd.Parameters.AddWithValue("@scpt", commonPointAtS[i]);
               insCmd.Parameters.AddWithValue("@bid", boundaryEID[i]);
               insCmd.Parameters.AddWithValue("@bface", bFaceBoundID[i]);
               if (BIMRLUtils.IsNull(commonPointAtB[i]))
                  insCmd.Parameters.AddWithValue("@bcpt", DBNull.Value);
               else
                  insCmd.Parameters.AddWithValue("@bcpt", commonPointAtB[i]);
               insCmd.Parameters.AddWithValue("@spoly", NpgsqlDbType.Jsonb, SFpolygonList[i]);
               insCmd.Parameters.AddWithValue("@snorm", SFnormalList[i]);
               insCmd.Parameters.AddWithValue("@san", SFanglefromnorthList[i]);
               insCmd.Parameters.AddWithValue("@sfctr", SFcentroidList[i]);
               insCmd.Parameters.AddWithValue("@bpoly", NpgsqlDbType.Jsonb, BFpolygonList[i]);
               insCmd.Parameters.AddWithValue("@bnorm", BFnormalList[i]);
               insCmd.Parameters.AddWithValue("@ban", BFanglefromnorthList[i]);
               insCmd.Parameters.AddWithValue("@bctr", BFcentroidList[i]);
               //insCmd.Prepare();
               insCmd.ExecuteNonQuery();
            }
            DBOperation.commitTransaction();
#endif
            insCmd.Dispose();
         }
#if ORACLE
         catch (OracleException e)
#endif
#if POSTGRES
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + currStep;
            _refBIMRLCommon.StackPushError(excStr);
            throw;
         }
         catch (SystemException e)
         {
            string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
            _refBIMRLCommon.StackPushError(excStr);
            throw;
         }
      }

      public void ProcessOrientation(string addCond)
      {
         List<string> faceidList = new List<string>();
         List<Vector3D> normalLIst = new List<Vector3D>();
         List<string> elemTypeList = new List<string>();
#if ORACLE
         List<SdoGeometry> polygonLIst = new List<SdoGeometry>();
         List<SdoGeometry> centroidList = new List<SdoGeometry>();
#endif
#if POSTGRES
         List<string> polygonLIst = new List<string>();
         List<Point3D> centroidList = new List<Point3D>();
#endif

         string sqlStmt = "SELECT ELEMENTID, ID, NORMAL, POLYGON, CENTROID, ELEMENTTYPE FROM " + DBOperation.formatTabName("BIMRL_TOPOFACEV") ;
         if (!string.IsNullOrEmpty(addCond))
               sqlStmt += " WHERE " + addCond;
         sqlStmt += " ORDER BY ELEMENTID, ID";

         try
         {
#if ORACLE
            OracleCommand cmd = new OracleCommand(sqlStmt, DBOperation.DBConn);
            cmd.FetchSize = 100;
            OracleDataReader reader = cmd.ExecuteReader();
#endif
#if POSTGRES
            NpgsqlCommand cmd = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
            NpgsqlDataReader reader = cmd.ExecuteReader();
#endif
            string prevElemid = "";
            while (reader.Read())
            {
               string elemid = reader.GetString(0);
               string faceid = reader.GetString(1);
#if ORACLE
               SdoGeometry normal = reader.GetValue(2) as SdoGeometry;
               Vector3D normalV = new Vector3D(normal.SdoPoint.XD.Value, normal.SdoPoint.YD.Value, normal.SdoPoint.ZD.Value);
               SdoGeometry polygon = reader.GetValue(3) as SdoGeometry;
               SdoGeometry centroid = reader.GetValue(4) as SdoGeometry;
#endif
#if POSTGRES
               Point3D normal = reader.GetFieldValue<Point3D>(2);
               Vector3D normalV = new Vector3D(normal.X, normal.Y, normal.Z);
               string polygon = reader.GetString(3);
               Point3D centroid = reader.GetFieldValue<Point3D>(4);
#endif
               string elemType = reader.GetString(5);

               if ((string.Compare(prevElemid, elemid) == 0) || string.IsNullOrEmpty(prevElemid))
               {
                  faceidList.Add(faceid);
                  normalLIst.Add(normalV);
                  polygonLIst.Add(polygon);
                  centroidList.Add(centroid);
                  elemTypeList.Add(elemType);
                  if (string.IsNullOrEmpty(prevElemid))
                        prevElemid = elemid;
               }
               else
               {
                  examineFaceOrientation(prevElemid, faceidList, normalLIst, polygonLIst, centroidList, elemTypeList);

                  faceidList.Clear();
                  normalLIst.Clear();
                  polygonLIst.Clear();
                  centroidList.Clear();
                  elemTypeList.Clear();

                  prevElemid = elemid;
                  faceidList.Add(faceid);
                  normalLIst.Add(normalV);
                  polygonLIst.Add(polygon);
                  centroidList.Add(centroid);
                  elemTypeList.Add(elemType);
               }
            }
            reader.Close();
            //reader.Dispose();
            cmd.Dispose();

            if (faceidList.Count > 0)
            {
               examineFaceOrientation(prevElemid, faceidList, normalLIst, polygonLIst, centroidList, elemTypeList);
            }
         }
#if ORACLE
         catch (OracleException e)
#endif
#if POSTGRES
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Read Error - " + e.Message + "\n\t" + sqlStmt;
            _refBIMRLCommon.StackPushError(excStr);
         }
         catch (SystemException e)
         {
            string excStr = "%%Read Error - " + e.Message + "\n\t" + sqlStmt;
            _refBIMRLCommon.StackPushError(excStr);
            throw;
         }
      }

#if ORACLE
      public void examineFaceOrientation(string elemid, List<string> faceidList, List<Vector3D> normalList, List<SdoGeometry> polygonList, List<SdoGeometry> centroidList, List<string> elemTypeList)
#endif
#if POSTGRES
      public void examineFaceOrientation(string elemid, List<string> faceidList, List<Vector3D> normalList, List<string> polygonList, List<Point3D> centroidList, List<string> elemTypeList)
#endif
      {
         // Determine orientation of a face. We are interested in TOP, BOTTOM, SIDE. To allow slight variation, we will measure the vector value on the axis within 10%
         // Since the normal value is already normalized, this means any value > 0.9 will be considered good. If there are more than one the closer to 1.0 and position will take priority
#if ORACLE
         OracleCommand cmd = new OracleCommand("", DBOperation.DBConn);
#endif
#if POSTGRES
         //NpgsqlCommand cmd = new NpgsqlCommand("", DBOperation.DBConn);
#endif
         string idlist = "";
         string stmt = "";
         try
         {
            // 1. Test for top
            double prevVal = 0.0;
            Point3D prevPos = new Point3D(double.MinValue, double.MinValue, double.MinValue);
            int prevIdx = -1;
            List<int> faceIdxColl = new List<int>();

            List<int> removeFromList = new List<int>();
            for (int i = 0; i < faceidList.Count; ++i)
            {
               Face3D face;
#if ORACLE
               SDOGeomUtils.generate_Face3D(polygonList[i], out face);
#endif
#if POSTGRES
               dynamic polyg = JsonConvert.DeserializeObject(polygonList[i]);
               face = JsonGeomUtils.Generate_Face3D(polyg);
#endif
               Point3D highestPoint = getHighestPoint(face);
               if (MathUtils.equalTolSign(normalList[i].Z, 1.0, 0.1) && normalList[i].Z > prevVal && highestPoint.Z > prevPos.Z)
               {
                  prevPos = highestPoint;
                  if (prevIdx >= 0)
                        removeFromList.Add(prevIdx);
                  prevIdx = i;
                  faceIdxColl.Clear();
                  faceIdxColl.Add(prevIdx);
                  prevVal = normalList[i].Z;
               }
               else if (MathUtils.equalTolSign(normalList[i].Z, 1.0, 0.1) && MathUtils.equalTol(normalList[i].Z,prevVal) && MathUtils.equalTol(highestPoint.Z,prevPos.Z))
               {
                  removeFromList.Add(prevIdx);
                  faceIdxColl.Add(prevIdx);
               }
            }
            if (prevIdx >= 0 && faceIdxColl.Count > 0)
            {
               if (!removeFromList.Contains(prevIdx))
                  removeFromList.Add(prevIdx);
               string orientation = faceOrientation.TOP.ToString();
               idlist = "";
               for (int k = 0; k < faceIdxColl.Count; ++k)
               {
                  if (!string.IsNullOrEmpty(idlist))
                        idlist += ", ";
                  idlist += "'" + faceidList[faceIdxColl[k]] + "'";
               }
               stmt = "UPDATE " + DBOperation.formatTabName("BIMRL_TOPO_FACE") + " SET ORIENTATION='" + orientation + "'"
                                    + ", TOPORBOTTOM_Z=" + prevPos.Z.ToString("F4")
                                    + " WHERE ELEMENTID='" + elemid + "' AND ID IN (" + idlist + ")";

#if ORACLE
               cmd.CommandText = stmt;
               int stat = cmd.ExecuteNonQuery();
#endif
#if POSTGRES
               DBOperation.ExecuteNonQueryWithTrans2(stmt);
#endif

               for (int j = removeFromList.Count - 1; j >= 0; --j)
               {
                  normalList.RemoveAt(removeFromList[j]);
                  faceidList.RemoveAt(removeFromList[j]);
                  polygonList.RemoveAt(removeFromList[j]);
                  centroidList.RemoveAt(removeFromList[j]);
               }
            }
            else
            {
               faceIdxColl.Clear();
               prevIdx = -1;
               prevVal = 0.4999;
               // No TOP face found: find alternative for TOPSIDE, which is defined as the largest positive +Z (above 0.5) and highest boundingbox
               for (int i = 0; i < faceidList.Count; ++i)
               {
                  Face3D face;
#if ORACLE
                  SDOGeomUtils.generate_Face3D(polygonList[i], out face);
#endif
#if POSTGRES
                  dynamic polyg = JsonConvert.DeserializeObject(polygonList[i]);
                  face = JsonGeomUtils.Generate_Face3D(polyg);
#endif

                  Point3D highestPoint = getHighestPoint(face);
                  if ((normalList[i].Z > prevVal && highestPoint.Z > prevPos.Z) )
                  {
                     prevPos = highestPoint;
                     prevIdx = i;
                     faceIdxColl.Clear();
                     faceIdxColl.Add(prevIdx);
                     prevVal = normalList[i].Z;
                  }
                  else if (MathUtils.equalTol(normalList[i].Z, prevVal, MathUtils.defaultTol) && MathUtils.equalTol(highestPoint.Z, prevPos.Z))
                  {
                     faceIdxColl.Add(i);
                  }
               }
               if (prevIdx >= 0 && faceIdxColl.Count > 0)
               {
                  string orientation = faceOrientation.TOPSIDE.ToString();
                  idlist = "";
                  for (int k = 0; k < faceIdxColl.Count; ++k)
                  {
                     if (!string.IsNullOrEmpty(idlist))
                        idlist += ", ";
                     idlist += "'" + faceidList[faceIdxColl[k]] + "'";
                  }

                  stmt = "UPDATE " + DBOperation.formatTabName("BIMRL_TOPO_FACE") + " SET ORIENTATION='" + orientation + "'"
                                       + ", TOPORBOTTOM_Z=" + prevPos.Z.ToString("F4")
                                       + " WHERE ELEMENTID='" + elemid + "' AND ID IN (" + idlist + ")";
#if ORACLE
                  cmd.CommandText = stmt;
                  int stat = cmd.ExecuteNonQuery();
#endif
#if POSTGRES
                  DBOperation.ExecuteNonQueryWithTrans2(stmt);
#endif
                  normalList.RemoveAt(prevIdx);
                  faceidList.RemoveAt(prevIdx);
                  polygonList.RemoveAt(prevIdx);
                  centroidList.RemoveAt(prevIdx);
               }
            }

            // 2. Test for bottom
            prevVal = 0.0;
            prevPos = new Point3D(double.MaxValue, double.MaxValue, double.MaxValue);
            prevIdx = -1;

            removeFromList.Clear();
            faceIdxColl.Clear();
            for (int i = 0; i < faceidList.Count; ++i)
            {
               Face3D face;
#if ORACLE
               SDOGeomUtils.generate_Face3D(polygonList[i], out face);
#endif
#if POSTGRES
               dynamic polyg = JsonConvert.DeserializeObject(polygonList[i]);
               face = JsonGeomUtils.Generate_Face3D(polyg);
#endif

               Point3D lowestPoint = getLowestPoint(face);
               if (MathUtils.equalTolSign(normalList[i].Z, -1.0, 0.1) && normalList[i].Z < prevVal && lowestPoint.Z < prevPos.Z)
               {
                  prevPos = lowestPoint;
                  if (prevIdx >= 0)
                        removeFromList.Add(prevIdx);
                  prevIdx = i;
                  faceIdxColl.Clear();
                  faceIdxColl.Add(prevIdx);
                  prevVal = normalList[i].Z;
               }
               else if (MathUtils.equalTolSign(normalList[i].Z, -1.0, 0.1) && MathUtils.equalTol(normalList[i].Z, prevVal) && MathUtils.equalTol(lowestPoint.Z, prevPos.Z))
               {
                  removeFromList.Add(prevIdx);
                  faceIdxColl.Add(prevIdx);
               }
            }
            if (prevIdx >= 0 && faceIdxColl.Count > 0)
            {
               if (!removeFromList.Contains(prevIdx))
                  removeFromList.Add(prevIdx);
               string orientation = faceOrientation.BOTTOM.ToString();
               idlist = "";
               for (int k = 0; k < faceIdxColl.Count; ++k)
               {
                  if (!string.IsNullOrEmpty(idlist))
                     idlist += ", ";
                  idlist += "'" + faceidList[faceIdxColl[k]] + "'";
               }
               stmt = "UPDATE " + DBOperation.formatTabName("BIMRL_TOPO_FACE") + " SET ORIENTATION='" + orientation + "'"
                                    + ", TOPORBOTTOM_Z=" + prevPos.Z.ToString("F4")
                                    + " WHERE ELEMENTID='" + elemid + "' AND ID IN (" + idlist + ")";
#if ORACLE
               cmd.CommandText = stmt;
               int stat = cmd.ExecuteNonQuery();
#endif
#if POSTGRES
               DBOperation.ExecuteNonQueryWithTrans2(stmt);
#endif

               for (int j = removeFromList.Count - 1; j >= 0; --j)
               {
                  normalList.RemoveAt(removeFromList[j]);
                  faceidList.RemoveAt(removeFromList[j]);
                  polygonList.RemoveAt(removeFromList[j]);
                  centroidList.RemoveAt(removeFromList[j]);
               }
            }
            else
            {
               faceIdxColl.Clear();
               prevIdx = -1;
               prevVal = -0.4999;
               // No BOTTOM face found: find alternative for TOPSIDE, which is defined as the largest positive -Z (above 0.5) and highest boundingbox
               for (int i = 0; i < faceidList.Count; ++i)
               {
                  Face3D face;
#if ORACLE
                  SDOGeomUtils.generate_Face3D(polygonList[i], out face);
#endif
#if POSTGRES
                  dynamic polyg = JsonConvert.DeserializeObject(polygonList[i]);
                  face = JsonGeomUtils.Generate_Face3D(polyg);
#endif

                  Point3D lowestPoint = getHighestPoint(face);
                  if (normalList[i].Z < prevVal && lowestPoint.Z < prevPos.Z)
                  {
                     prevPos = lowestPoint;
                     prevIdx = i;
                     faceIdxColl.Clear();
                     faceIdxColl.Add(prevIdx);
                     prevVal = normalList[i].Z;
                  }
               }
               if (prevIdx >= 0 && faceIdxColl.Count > 0)
               {
                  string orientation = faceOrientation.UNDERSIDE.ToString();
                  idlist = "";
                  for (int k = 0; k < faceIdxColl.Count; ++k)
                  {
                     if (!string.IsNullOrEmpty(idlist))
                        idlist += ", ";
                     idlist += "'" + faceidList[faceIdxColl[k]] + "'";
                  }

                  stmt = "UPDATE " + DBOperation.formatTabName("BIMRL_TOPO_FACE") + " SET ORIENTATION='" + orientation + "'"
                     + ", TOPORBOTTOM_Z=" + prevPos.Z.ToString("F4")
                     + " WHERE ELEMENTID='" + elemid + "' AND ID IN (" + idlist + ")";

#if ORACLE
                  cmd.CommandText = stmt;
                  int stat = cmd.ExecuteNonQuery();
#endif
#if POSTGRES
                  DBOperation.ExecuteNonQueryWithTrans2(stmt);
#endif

                  normalList.RemoveAt(prevIdx);
                  faceidList.RemoveAt(prevIdx);
                  polygonList.RemoveAt(prevIdx);
                  centroidList.RemoveAt(prevIdx);
               }
            }

            // 3. Test for side
            prevVal = 0.0;
            prevPos = new Point3D(double.MaxValue, double.MaxValue, double.MaxValue);
            prevIdx = -1;

            removeFromList.Clear();
            faceIdxColl.Clear();
            for (int i = 0; i < faceidList.Count; ++i)
            {
#if ORACLE
               Point3D centroid = new Point3D(centroidList[i].SdoPoint.XD.Value, centroidList[i].SdoPoint.YD.Value, centroidList[i].SdoPoint.ZD.Value);
#endif
#if POSTGRES
               Point3D centroid = centroidList[i];
#endif
               if (MathUtils.equalTol(normalList[i].Z, 0.0, 0.1))
               {
                  prevPos = centroid;
                  if (prevIdx >= 0)
                        removeFromList.Add(prevIdx);
                  prevIdx = i;
                  faceIdxColl.Add(prevIdx);
                  prevVal = normalList[i].Z;
               }
            }
            if (prevIdx >= 0 && faceIdxColl.Count> 0)
            {
               if (!removeFromList.Contains(prevIdx))
                  removeFromList.Add(prevIdx);
               string orientation = faceOrientation.SIDE.ToString();
               idlist = "";
               List<string> fIdList = new List<string>();
               if (faceIdxColl.Count > 900)
               {
                  for (int k = 0; k < faceIdxColl.Count; ++k)
                  {
                     fIdList.Add(faceidList[faceIdxColl[k]]);
                  }
                  //#if ORACLE
                  //                  OracleParameter par = new OracleParameter();
                  //                  par = cmd.Parameters.Add("1", OracleDbType.Varchar2);
                  //                  par.Value = fIdList.ToArray();
                  //                  par.Size = fIdList.Count;
                  //#endif

#if ORACLE
                  DBOperation.executeSingleStmt("DELETE FROM BIMRLQUERYTEMP");

                  OracleCommand addCmd = new OracleCommand("INSERT INTO BIMRLQUERYTEMP (ID1) VALUES (:1)", DBOperation.DBConn);
                  OracleParameter param = new OracleParameter();
                  param = addCmd.Parameters.Add("1", OracleDbType.Varchar2);
                  param.Direction = ParameterDirection.Input;
                  addCmd.ArrayBindCount = fIdList.Count;
                  addCmd.ExecuteNonQuery();
#endif
#if POSTGRES
                  DBOperation.ExecuteNonQueryWithTrans2("DELETE FROM BIMRLQUERYTEMP");
                  //NpgsqlCommand addCmd = new NpgsqlCommand("INSERT INTO BIMRLQUERYTEMP (ID1) VALUES (@id1)", DBOperation.DBConn);

                  //foreach(string id in fIdList)
                  //{
                  //   addCmd.Parameters.Clear();
                  //   addCmd.Parameters.AddWithValue(NpgsqlDbType.Varchar, id);
                  //   //addCmd.Prepare();
                  //   addCmd.ExecuteNonQuery();
                  //}
                  IList<object> parList = new List<object>();
                  foreach (string id in fIdList)
                     parList.Add(id);

                  DBOperation.ExecuteNonQueryWithTrans2("INSERT INTO BIMRLQUERYTEMP (ID1) VALUES (@0)", parList);
#endif
                  stmt = "UPDATE " + DBOperation.formatTabName("BIMRL_TOPO_FACE") + " SET ORIENTATION='" + orientation + "'"
                                       + " WHERE ELEMENTID='" + elemid + "' AND ID IN ( SELECT ID1 FROM BIMRLQUERYTEMP )";
               }
               else
               {
                  for (int k = 0; k < faceIdxColl.Count; ++k)
                  {
                     if (!string.IsNullOrEmpty(idlist))
                        idlist += ", ";
                     idlist += "'" + faceidList[faceIdxColl[k]] + "'";
                  }
                  stmt = "UPDATE " + DBOperation.formatTabName("BIMRL_TOPO_FACE") + " SET ORIENTATION='" + orientation + "'"
                                       + " WHERE ELEMENTID='" + elemid + "' AND ID IN (" + idlist + ")";
               }
#if ORACLE
               cmd.CommandText = stmt;
               int stat = cmd.ExecuteNonQuery();
#endif
#if POSTGRES
               DBOperation.ExecuteNonQueryWithTrans2(stmt);
#endif
               for (int j = removeFromList.Count - 1; j >= 0; --j)
               {
                  normalList.RemoveAt(removeFromList[j]);
                  faceidList.RemoveAt(removeFromList[j]);
                  centroidList.RemoveAt(removeFromList[j]);
                  polygonList.RemoveAt(removeFromList[j]);
               }
            }
         }
#if ORACLE
         catch (OracleException e)
#endif
#if POSTGRES
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Update Orientation Error - " + e.Message + "\n\t" + "; elementId: " + elemid + "; faceid: " + idlist;
            _refBIMRLCommon.StackPushError(excStr);
         }
         catch (SystemException e)
         {
            string excStr = "%%Update Orientation Error - " + e.Message + "\n\t" + "; elementId: " + elemid + "; faceid: " + idlist;
            _refBIMRLCommon.StackPushError(excStr);
            throw;
         }
      }

      // return -1 if not axis aligned, 0 for X, 1 for Y, 2 for Z
      int normalAxisAligned(Vector3D normal)
      {
         int axis = -1;
         Vector3D vX = new Vector3D(1.0, 0.0, 0.0);
         Vector3D vY = new Vector3D(0.0, 1.0, 0.0);
         Vector3D vZ = new Vector3D(0.0, 0.0, 1.0);
         Vector3D vXn = new Vector3D(-1.0, 0.0, 0.0);
         Vector3D vYn = new Vector3D(0.0, -1.0, 0.0);
         Vector3D vZn = new Vector3D(0.0, 0.0, -1.0);
         if (normal == vX || normal == vXn)
            axis = 0;
         else if (normal == vY || normal == vYn)
            axis = 1;
         else if (normal == vZ || normal == vZn)
            axis = 2;

         return axis;
      }

      Point3D getHighestPoint(Face3D face)
      {
         Point3D highestVert = new Point3D(double.MinValue, double.MinValue, double.MinValue);
         foreach (Point3D vert in face.vertices)
         {
            if (vert.Z > highestVert.Z)
            {
               highestVert.X = vert.X;
               highestVert.Y = vert.Y;
               highestVert.Z = vert.Z;
            }
         }
         return highestVert;
      }

      Point3D getLowestPoint(Face3D face)
      {
         Point3D lowestVert = new Point3D(double.MaxValue, double.MaxValue, double.MaxValue);
         foreach (Point3D vert in face.vertices)
         {
            if (vert.Z < lowestVert.Z)
            {
               lowestVert.X = vert.X;
               lowestVert.Y = vert.Y;
               lowestVert.Z = vert.Z;
            }
         }
         return lowestVert;
      }

#if ORACLE
      SdoGeometry Point3DToSdoGeometry(Point3D point)
      {
         SdoGeometry geom = new SdoGeometry();
         geom.Dimensionality = 3;
         geom.LRS = 0;
         geom.GeometryType = (int)SdoGeometryTypes.GTYPE.POINT;
         int gType = geom.PropertiesToGTYPE();

         SdoPoint centroidP = new SdoPoint();
         centroidP.XD = point.X;
         centroidP.YD = point.Y;
         centroidP.ZD = point.Z;
         geom.SdoPoint = centroidP;

         return geom;
      }

      Face3D SDOToFace3D(SdoGeometry sdoGeomData)
      {
         int[] elInfo = sdoGeomData.ElemArrayOfInts;
         double[] ordArray = sdoGeomData.OrdinatesArrayOfDoubles;

         int noLoop = elInfo.Length / 3;     // first loop is the outerloop and the rest will be innerloop
         int totalVerts = ordArray.Length / 3;
         List<List<Point3D>> faceloops = new List<List<Point3D>>();
         for (int i = 0; i < noLoop; i++)
         {
            List<Point3D> vertsInLoop = new List<Point3D>();
            int initPos = elInfo[i * 3] - 1;

            int noVert = 0;
            if (i == noLoop - 1)
               noVert = (totalVerts - (elInfo[i * 3] - 1) / 3);
            else
               noVert = (elInfo[(i + 1) * 3] - elInfo[i * 3]) / 3;

            for (int j=0; j<noVert; ++j)
            {
               int pos = initPos + j * 3;
               vertsInLoop.Add(new Point3D(ordArray[pos], ordArray[pos + 1], ordArray[pos + 2]));
            }

            faceloops.Add(vertsInLoop);
         }

         Face3D face = new Face3D(faceloops);
         return face;
      }
#endif

      void processDoorLeafOrWindowPanelAttribute(string elemid)
      {
         string currStmt = "SELECT COUNT(*) FROM " + DBOperation.formatTabName("BIMRL_PROPERTIES") + " WHERE ELEMENTID='" + elemid
                              + "' AND (PROPERTYGROUPNAME='IFCDOORPANELPROPERTIES' OR PROPERTYGROUPNAME='IFCWINDOWPANELPROPERTIES') AND PROPERTYNAME='PANELPOSITION'";
#if ORACLE
         OracleCommand cmd2 = new OracleCommand(currStmt, DBOperation.DBConn);
#endif
#if POSTGRES
         NpgsqlCommand cmd2 = new NpgsqlCommand(currStmt, DBOperation.DBConn);
         cmd2.Prepare();
#endif
         object ret = cmd2.ExecuteScalar();
         int? panelCntVal = ret as int?;
         int panelCnt = 1;
         if (panelCntVal.HasValue)
            panelCnt = panelCntVal.Value;

         SortedList<double, Tuple<string, Vector3D>> faceArea = new SortedList<double,Tuple<string,Vector3D>>();

         string sqlStmt = "SELECT A.ID, SDO_GEOM.SDO_AREA(A.POLYGON, B.DIMINFO) AREA, A.NORMAL FROM " + DBOperation.formatTabName("BIMRL_TOPOFACEV")
                           + " A, ALL_SDO_GEOM_METADATA B WHERE A.ELEMENTID='" + elemid + "' AND A.ORIENTATION='" + faceOrientation.SIDE.ToString() + "' AND"
                           + " B.TABLE_NAME='BIMRL_TOPO_FACE_" + DBOperation.currFedModel.FederatedID.ToString("X4") + "' AND OWNER='" + DBOperation.currFedModel.Owner + "'"
                           + " AND B.COLUMN_NAME='POLYGON' ORDER BY AREA DESC";
         currStmt = sqlStmt;
         try
         {
#if ORACLE
            OracleCommand cmd = new OracleCommand(sqlStmt, DBOperation.DBConn);
            OracleDataReader reader = cmd.ExecuteReader();
#endif
#if POSTGRES
            NpgsqlCommand cmd = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
            cmd.Prepare();
            NpgsqlDataReader reader = cmd.ExecuteReader();
#endif
            while (reader.Read())
            {
               string id = reader.GetString(0);
               double area = reader.GetDouble(1);
#if ORACLE
               SdoGeometry normal = reader.GetValue(2) as SdoGeometry;
               Vector3D normalV = new Vector3D(normal.SdoPoint.XD.Value, normal.SdoPoint.YD.Value, normal.SdoPoint.ZD.Value);
#endif
#if POSTGRES
               Point3D normal = reader.GetFieldValue<Point3D>(2);
               Vector3D normalV = new Vector3D(normal.X, normal.Y, normal.Z);
#endif
               faceArea.Add(area, new Tuple<string, Vector3D>(id, normalV));
            }
            reader.Dispose();

            List<Tuple<string, string>> leafList = new List<Tuple<string, string>>();
            Vector3D firstNorm = null;
            Vector3D revfirstNorm = null;
            double lastArea = double.MinValue;
            string panelFStr = "PANEL - FRONT";
            string panelBStr = "PANEL - BACK";
            int count = 0;

            for (int i = faceArea.Count - 1; i < 0; --i)
            {
               if (i == faceArea.Count - 1)
               {
                  firstNorm = faceArea[i].Item2;
                  lastArea = faceArea.ElementAt(i).Key;
                  revfirstNorm = new Vector3D(faceArea[i].Item2.X, faceArea[i].Item2.Y, faceArea[i].Item2.Z);
                  revfirstNorm.Negate();
                  leafList.Add(new Tuple<string, string>(faceArea[i].Item1, panelFStr));
                  count++;
                  continue;
               }
               if (faceArea.ElementAt(i).Key == lastArea)
               {
                  if (faceArea[i].Item2 == firstNorm)
                  {
                     leafList.Add(new Tuple<string, string>(faceArea[i].Item1, panelFStr));
                     count++;
                  }
                  else if (faceArea[i].Item2 == revfirstNorm)
                  {
                     leafList.Add(new Tuple<string, string>(faceArea[i].Item1, panelBStr));
                     count++;
                  }
               }
               else
               {
                  if (count * 2 > panelCnt)
                     break;

                  if (faceArea[i].Item2 == firstNorm)
                  {
                     leafList.Add(new Tuple<string, string>(faceArea[i].Item1, panelFStr));
                     lastArea = faceArea.ElementAt(i).Key;
                     count++;
                  }
                  else if (faceArea[i].Item2 == revfirstNorm)
                  {
                     leafList.Add(new Tuple<string, string>(faceArea[i].Item1, panelBStr));
                     lastArea = faceArea.ElementAt(i).Key;
                     count++;
                  }
               }
            }

#if ORACLE
            string sqlUpd = "UPDATE " + DBOperation.formatTabName("BIMRL_TOPOFACEV") + " SET ATTRIBUTE=:1 WHERE ELEMENTID='" + elemid + "' AND ID=:2";
            OracleCommand cmdUpd = new OracleCommand(sqlUpd, DBOperation.DBConn);
            OracleParameter[] pars = new OracleParameter[2];
            pars[0] = cmdUpd.Parameters.Add("1", OracleDbType.Varchar2);
            pars[0].Direction = ParameterDirection.Input;
            pars[1] = cmdUpd.Parameters.Add("2", OracleDbType.Varchar2);
            pars[1].Direction = ParameterDirection.Input;
            pars[0].Size = 1;
            pars[1].Size = 1;
#endif
#if POSTGRES
            NpgsqlConnection arbConn = DBOperation.arbitraryConnection();
            NpgsqlTransaction arbTrans = arbConn.BeginTransaction();
            string sqlUpd = "UPDATE " + DBOperation.formatTabName("BIMRL_TOPOFACEV") + " SET ATTRIBUTE=@attr WHERE ELEMENTID=@elemid AND ID=@id";
            NpgsqlCommand cmdUpd = new NpgsqlCommand(sqlUpd, arbConn);

            cmdUpd.Parameters.Add("@elemid", NpgsqlDbType.Varchar);
            cmdUpd.Parameters.Add("@attr", NpgsqlDbType.Varchar);
            cmdUpd.Parameters.Add("@id", NpgsqlDbType.Varchar);
            //cmdUpd.Prepare();
#endif
            currStmt = sqlUpd;
            foreach (Tuple<string, string> panel in leafList)
            {
#if ORACLE
               pars[0].Value = panel.Item2;
               pars[1].Value = panel.Item1;
#endif
#if POSTGRES
               cmdUpd.Parameters["@elemid"].Value = elemid;
               if (string.IsNullOrEmpty(panel.Item2))
                  cmdUpd.Parameters["@attr"].Value = DBNull.Value;
               else
                  cmdUpd.Parameters["@attr"].Value = panel.Item2;
               if (string.IsNullOrEmpty(panel.Item1))
                  cmdUpd.Parameters["@id"].Value = DBNull.Value;
               else
                  cmdUpd.Parameters["@id"].Value = panel.Item1;
#endif
               cmdUpd.ExecuteNonQuery();
            }
#if ORACLE
            DBOperation.commitTransaction();
         }
         catch (OracleException e)
#endif
#if POSTGRES
            arbTrans.Commit();
            cmdUpd.Dispose();
            arbConn.Close();
         }
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Update Orientation Error - " + e.Message + "\n\t" + currStmt;
            _refBIMRLCommon.StackPushError(excStr);
         }
         catch (SystemException e)
         {
            string excStr = "%%Update Orientation Error - " + e.Message + "\n\t" + currStmt;
            _refBIMRLCommon.StackPushError(excStr);
            throw;
         }
      }
   }

}
