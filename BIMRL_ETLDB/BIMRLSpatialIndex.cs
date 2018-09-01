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
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;
#if ORACLE
using Oracle.DataAccess.Types;
using Oracle.DataAccess.Client;
using NetSdoGeometry;
#endif
#if POSTGRES
using Npgsql;
using NpgsqlTypes;
#endif
using BIMRL.Common;

namespace BIMRL
{
   public class BIMRLSpatialIndex
   {
      public static List<List<int>> polyHFaceVertIdxList = new List<List<int>>();
      public static List<List<double>> polyHCoordListList = new List<List<double>>();
      public static List<string> elemIDList = new List<string>();
      DataTable IdxList = new DataTable();

      BIMRLCommon _refBIMRLCommon;

      public BIMRLSpatialIndex(BIMRLCommon bimrlCommon)
      {
         _refBIMRLCommon = bimrlCommon;
         polyHFaceVertIdxList.Clear();
         polyHCoordListList.Clear();
         elemIDList.Clear();
      }

      public void createSpatialIndexFromBIMRLElement(int federatedId, string whereCond, bool createFaces, bool createSpIdx)
      {
         DBOperation.beginTransaction();
         string currStep = string.Empty;
         bool selectedRegen = false;

         Point3D llb;
         Point3D urt;
         DBOperation.getWorldBB(federatedId, out llb, out urt);
         Octree.WorldBB = new BoundingBox3D(llb, urt);
         Octree.MaxDepth = DBOperation.OctreeSubdivLevel;
         Vector3D trueNorth = BIMRLUtils.GetProjectTrueNorth(federatedId, ref _refBIMRLCommon);

#if ORACLE
         string sqlStmt = "select elementid, elementtype, geometrybody, IsSolidGeometry from " + DBOperation.formatTabName("BIMRL_ELEMENT", federatedId) + " where geometrybody is not null ";
         if (!string.IsNullOrEmpty(whereCond))
         {
            sqlStmt += " and " + whereCond;
            selectedRegen = true;
         }
         // The following is needed to update the element table with Bbox information
         string sqlStmt3 = "UPDATE " + DBOperation.formatTabName("BIMRL_ELEMENT", federatedId) + " SET GeometryBody_BBOX = :bbox, "
                           + "GeometryBody_BBOX_CENTROID = :cent WHERE ELEMENTID = :eid";
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
         OracleCommand commandUpdBbox = new OracleCommand(sqlStmt3, DBOperation.DBConn);
         command.FetchSize = 20;
         OracleDataReader reader;
#endif
#if POSTGRES
         string sqlStmt = "select elementid, elementtype, geometrybody_geomtype, geometrybody from "
                           + DBOperation.formatTabName("BIMRL_ELEMENT", federatedId) + " where geometrybody is not null ";
         if (!string.IsNullOrEmpty(whereCond))
         {
            sqlStmt += " and " + whereCond;
            selectedRegen = true;
         }
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
         NpgsqlDataReader reader;
#endif

         try
         {
            command.CommandText = "SELECT COUNT(*) FROM " + DBOperation.formatTabName("BIMRL_ELEMENT", federatedId) + " where geometrybody is not null ";
            object rC = command.ExecuteScalar();
            int totalRowCount = Convert.ToInt32(rC.ToString()) * (int)Math.Pow(8, 2);

            currStep = sqlStmt;
            command.CommandText = sqlStmt;

            int sublistCnt = 0;
            List<string> eidUpdList = new List<string>();

#if ORACLE
            commandUpdBbox.CommandText = sqlStmt3;
            commandUpdBbox.Parameters.Clear();

            OracleParameter[] Bbox = new OracleParameter[3];
            Bbox[0] = commandUpdBbox.Parameters.Add("bbox", OracleDbType.Object);
            Bbox[0].Direction = ParameterDirection.Input;
            Bbox[0].UdtTypeName = "MDSYS.SDO_GEOMETRY";
            List<List<SdoGeometry>> bboxListList = new List<List<SdoGeometry>>();
            List<SdoGeometry> bboxList = new List<SdoGeometry>();

            Bbox[1] = commandUpdBbox.Parameters.Add("cent", OracleDbType.Object);
            Bbox[1].Direction = ParameterDirection.Input;
            Bbox[1].UdtTypeName = "MDSYS.SDO_GEOMETRY";
            List<List<SdoGeometry>> centListList = new List<List<SdoGeometry>>();
            List<SdoGeometry> centList = new List<SdoGeometry>();

            Bbox[2] = commandUpdBbox.Parameters.Add("eid", OracleDbType.Varchar2);
            Bbox[2].Direction = ParameterDirection.Input;
            List<List<string>> eidUpdListList = new List<List<string>>();
#endif
#if POSTGRES
            List<Point3D[]> bboxList = new List<Point3D[]>();
            List<Point3D> centList = new List<Point3D>();
            command.Prepare();
#endif
            // end for Bbox

            List<ManualResetEvent> manualEvents = new List<ManualResetEvent>();
            TopoFaceState stateInfo;

            Octree octreeInstance = null;
            if (selectedRegen)
               octreeInstance = new Octree(federatedId, totalRowCount, DBOperation.OctreeSubdivLevel);
            else
               octreeInstance = new Octree(federatedId, totalRowCount, DBOperation.OctreeSubdivLevel, false, true);      // Since it is not selectedRegen, we will rebuild the entire tree, skip Dict regen for this case

            string elemID = string.Empty;

            reader = command.ExecuteReader();
            while (reader.Read())
            {
               elemID = reader.GetString(0);
               string elemTyp = reader.GetString(1);

#if ORACLE
               SdoGeometry sdoGeomData = reader.GetValue(2) as SdoGeometry;
               bool isSolid = true;    // for backward compatibility
               if (!reader.IsDBNull(3))
               {
                  string solid = reader.GetString(3);
                  if (solid.Equals("N"))
                     isSolid = false;
               }
               Polyhedron geom;
               if (!SDOGeomUtils.generate_Polyhedron(sdoGeomData, out geom, isSolid))
                  continue;                                       // if there is something not right, skip the geometry

               // - Update geometry info with BBox information
               SdoGeometry bbox = new SdoGeometry();
               bbox.Dimensionality = 3;
               bbox.LRS = 0;
               bbox.GeometryType = (int)SdoGeometryTypes.GTYPE.POLYGON;
               int gType = bbox.PropertiesToGTYPE();

               double[] arrCoord = new double[6];
               int[] elemInfoArr = { 1, (int)SdoGeometryTypes.ETYPE_SIMPLE.POLYGON_EXTERIOR, 1 };
               arrCoord[0] = geom.boundingBox.LLB.X;
               arrCoord[1] = geom.boundingBox.LLB.Y;
               arrCoord[2] = geom.boundingBox.LLB.Z;
               arrCoord[3] = geom.boundingBox.URT.X;
               arrCoord[4] = geom.boundingBox.URT.Y;
               arrCoord[5] = geom.boundingBox.URT.Z;

               bbox.ElemArrayOfInts = elemInfoArr;
               bbox.OrdinatesArrayOfDoubles = arrCoord;
               bboxList.Add(bbox);

               SdoGeometry centroid = new SdoGeometry();
               centroid.Dimensionality = 3;
               centroid.LRS = 0;
               centroid.GeometryType = (int)SdoGeometryTypes.GTYPE.POINT;
               gType = centroid.PropertiesToGTYPE();
               SdoPoint cent = new SdoPoint();
               cent.XD = geom.boundingBox.Center.X;
               cent.YD = geom.boundingBox.Center.Y;
               cent.ZD = geom.boundingBox.Center.Z;
               centroid.SdoPoint = cent;
               centList.Add(centroid);

               eidUpdList.Add(elemID);

               sublistCnt++;

               // Set 500 records as a threshold for interval commit later on
               if (sublistCnt >= 500)
               {
                  bboxListList.Add(bboxList);
                  centListList.Add(centList);
                  eidUpdListList.Add(eidUpdList);

                  sublistCnt = 0;
                  bboxList = new List<SdoGeometry>();
                  centList = new List<SdoGeometry>();
                  eidUpdList = new List<string>();
               }
               
               // We will skip large buildinglementproxy that has more than 5000 vertices
               bool largeMesh = (string.Compare(elemTyp, "IFCBUILDINGELEMENTPROXY", true) == 0) && geom.Vertices.Count > 5000;

               // There is a problem with concurrent insert into Oracle. For now enable this only for Postgres version
               if ((createFaces && !largeMesh) || (createFaces && selectedRegen))
               {
                  // - Process face information and create consolidated faces and store them into BIMRL_TOPO_FACE table
                  BIMRLGeometryPostProcess processFaces = new BIMRLGeometryPostProcess(elemID, geom, _refBIMRLCommon, federatedId, null, trueNorth);
                  processFaces.simplifyAndMergeFaces();
                  processFaces.insertIntoDB(false);
               }
#endif
#if POSTGRES
               GeometryTypeEnum geomType = reader.GetFieldValue<GeometryTypeEnum>(2);
               string geomStr = reader.GetString(3);
               object geomObj = JsonGeomUtils.generateGeometryFromJson(geomType, geomStr);
               List<Polyhedron> geomList = geomObj as List<Polyhedron>;
               if (geomObj == null || geomList == null || geomList.Count == 0)
                  continue;
               Polyhedron geom = Polyhedron.UnionPolyhedronList(geomList);
               Point3D[] bboxCoords = new Point3D[2];
               bboxCoords[0] = geom.boundingBox.LLB;
               bboxCoords[1] = geom.boundingBox.URT;
               bboxList.Add(bboxCoords);
               centList.Add(geom.boundingBox.Center);
               eidUpdList.Add(elemID);
               sublistCnt++;

               if (createFaces)
               {
                  foreach (Polyhedron lump in geomList)
                  {
                     ManualResetEvent currManualEvent = (new ManualResetEvent(false));
                     manualEvents.Add(currManualEvent);
                     stateInfo = new TopoFaceState(elemID, _refBIMRLCommon, lump, elemTyp, federatedId, currManualEvent, trueNorth);
                     ThreadPool.QueueUserWorkItem(new WaitCallback(BIMRLGeometryPostProcess.ProcessTopoFace), stateInfo);
                  }
               }

               // We will skip large buildinglementproxy that has more than 5000 vertices
               //bool largeMesh = (string.Compare(elemTyp, "IFCBUILDINGELEMENTPROXY", true) == 0) && geom.Vertices.Count > 5000;
               //if ((createFaces && !largeMesh) || (createFaces && selectedRegen))
               //{
               //   // - Process face information and create consolidated faces and store them into BIMRL_TOPO_FACE table
               //   BIMRLGeometryPostProcess processFaces = new BIMRLGeometryPostProcess(elemID, geom, _refBIMRLCommon, federatedId, null, trueNorth);
               //   processFaces.simplifyAndMergeFaces();
               //   processFaces.insertIntoDB(false);
               //}
#endif

               if (createSpIdx)
                  octreeInstance.ComputeOctree(elemID, geom);
            }

            reader.Close();
            reader.Dispose();

            // Wait for all the threads to complete the work
#if POSTGRES
            var wait = true;
            while (wait)
            {
               if (manualEvents.Count > 0)
               {
                  WaitHandle.WaitAll(manualEvents.Take(60).ToArray());
                  manualEvents.RemoveRange(0, manualEvents.Count > 59 ? 60 : manualEvents.Count);
                  wait = manualEvents.Any();
               }
               else
                  break;
            }
#endif
            DBOperation.commitTransaction();
            command.Dispose();
            BIMRLGeometryPostProcess.ResetFaceIdCache();

            if (createSpIdx)
            {
               // Truncate the table first before reinserting the records
               FederatedModelInfo fedModel = DBOperation.getFederatedModelByID(federatedId);
               if (DBOperation.DBUserID.Equals(fedModel.FederatedID))
#if ORACLE
                  DBOperation.executeSingleStmt("TRUNCATE TABLE " + DBOperation.formatTabName("BIMRL_SPATIALINDEX", federatedId));
               else
                  DBOperation.executeSingleStmt("DELETE FROM " + DBOperation.formatTabName("BIMRL_SPATIALINDEX"));
#endif
#if POSTGRES
                  DBOperation.ExecuteNonQueryWithTrans2("TRUNCATE TABLE " + DBOperation.formatTabName("BIMRL_SPATIALINDEX", federatedId), commit: true);
               else
                  DBOperation.ExecuteNonQueryWithTrans2("DELETE FROM " + DBOperation.formatTabName("BIMRL_SPATIALINDEX"), commit: true);
#endif
               collectSpatialIndexAndInsert(octreeInstance, federatedId);
            }

#if ORACLE
            if (sublistCnt > 0)
            {
               bboxListList.Add(bboxList);
               centListList.Add(centList);
               eidUpdListList.Add(eidUpdList);
            }

            for (int i = 0; i < eidUpdListList.Count; i++)
            {
               Bbox[0].Value = bboxListList[i].ToArray();
               Bbox[0].Size = bboxListList[i].Count;
               Bbox[1].Value = centListList[i].ToArray();
               Bbox[1].Size = centListList[i].Count;
               Bbox[2].Value = eidUpdListList[i].ToArray();
               Bbox[2].Size = eidUpdListList[i].Count;

               commandUpdBbox.ArrayBindCount = eidUpdListList[i].Count;    // No of values in the array to be inserted
               int commandStatus = commandUpdBbox.ExecuteNonQuery();
               DBOperation.commitTransaction();
            }
            
            if (!string.IsNullOrEmpty(whereCond) && createSpIdx)
            {
               command.CommandText = "UPDATE BIMRL_FEDERATEDMODEL SET MAXOCTREELEVEL=" + Octree.MaxDepth.ToString() + " WHERE FEDERATEDID=" + federatedId.ToString();
               command.ExecuteNonQuery();
               DBOperation.commitTransaction();
            }
         }
         catch (OracleException e)

#endif
#if POSTGRES
            // The following is needed to update the element table with Bbox information
            string sqlStmt3 = "UPDATE " + DBOperation.formatTabName("BIMRL_ELEMENT", federatedId) + " SET GeometryBody_BBOX = @bbox, "
                              + "GeometryBody_BBOX_CENTROID = @cent WHERE ELEMENTID = @eid";
            NpgsqlCommand commandUpdBbox = new NpgsqlCommand(sqlStmt3, DBOperation.DBConn);

            //commandUpdBbox.Prepare();

            for (int i = 0; i < bboxList.Count; ++i)
            {
               commandUpdBbox.Parameters.Clear();
               Point3D[] bbox = bboxList[i];
               commandUpdBbox.Parameters.AddWithValue("@bbox", NpgsqlDbType.Array | NpgsqlDbType.Composite, bbox);
               Point3D centr = centList[i];
               commandUpdBbox.Parameters.AddWithValue("@cent", centr);
               commandUpdBbox.Parameters.AddWithValue("@eid", eidUpdList[i]);
               int commandStatus = commandUpdBbox.ExecuteNonQuery();
            }
            DBOperation.commitTransaction();
            commandUpdBbox.Dispose();

            if (!string.IsNullOrEmpty(whereCond) && createSpIdx)
            {
               DBOperation.ExecuteNonQueryWithTrans2("UPDATE BIMRL_FEDERATEDMODEL SET MAXOCTREELEVEL=" + Octree.MaxDepth.ToString() + " WHERE FEDERATEDID=" + federatedId.ToString());
            }
         }
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Read Error - " + e.Message + "\n\t" + currStep;
            _refBIMRLCommon.StackPushError(excStr);
         }
         catch (SystemException e)
         {
            string excStr = "%%Read Error - " + e.Message + "\n\t" + currStep;
            _refBIMRLCommon.StackPushError(excStr);
            throw;
         }
         command.Dispose();
      }

      public void createFacesFromBIMRLElement(int federatedId, string whereCond)
      {
         DBOperation.beginTransaction();
         string currStep = string.Empty;
         List<ManualResetEvent> manualEvents = new List<ManualResetEvent>();
         TopoFaceState stateInfo;
         Vector3D trueNorth = BIMRLUtils.GetProjectTrueNorth(federatedId, ref _refBIMRLCommon);

#if ORACLE
         SdoGeometry sdoGeomData = new SdoGeometry();

         string sqlStmt = "select elementid, geometrybody, issolidgeometry from " + DBOperation.formatTabName("BIMRL_ELEMENT", federatedId) + " where geometrybody is not null ";
         if (!string.IsNullOrEmpty(whereCond))
            sqlStmt += " and " + whereCond;

         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
         command.FetchSize = 20;
         OracleDataReader reader;
#endif
#if POSTGRES
         string sqlStmt = "select elementid, geometrybody_geomtype, geometrybody from " + DBOperation.formatTabName("BIMRL_ELEMENT", federatedId) + " where geometrybody is not null ";
         if (!string.IsNullOrEmpty(whereCond))
            sqlStmt += " and " + whereCond;

         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
         command.Prepare();
         NpgsqlDataReader reader;
#endif
         currStep = sqlStmt;
         try
         {
            reader = command.ExecuteReader();

            while (reader.Read())
            {
               string elemID = reader.GetString(0);

#if ORACLE
               sdoGeomData = reader.GetValue(1) as SdoGeometry;
               string solid = reader.GetString(2);
               bool isSolid = true;
               if (solid.Equals("N"))
                  isSolid = false;

               Polyhedron geom;
               if (!SDOGeomUtils.generate_Polyhedron(sdoGeomData, out geom, isSolid))
                  continue;                                       
               
               // if there is something not right, skip the geometry
               {
                  //// - Process face information and create consolidated faces and store them into BIMRL_TOPO_FACE table
                  BIMRLGeometryPostProcess processFaces = new BIMRLGeometryPostProcess(elemID, geom, _refBIMRLCommon, federatedId, null);
                  processFaces.simplifyAndMergeFaces();
                  processFaces.insertIntoDB(false);
                  //ManualResetEvent currManualEvent = (new ManualResetEvent(false));
                  //manualEvents.Add(currManualEvent);
                  //stateInfo = new TopoFaceState(elemID, _refBIMRLCommon, geom, "", federatedId, currManualEvent, trueNorth);
                  //ThreadPool.QueueUserWorkItem(new WaitCallback(BIMRLGeometryPostProcess.ProcessTopoFace), stateInfo);
               }
            }
            reader.Dispose();
            // Wait for all threads to complete
            //WaitHandle.WaitAll(manualEvents.ToArray());
            //var wait = true;
            //while (wait)
            //{
            //   WaitHandle.WaitAll(manualEvents.Take(60).ToArray());
            //   manualEvents.RemoveRange(0, manualEvents.Count > 59 ? 60 : manualEvents.Count);
            //   wait = manualEvents.Any();
            //}
         }
         catch (OracleException e)
#endif
#if POSTGRES
               GeometryTypeEnum geomType = reader.GetFieldValue<GeometryTypeEnum>(1);
               string geomStr = reader.GetString(2);

               object geomObj = JsonGeomUtils.generateGeometryFromJson(geomType, geomStr);
               List<Polyhedron> polyHList = geomObj as List<Polyhedron>;
               if (geomObj == null || polyHList == null || polyHList.Count == 0)
                  continue;

               //Polyhedron geom = Polyhedron.UnionPolyhedronList(polyHList);
               foreach (Polyhedron geom in polyHList)
               {
                  //// - Process face information and create consolidated faces and store them into BIMRL_TOPO_FACE table
                  //BIMRLGeometryPostProcess processFaces = new BIMRLGeometryPostProcess(elemID, geom, _refBIMRLCommon, federatedId, null, trueNorth);
                  //processFaces.simplifyAndMergeFaces();
                  //processFaces.insertIntoDB(false);
                  ManualResetEvent currManualEvent = (new ManualResetEvent(false));
                  manualEvents.Add(currManualEvent);
                  stateInfo = new TopoFaceState(elemID, _refBIMRLCommon, geom, "", federatedId, currManualEvent, trueNorth);
                  ThreadPool.QueueUserWorkItem(new WaitCallback(BIMRLGeometryPostProcess.ProcessTopoFace), stateInfo);
               }
            }
            reader.Dispose();

            var wait = true;
            while (wait)
            {
               if (manualEvents.Count > 0)
               {
                  WaitHandle.WaitAll(manualEvents.Take(60).ToArray());
                  manualEvents.RemoveRange(0, manualEvents.Count > 59 ? 60 : manualEvents.Count);
                  wait = manualEvents.Any();
               }
               else
                  break;
            }
            BIMRLGeometryPostProcess.ResetFaceIdCache();
         }
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Read Error - " + e.Message + "\n\t" + currStep;
            _refBIMRLCommon.StackPushError(excStr);

         }
         catch (SystemException e)
         {
            string excStr = "%%Read Error - " + e.Message + "\n\t" + currStep;
            _refBIMRLCommon.StackPushError(excStr);
            throw;
         }

         command.Dispose();
      }

      public void computeMajorAxes(int federatedId, string whereCond)
      {
         DBOperation.beginTransaction();
         string currStep = string.Empty;
         Vector3D trueNorth = BIMRLUtils.GetProjectTrueNorth(federatedId, ref _refBIMRLCommon);

#if ORACLE
         string sqlStmt = "select elementid, geometrybody from " + DBOperation.formatTabName("BIMRL_ELEMENT", federatedId) + " where geometrybody is not null ";
         if (!string.IsNullOrEmpty(whereCond))
            sqlStmt += " and " + whereCond;

         SdoGeometry sdoGeomData = new SdoGeometry();

         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
         command.FetchSize = 20;
         OracleDataReader reader;
#endif
#if POSTGRES
         string sqlStmt = "select elementid, geometrybody_geomtype, geometrybody from " + DBOperation.formatTabName("BIMRL_ELEMENT", federatedId) + " where geometrybody is not null ";
         if (!string.IsNullOrEmpty(whereCond))
            sqlStmt += " and " + whereCond;

         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
         NpgsqlDataReader reader;
#endif
         try
         {
            currStep = sqlStmt;
            reader = command.ExecuteReader();
            while (reader.Read())
            {
               string elemID = reader.GetString(0);
#if ORACLE
               sdoGeomData = reader.GetValue(1) as SdoGeometry;

               Polyhedron geom;
               if (!SDOGeomUtils.generate_Polyhedron(sdoGeomData, out geom))
                  continue;                                       // if there is something not right, skip the geometry
#endif
#if POSTGRES
               GeometryTypeEnum geomType = reader.GetFieldValue<GeometryTypeEnum>(1);
               string geomStr = reader.GetString(2);

               object geomObj = JsonGeomUtils.generateGeometryFromJson(geomType, geomStr);
               List<Polyhedron> polyHList = geomObj as List<Polyhedron>;
               if (geomObj == null || polyHList == null || polyHList.Count == 0)
                  continue;

               Polyhedron geom = Polyhedron.UnionPolyhedronList(polyHList);
#endif
               // - Process face information and create consolidated faces and store them into BIMRL_TOPO_FACE table
               BIMRLGeometryPostProcess majorAxes = new BIMRLGeometryPostProcess(elemID, geom, _refBIMRLCommon, federatedId, null, trueNorth);
               majorAxes.deriveMajorAxes();
            }
            reader.Dispose();
         }
#if ORACLE
         catch (OracleException e)
#endif
#if POSTGRES
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Read Error - " + e.Message + "\n\t" + currStep;
            _refBIMRLCommon.StackPushError(excStr);
         }
         catch (SystemException e)
         {
            string excStr = "%%Read Error - " + e.Message + "\n\t" + currStep;
            _refBIMRLCommon.StackPushError(excStr);
            throw;
         }

         command.Dispose();
      }

      public void collectSpatialIndexAndInsert(Octree octreeInstance, int federatedId)
      {
         int XMin;
         int YMin;
         int ZMin;
         int XMax;
         int YMax;
         int ZMax;

#if ORACLE
         string sqlStmt = "INSERT INTO " + DBOperation.formatTabName("BIMRL_SPATIALINDEX", federatedId) + " (ELEMENTID, CELLID, XMinBound, YMinBound, ZMinBound, XMaxBound, YMaxBound, ZMaxBound, Depth) "
                              + "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)";
         OracleCommand commandIns = new OracleCommand(sqlStmt, DBOperation.DBConn);

         OracleParameter[] spatialIdx = new OracleParameter[9]; 
         spatialIdx[0] = commandIns.Parameters.Add("1", OracleDbType.Varchar2);
         spatialIdx[0].Direction = ParameterDirection.Input;
         spatialIdx[1] = commandIns.Parameters.Add("2", OracleDbType.Varchar2);
         spatialIdx[1].Direction = ParameterDirection.Input;
         spatialIdx[2] = commandIns.Parameters.Add("3", OracleDbType.Int32);
         spatialIdx[2].Direction = ParameterDirection.Input;
         spatialIdx[3] = commandIns.Parameters.Add("4", OracleDbType.Int32);
         spatialIdx[3].Direction = ParameterDirection.Input;
         spatialIdx[4] = commandIns.Parameters.Add("5", OracleDbType.Int32);
         spatialIdx[4].Direction = ParameterDirection.Input;
         spatialIdx[5] = commandIns.Parameters.Add("6", OracleDbType.Int32);
         spatialIdx[5].Direction = ParameterDirection.Input;
         spatialIdx[6] = commandIns.Parameters.Add("7", OracleDbType.Int32);
         spatialIdx[6].Direction = ParameterDirection.Input;
         spatialIdx[7] = commandIns.Parameters.Add("8", OracleDbType.Int32);
         spatialIdx[7].Direction = ParameterDirection.Input;
         spatialIdx[8] = commandIns.Parameters.Add("9", OracleDbType.Int32);
         spatialIdx[8].Direction = ParameterDirection.Input;

         int initArraySize = 1000;
         List<string> elementIDList = new List<string>(initArraySize);
         List<string> cellIDStrList = new List<string>(initArraySize);

         List<int> XMinB = new List<int>(initArraySize);
         List<int> YMinB = new List<int>(initArraySize);
         List<int> ZMinB = new List<int>(initArraySize);
         List<int> XMaxB = new List<int>(initArraySize);
         List<int> YMaxB = new List<int>(initArraySize);
         List<int> ZMaxB = new List<int>(initArraySize);
         List<int> depthList = new List<int>(initArraySize);


         int recCount = 0;

         foreach (KeyValuePair<UInt64, Octree.CellData> dictEntry in octreeInstance.MasterDict)
         {
            CellID64 cellID = new CellID64(dictEntry.Key);
            string cellIDstr = cellID.ToString();
            CellID64.getCellIDComponents(cellID, out XMin, out YMin, out ZMin, out XMax, out YMax, out ZMax);
            int cellLevel = CellID64.getLevel(cellID);

            if (dictEntry.Value.data != null && dictEntry.Value.nodeType != 0)
            {
               foreach (int tupEID in dictEntry.Value.data)
               {
                  List<int> cBound = new List<int>();

                  //ElementID eID = new ElementID(Octree.getElementIDByIndex(tupEID));
                  //elementIDList.Add(eID.ElementIDString);
                  string eidStr = ElementID.GetElementIDstrFromKey(Octree.getElementIDByIndex(tupEID));
                  elementIDList.Add(eidStr);
                  cellIDStrList.Add(cellIDstr);

                  //CellID64.getCellIDComponents(cellID, out XMin, out YMin, out ZMin, out XMax, out YMax, out ZMax);
                  XMinB.Add(XMin);
                  YMinB.Add(YMin);
                  ZMinB.Add(ZMin);
                  XMaxB.Add(XMax);
                  YMaxB.Add(YMax);
                  ZMaxB.Add(ZMax);
                  //depthList.Add(CellID64.getLevel(cellID));
                  depthList.Add(cellLevel);
               }
            }

            try 
            {
               recCount = elementIDList.Count;
               if (recCount >= initArraySize)
               {
                  spatialIdx[0].Value = elementIDList.ToArray();
                  spatialIdx[0].Size = recCount;
                  spatialIdx[1].Value = cellIDStrList.ToArray();
                  spatialIdx[1].Size = recCount;
                  spatialIdx[2].Value = XMinB.ToArray();
                  spatialIdx[2].Size = recCount;

                  spatialIdx[3].Value = YMinB.ToArray();
                  spatialIdx[3].Size = recCount;

                  spatialIdx[4].Value = ZMinB.ToArray();
                  spatialIdx[4].Size = recCount;

                  spatialIdx[5].Value = XMaxB.ToArray();
                  spatialIdx[5].Size = recCount;

                  spatialIdx[6].Value = YMaxB.ToArray();
                  spatialIdx[6].Size = recCount;

                  spatialIdx[7].Value = ZMaxB.ToArray();
                  spatialIdx[7].Size = recCount;

                  spatialIdx[8].Value = depthList.ToArray();
                  spatialIdx[8].Size = recCount;

                  commandIns.ArrayBindCount = recCount;

                  int commandStatus = commandIns.ExecuteNonQuery();
                  DBOperation.commitTransaction();

                  elementIDList.Clear();
                  cellIDStrList.Clear();
                  XMinB.Clear();
                  YMinB.Clear();
                  ZMinB.Clear();
                  XMaxB.Clear();
                  YMaxB.Clear();
                  ZMaxB.Clear();
                  depthList.Clear();
               }
            }
            catch (OracleException e)
            {
               string excStr = "%%Insert Spatial Index Error - " + e.Message + "\n\t" ;
               _refBIMRLCommon.StackPushIgnorableError(excStr);
            }
            catch (SystemException e)
            {
               string excStr = "%%Insert Spatial Index Error - " + e.Message + "\n\t";
               _refBIMRLCommon.StackPushError(excStr);
               throw;
            }
         }

         // At last if there are entries in the list, insert them
         try
         {
            recCount = elementIDList.Count;
            if (recCount > 0)
            {
               spatialIdx[0].Value = elementIDList.ToArray();
               spatialIdx[0].Size = recCount;
               spatialIdx[1].Value = cellIDStrList.ToArray();
               spatialIdx[1].Size = recCount;
               spatialIdx[2].Value = XMinB.ToArray();
               spatialIdx[2].Size = recCount;

               spatialIdx[3].Value = YMinB.ToArray();
               spatialIdx[3].Size = recCount;

               spatialIdx[4].Value = ZMinB.ToArray();
               spatialIdx[4].Size = recCount;

               spatialIdx[5].Value = XMaxB.ToArray();
               spatialIdx[5].Size = recCount;

               spatialIdx[6].Value = YMaxB.ToArray();
               spatialIdx[6].Size = recCount;

               spatialIdx[7].Value = ZMaxB.ToArray();
               spatialIdx[7].Size = recCount;

               spatialIdx[8].Value = depthList.ToArray();
               spatialIdx[8].Size = recCount;

               commandIns.ArrayBindCount = recCount;

               int commandStatus = commandIns.ExecuteNonQuery();
               DBOperation.commitTransaction();

               elementIDList.Clear();
               cellIDStrList.Clear();
               XMinB.Clear();
               YMinB.Clear();
               ZMinB.Clear();
               XMaxB.Clear();
               YMaxB.Clear();
               ZMaxB.Clear();
               depthList.Clear();
            }
         }
         catch (OracleException e)
         {
            string excStr = "%%Insert Spatial Index Error - " + e.Message + "\n\t";
               _refBIMRLCommon.StackPushIgnorableError(excStr);
         }
         catch (SystemException e)
         {
               string excStr = "%%Insert Spatial Index Error - " + e.Message + "\n\t";
               _refBIMRLCommon.StackPushError(excStr);
               throw;
         }
         commandIns.Dispose();
#endif
#if POSTGRES
         string sqlStmt = "INSERT INTO " + DBOperation.formatTabName("BIMRL_SPATIALINDEX", federatedId) + " (ELEMENTID, CELLID, XMinBound, YMinBound, ZMinBound, XMaxBound, YMaxBound, ZMaxBound, Depth) "
                              + "VALUES (@eid, @cid, @xmin, @ymin, @zmin, @xmax, @ymax, @zmax, @dep)";

         NpgsqlConnection arbConn = DBOperation.arbitraryConnection();
         NpgsqlCommand commandIns = new NpgsqlCommand(sqlStmt, arbConn);
         NpgsqlTransaction arbTrans = arbConn.BeginTransaction();
         commandIns.Parameters.Add("@eid", NpgsqlDbType.Varchar);
         commandIns.Parameters.Add("@cid", NpgsqlDbType.Varchar);
         commandIns.Parameters.Add("@xmin", NpgsqlDbType.Integer);
         commandIns.Parameters.Add("@ymin", NpgsqlDbType.Integer);
         commandIns.Parameters.Add("@zmin", NpgsqlDbType.Integer);
         commandIns.Parameters.Add("@xmax", NpgsqlDbType.Integer);
         commandIns.Parameters.Add("@ymax", NpgsqlDbType.Integer);
         commandIns.Parameters.Add("@zmax", NpgsqlDbType.Integer);
         commandIns.Parameters.Add("@dep", NpgsqlDbType.Integer);
         commandIns.Prepare();

         int recInsCount = 0;
         foreach (KeyValuePair<UInt64, Octree.CellData> dictEntry in octreeInstance.MasterDict)
         {
            CellID64 cellID = new CellID64(dictEntry.Key);
            string cellIDstr = cellID.ToString();
            CellID64.getCellIDComponents(cellID, out XMin, out YMin, out ZMin, out XMax, out YMax, out ZMax);
            int cellLevel = CellID64.getLevel(cellID);

            if (dictEntry.Value.data != null && dictEntry.Value.nodeType != 0)
            {
               foreach (int tupEID in dictEntry.Value.data)
               {
                  //ElementID eID = new ElementID(Octree.getElementIDByIndex(tupEID));
                  //string elemID = eID.ElementIDString;
                  string elemID = ElementID.GetElementIDstrFromKey(Octree.getElementIDByIndex(tupEID));

                  commandIns.Parameters["@eid"].Value = elemID;
                  commandIns.Parameters["@cid"].Value = cellIDstr;
                  commandIns.Parameters["@xmin"].Value = XMin;
                  commandIns.Parameters["@ymin"].Value = YMin;
                  commandIns.Parameters["@zmin"].Value = ZMin;
                  commandIns.Parameters["@xmax"].Value = XMax;
                  commandIns.Parameters["@ymax"].Value = YMax;
                  commandIns.Parameters["@zmax"].Value = ZMax;
                  commandIns.Parameters["@dep"].Value = cellLevel;

                  try
                  {
                     arbTrans.Save("insSavePoint");
                     int commandStatus = commandIns.ExecuteNonQuery();
                     recInsCount++;
                     if (recInsCount > DBOperation.commitInterval)
                     {
                        arbTrans.Commit();
                        recInsCount = 0;
                        arbTrans = arbConn.BeginTransaction();
                     }
                     else
                        arbTrans.Release("insSavePoint");
                  }
                  catch (NpgsqlException e)
                  {
                     string excStr = "%%Insert Spatial Index Error - " + e.Message + "\n\t";
                     _refBIMRLCommon.StackPushIgnorableError(excStr);
                     arbTrans.Rollback("insSavePoint");
                  }
                  catch (SystemException e)
                  {
                     string excStr = "%%Insert Spatial Index Error - " + e.Message + "\n\t";
                     _refBIMRLCommon.StackPushError(excStr);
                     throw;
                  }
               }
            }
         }

         if (recInsCount > 0)
            arbTrans.Commit();

         commandIns.Dispose();
         arbConn.Close();
#endif
      }

   }
}
