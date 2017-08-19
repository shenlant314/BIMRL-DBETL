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
using System.Diagnostics;
using Xbim.ModelGeometry.Scene;
using Xbim.ModelGeometry.Scene.Extensions;
using Xbim.Ifc;
using Xbim.Ifc4.Interfaces;
using Xbim.Common.Geometry;
using Xbim.Ifc4.MeasureResource;
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
using Newtonsoft.Json;

namespace BIMRL
{
   public class BIMRLElement
   {
      IfcStore _model;
      BIMRLCommon _refBIMRLCommon;
      bool isIfc2x3 = false;
      bool needConversion = true;

      struct UnitSetting
      {
         public string unitName;
         public string unitType;    // "METRIC" or "IMPERIAL"
         public double conversionFactor;
         public string unitOfMeasure;
      }

      public BIMRLElement(IfcStore m, BIMRLCommon refBIMRLCommon)
      {
         _model = m;
         _refBIMRLCommon = refBIMRLCommon;
         if (m.IfcSchemaVersion == Xbim.Common.Step21.IfcSchemaVersion.Ifc2X3)
            isIfc2x3 = true;
         if (MathUtils.equalTol(_model.ModelFactors.LengthToMetresConversionFactor, 1.0))
            needConversion = false;    // If the unit is already in Meter, no conversion needed
      }

      private void processGeometries()
      {
         DBOperation.beginTransaction();
         string currStep = string.Empty;
         Xbim3DModelContext context = new Xbim3DModelContext(_model);
         ConcurrentDictionary<int, XbimMatrix3D> RelTransform = new ConcurrentDictionary<int, XbimMatrix3D>();

         int commandStatus = -1;
         int currInsertCount = 0;

#if ORACLE
         OracleCommand command = new OracleCommand(" ", DBOperation.DBConn);
#endif
#if POSTGRES
         NpgsqlCommand command = new NpgsqlCommand(" ", DBOperation.DBConn);

#endif
         XbimMatrix3D m3D = new XbimMatrix3D();

         foreach (int productLabel in _refBIMRLCommon.insEntityLabelList)
         {
            //IEnumerable<XbimGeometryData> geomDataList = _model.GetGeometryData(productLabel, XbimGeometryType.TriangulatedMesh);
            IIfcProduct product = _model.Instances[productLabel] as IIfcProduct;
            IEnumerable<XbimShapeInstance> shapeInstances;

            if (product is IIfcFeatureElement)
               shapeInstances = context.ShapeInstancesOf(product).Where(x => x.RepresentationType == XbimGeometryRepresentationType.OpeningsAndAdditionsOnly);
            else
               shapeInstances = context.ShapeInstancesOf(product).Where(x => x.RepresentationType == XbimGeometryRepresentationType.OpeningsAndAdditionsIncluded);
            //if (shapeInstances == null)
            //   continue;

            bool trfOnly = false;
            XbimMatrix3D newRelTrf = XbimMatrix3D.Identity;
            if (shapeInstances.Count() == 0)
            {
               //continue;         // SKip if the product has no geometry
               // If the body is null, we will still process the other information, mainly the transformation information
               trfOnly = true;
               IIfcLocalPlacement locPlacement = product.ObjectPlacement as IIfcLocalPlacement;
               if (locPlacement == null)
                  continue;
               IIfcLocalPlacement refPlacement = locPlacement.PlacementRelTo as IIfcLocalPlacement;
               XbimMatrix3D refTransform;
               if (refPlacement == null || !RelTransform.TryGetValue(refPlacement.EntityLabel, out refTransform))
                  refTransform = XbimMatrix3D.Identity;

               IIfcAxis2Placement3D placement3D = locPlacement.RelativePlacement as IIfcAxis2Placement3D;
               XbimMatrix3D prodTrf = XbimMatrix3D.Identity;
               IList<XbimVector3D> pVec = null;
               if (placement3D != null)
               {
                  pVec = placement3D.P;
                  prodTrf.M11 = pVec[0].X;
                  prodTrf.M12 = pVec[0].Y;
                  prodTrf.M13 = pVec[0].Z;
                  prodTrf.M21 = pVec[1].X;
                  prodTrf.M22 = pVec[1].Y;
                  prodTrf.M23 = pVec[1].Z;
                  prodTrf.M31 = pVec[2].X;
                  prodTrf.M32 = pVec[2].Y;
                  prodTrf.M33 = pVec[2].Z;

                  IIfcCartesianPoint p = placement3D.Location;
                  prodTrf.OffsetX = p.X * _model.ModelFactors.LengthToMetresConversionFactor;
                  prodTrf.OffsetY = p.Y * _model.ModelFactors.LengthToMetresConversionFactor;
                  prodTrf.OffsetZ = p.Z * _model.ModelFactors.LengthToMetresConversionFactor;
               }
               else
               {
                  IIfcAxis2Placement2D placement2D = locPlacement.RelativePlacement as IIfcAxis2Placement2D;
                  if (placement2D == null)
                     continue;
                  pVec = placement2D.P;
                  prodTrf.M11 = pVec[0].X;
                  prodTrf.M12 = pVec[0].Y;
                  prodTrf.M13 = pVec[0].Z;
                  prodTrf.M21 = pVec[1].X;
                  prodTrf.M22 = pVec[1].Y;
                  prodTrf.M23 = pVec[1].Z;
                  prodTrf.M31 = 0;
                  prodTrf.M32 = 0;
                  prodTrf.M33 = 0;

                  IIfcCartesianPoint p = placement2D.Location;
                  prodTrf.OffsetX = p.X * _model.ModelFactors.LengthToMetresConversionFactor;
                  prodTrf.OffsetY = p.Y * _model.ModelFactors.LengthToMetresConversionFactor;
                  prodTrf.OffsetZ = 0;
               }

               newRelTrf = XbimMatrix3D.Multiply(refTransform, prodTrf);
               if (!RelTransform.ContainsKey(locPlacement.EntityLabel))
                  RelTransform.TryAdd(locPlacement.EntityLabel, newRelTrf);
            }

            string prodGuid = _refBIMRLCommon.guidLineNoMapping_Getguid(BIMRLProcessModel.currModelID, productLabel);
            // foreach (var geomData in _model.GetGeometryData(XbimGeometryType.TriangulatedMesh))

            //if (geomDataList.Count() == 0)
            //    continue;                   // no geometry for this product

#if ORACLE
            int startingOffset = 0;
            List<int> elemInfoArr = new List<int>();
            List<double> arrCoord = new List<double>();
#endif
#if POSTGRES
            IList<Polyhedron> ProdGeometries = new List<Polyhedron>();
#endif
            double totalSurfArea = 0.0;

            //foreach (XbimGeometryData geomData in geomDataList)
            foreach (XbimShapeInstance shapeInst in shapeInstances)
            {
               //m3D = geomData.Transform;
               XbimMeshGeometry3D prodGeom = new XbimMeshGeometry3D();
               IXbimShapeGeometryData shapeGeom = context.ShapeGeometry(shapeInst.ShapeGeometryLabel);
               XbimModelExtensions.Read(prodGeom, shapeGeom.ShapeData, shapeInst.Transformation);
               m3D = shapeInst.Transformation;

               //m3D = XbimMatrix3D.FromArray(geomData.DataArray2);      // Xbim 3.0 removes Transform property!
               //     XbimTriangulatedModelStream triangleStream = new XbimTriangulatedModelStream(geomData.ShapeData);
               //     XbimMeshGeometry3D builder = new XbimMeshGeometry3D();
               //     triangleStream.BuildWithNormals(builder, m3D);  //This reads only the Vertices and triangle indexes, other modes of Build can read other info (e.g. BuildWithNormal, or BuildPNI)

#if ORACLE
               elemInfoArr.Add(startingOffset + 1);     // The first three tuple defines the geometry as solid (1007) starting at position offset 1
               elemInfoArr.Add((int)SdoGeometryTypes.ETYPE_COMPOUND.SOLID);
               elemInfoArr.Add(1);
               elemInfoArr.Add(startingOffset + 1);     // The second three tuple defines that the solid is formed by an exrternal surface (1006) starting at position offset 1
               elemInfoArr.Add((int)SdoGeometryTypes.ETYPE_COMPOUND.SURFACE_EXTERIOR);
               elemInfoArr.Add(prodGeom.TriangleIndexCount / 3); // no. of the (triangle) faces that follows
               int header = startingOffset + 6;
#endif
#if POSTGRES
               IList<Face3D> polyHFaces = new List<Face3D>();
#endif

               for (int noTr = 0; noTr < prodGeom.TriangleIndexCount / 3; noTr++)
               {
                  // int arrPos = header + noTr * 3;       // ElemInfoArray uses 3 members in the long array

#if ORACLE
                  elemInfoArr.Add(startingOffset + noTr * 3 * 4 + 1);  // offset position
                  elemInfoArr.Add((int)SdoGeometryTypes.ETYPE_SIMPLE.POLYGON_EXTERIOR);   // This three tupple defines a face (triangular face) that is made up of 3 points/vertices
                  elemInfoArr.Add(1);
#endif

                  // SDO requires the points to be closed, i.e. for triangle, we will need 4 points: v1, v2, v3, v1
                  XbimPoint3D vOrig = new XbimPoint3D();
                  //XbimPoint3D v0 = new XbimPoint3D();
                  Point3D v0 = new Point3D();
                  IList<Point3D> triangleCoords = new List<Point3D>();
                  for (int i = 0; i < 4; i++)
                  {
                     if (i < 3)
                     {
                        vOrig = prodGeom.Positions[prodGeom.TriangleIndices[noTr * 3 + i]];

                        Point3D v = new Point3D(vOrig.X, vOrig.Y, vOrig.Z);
                        if (needConversion)
                        {
                           v.X = v.X * _model.ModelFactors.LengthToMetresConversionFactor;          // vertex i
                           v.Y = v.Y * _model.ModelFactors.LengthToMetresConversionFactor;
                           v.Z = v.Z * _model.ModelFactors.LengthToMetresConversionFactor;
                        }
                        triangleCoords.Add(v);
#if ORACLE
                        arrCoord.Add(v.X);          // vertex i
                        arrCoord.Add(v.Y);
                        arrCoord.Add(v.Z);

                        // Keep the first point to close the triangle at the end
                        if (i==0)
                        {
                           v0.X = v.X;
                           v0.Y = v.Y;
                           v0.Z = v.Z;
                        }
#endif

                        // Evaluate each point to calculate min bounding box of the entire (federated) model
                        if (v.X < _refBIMRLCommon.LLB_X)
                           _refBIMRLCommon.LLB_X = v.X;
                        else if (v.X > _refBIMRLCommon.URT_X)
                           _refBIMRLCommon.URT_X = v.X;

                        if (v.Y < _refBIMRLCommon.LLB_Y)
                           _refBIMRLCommon.LLB_Y = v.Y;
                        else if (v.Y > _refBIMRLCommon.URT_Y)
                           _refBIMRLCommon.URT_Y = v.Y;

                        if (v.Z < _refBIMRLCommon.LLB_Z)
                           _refBIMRLCommon.LLB_Z = v.Z;
                        else if (v.Z > _refBIMRLCommon.URT_Z)
                           _refBIMRLCommon.URT_Z = v.Z;
                     }
                     else
                     {
                        double surfaceArea = CalculateAreaOfTriangle(triangleCoords);
                        totalSurfArea += surfaceArea;
#if ORACLE
                        // Close the polygon with the starting point (i=0)
                        arrCoord.Add(v0.X);
                        arrCoord.Add(v0.Y);
                        arrCoord.Add(v0.Z);
#endif
                     }
                  }
#if POSTGRES
                  Face3D face = new Face3D(triangleCoords.ToList());
                  polyHFaces.Add(face);
#endif
               }
#if ORACLE
               startingOffset = startingOffset + prodGeom.TriangleIndexCount * 4;
               //polyHStartingOffset = currFVindex + 3;
#endif
#if POSTGRES
               Polyhedron pH = new Polyhedron(polyHFaces.ToList());
               ProdGeometries.Add(pH);
#endif
            }

#if ORACLE
            SdoGeometry sdoGeomData = new SdoGeometry();
            int gType = 0;
            if (!trfOnly)
            {
               // Assume solid only for now
               sdoGeomData.Dimensionality = 3;
               sdoGeomData.LRS = 0;
               if (shapeInstances.Count() == 1)
                  sdoGeomData.GeometryType = (int)SdoGeometryTypes.GTYPE.SOLID;
               else
                  sdoGeomData.GeometryType = (int)SdoGeometryTypes.GTYPE.MULTISOLID;
               gType = sdoGeomData.PropertiesToGTYPE();

               sdoGeomData.ElemArrayOfInts = elemInfoArr.ToArray();
               sdoGeomData.OrdinatesArrayOfDoubles = arrCoord.ToArray();
            }
#endif
#if POSTGRES
            if (!trfOnly)
            {

            }
#endif

            if (!string.IsNullOrEmpty(prodGuid))
            {
               // Found match, update the table with geometry data
#if ORACLE
               string sqlStmt = "update " + DBOperation.formatTabName("BIMRL_ELEMENT") + " set GEOMETRYBODY=:1, TRANSFORM_COL1=:2, TRANSFORM_COL2=:3, TRANSFORM_COL3=:4, TRANSFORM_COL4=:5, TOTAL_SURFACE_AREA=:6"
               + " Where elementid = '" + prodGuid + "'";
               command.CommandText = sqlStmt;
#endif
#if POSTGRES
               string sqlStmt = "update " + DBOperation.formatTabName("BIMRL_ELEMENT") + " set geometrybody_geomtype=@gtyp, GEOMETRYBODY=@gbody, TRANSFORM_COL=@trf, TOTAL_SURFACE_AREA=@surfa"
               + " Where elementid = @eid";
               command.CommandText = sqlStmt;
#endif
               // int status = DBOperation.updateGeometry(sqlStmt, sdoGeomData);
               currStep = sqlStmt;

               try
               {
#if ORACLE
                  OracleParameter[] sdoGeom = new OracleParameter[6];
                  for (int i = 0; i < sdoGeom.Count()-1; ++i)
                  {
                        sdoGeom[i] = command.Parameters.Add((i+1).ToString(), OracleDbType.Object);
                        sdoGeom[i].Direction = ParameterDirection.Input;
                        sdoGeom[i].UdtTypeName = "MDSYS.SDO_GEOMETRY";
                        sdoGeom[i].Size = 1;
                  }
                  sdoGeom[0].Value = trfOnly ? null : sdoGeomData;

                  SdoGeometry trcol1 = new SdoGeometry();
                  trcol1.Dimensionality = 3;
                  trcol1.LRS = 0;
                  trcol1.GeometryType = (int)SdoGeometryTypes.GTYPE.POINT;
                  gType = trcol1.PropertiesToGTYPE();
                  SdoPoint trcol1V = new SdoPoint();
                  trcol1V.XD = trfOnly ? newRelTrf.M11 : m3D.M11;
                  trcol1V.YD = trfOnly ? newRelTrf.M12 : m3D.M12;
                  trcol1V.ZD = trfOnly ? newRelTrf.M13 : m3D.M13;
                  trcol1.SdoPoint = trcol1V;
                  sdoGeom[1].Value = trcol1;

                  SdoGeometry trcol2 = new SdoGeometry();
                  trcol2.Dimensionality = 3;
                  trcol2.LRS = 0;
                  trcol2.GeometryType = (int)SdoGeometryTypes.GTYPE.POINT;
                  gType = trcol2.PropertiesToGTYPE();
                  SdoPoint trcol2V = new SdoPoint();
                  trcol2V.XD = trfOnly ? newRelTrf.M21 : m3D.M21;
                  trcol2V.YD = trfOnly ? newRelTrf.M22 : m3D.M22;
                  trcol2V.ZD = trfOnly ? newRelTrf.M23 : m3D.M23;
                  trcol2.SdoPoint = trcol2V;
                  sdoGeom[2].Value = trcol2;

                  SdoGeometry trcol3 = new SdoGeometry();
                  trcol3.Dimensionality = 3;
                  trcol3.LRS = 0;
                  trcol3.GeometryType = (int)SdoGeometryTypes.GTYPE.POINT;
                  gType = trcol3.PropertiesToGTYPE();
                  SdoPoint trcol3V = new SdoPoint();
                  trcol3V.XD = trfOnly ? newRelTrf.M31 : m3D.M31;
                  trcol3V.YD = trfOnly ? newRelTrf.M32 : m3D.M32;
                  trcol3V.ZD = trfOnly ? newRelTrf.M33 : m3D.M33;
                  trcol3.SdoPoint = trcol3V;
                  sdoGeom[3].Value = trcol3;

                  SdoGeometry trcol4 = new SdoGeometry();
                  trcol4.Dimensionality = 3;
                  trcol4.LRS = 0;
                  trcol4.GeometryType = (int)SdoGeometryTypes.GTYPE.POINT;
                  gType = trcol4.PropertiesToGTYPE();
                  SdoPoint trcol4V = new SdoPoint();
                  trcol4V.XD = trfOnly ? newRelTrf.OffsetX : m3D.OffsetX * _model.ModelFactors.LengthToMetresConversionFactor;
                  trcol4V.YD = trfOnly ? newRelTrf.OffsetY : m3D.OffsetY * _model.ModelFactors.LengthToMetresConversionFactor;
                  trcol4V.ZD = trfOnly ? newRelTrf.OffsetZ : m3D.OffsetZ * _model.ModelFactors.LengthToMetresConversionFactor;
                  trcol4.SdoPoint = trcol4V;
                  sdoGeom[4].Value = trcol4;

                  sdoGeom[5] = command.Parameters.Add("6", OracleDbType.Double);
                  sdoGeom[5].Direction = ParameterDirection.Input;
                  sdoGeom[5].Value = totalSurfArea;
#endif
#if POSTGRES
                  command.Parameters.Clear();
                  command.Parameters.AddWithValue("@gtyp", NpgsqlDbType.Enum, GeometryTypeEnum.geomsolid3d);
                  string geomJson = JsonConvert.SerializeObject(ProdGeometries);
                  command.Parameters.AddWithValue("@gbody", NpgsqlDbType.Jsonb, geomJson);
                  // Organized the data according to 
                  //            | Xaxis-x Xaxis-y Xaxis-z Offset-x |
                  //            | Yaxis-x Yaxis-y Yaxis-z Offset-y |
                  //            | Zaxis-x Zaxis-y Zaxis-z Offset-z |
                  //            |    0       0       0       1     |

                  double[,] trf = new double[4, 4];
                  trf[0, 0] = trfOnly ? newRelTrf.M11 : m3D.M11;
                  trf[0, 1] = trfOnly ? newRelTrf.M12 : m3D.M12;
                  trf[0, 2] = trfOnly ? newRelTrf.M13 : m3D.M13;
                  trf[0, 3] = trfOnly ? newRelTrf.OffsetX : m3D.OffsetX;
                  trf[1, 0] = trfOnly ? newRelTrf.M21 : m3D.M21;
                  trf[1, 1] = trfOnly ? newRelTrf.M22 : m3D.M22;
                  trf[1, 2] = trfOnly ? newRelTrf.M23 : m3D.M23;
                  trf[1, 3] = trfOnly ? newRelTrf.OffsetY : m3D.OffsetY;
                  trf[2, 0] = trfOnly ? newRelTrf.M31 : m3D.M31;
                  trf[2, 1] = trfOnly ? newRelTrf.M32 : m3D.M32;
                  trf[2, 2] = trfOnly ? newRelTrf.M33 : m3D.M33;
                  trf[2, 3] = trfOnly ? newRelTrf.OffsetZ : m3D.OffsetZ;
                  trf[3, 0] = 0;
                  trf[3, 1] = 0;
                  trf[3, 2] = 0;
                  trf[3, 3] = 1;
                  command.Parameters.AddWithValue("@trf", NpgsqlDbType.Array | NpgsqlDbType.Double, trf);
                  command.Parameters.AddWithValue("@surfa", NpgsqlDbType.Double, totalSurfArea);
                  command.Parameters.AddWithValue("@eid", NpgsqlDbType.Text, prodGuid);
                  // This update statement will be repeated many times for each object with geometry. Prepare the statement.
                  command.Prepare();
#endif
                  commandStatus = command.ExecuteNonQuery();
                  command.Parameters.Clear();

                  currInsertCount++;

                  if (currInsertCount % DBOperation.commitInterval == 0)
                  {
                        //Do commit at interval but keep the long transaction (reopen)
                        DBOperation.commitTransaction();
                  }
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
                  //command.Dispose();   // Log Oracle error and continue
#if ORACLE
                  command = new OracleCommand(" ", DBOperation.DBConn);
#endif
#if POSTGRES
                  command = new NpgsqlCommand(" ", DBOperation.DBConn);
#endif
                  // throw;
               }
               catch (SystemException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushError(excStr);
                  throw;
               }
            }
         }

         DBOperation.commitTransaction();
         command.Dispose();
      }  

      private double CalculateAreaOfTriangle(IList<Point3D> triangleCoords)
      {
         double area = 0.0;
         if (triangleCoords.Count >= 3)
         {
            Vector3D vAB = new Vector3D((triangleCoords[1].X - triangleCoords[0].X), (triangleCoords[1].Y - triangleCoords[0].Y), (triangleCoords[1].Z - triangleCoords[0].Z));
            Vector3D vAC = new Vector3D((triangleCoords[2].X - triangleCoords[0].X), (triangleCoords[2].Y - triangleCoords[0].Y), (triangleCoords[2].Z - triangleCoords[0].Z));
            Vector3D crossP = Vector3D.CrossProduct(vAB, vAC);
            area = 0.5 * crossP.Length;
            // Heron's formula
            //double a = Point3D.distance(triangleCoords[0], triangleCoords[1]);
            //double b = Point3D.distance(triangleCoords[1], triangleCoords[2]);
            //double c = Point3D.distance(triangleCoords[2], triangleCoords[0]);
            //double s = (a + b + c) / 2;
            //area = Math.Sqrt(s * (s - a) * (s - b) * (s - c));
         }
         return area;
      }

      public void 
         processElements()
      {
         DBOperation.beginTransaction();

         int commandStatus = -1;
         int currInsertCount = 0;
         string container = string.Empty;
         string SqlStmt;
         string currStep = string.Empty;

#if ORACLE
         OracleCommand command = new OracleCommand(" ", DBOperation.DBConn);
#endif
#if POSTGRES
         NpgsqlCommand command = new NpgsqlCommand(" ", DBOperation.DBConn);
#endif
         try
         {
            // Process IfcProject
            IIfcProject project = _model.Instances.OfType<IIfcProject>().FirstOrDefault();
            SqlStmt = "SELECT COUNT(*) FROM " + DBOperation.formatTabName("BIMRL_ELEMENT") + " WHERE ELEMENTID='" + project.GlobalId.ToString() + "'";
#if ORACLE
            OracleCommand chkCmd = new OracleCommand(SqlStmt, DBOperation.DBConn);
#endif
#if POSTGRES
            NpgsqlCommand chkCmd = new NpgsqlCommand(SqlStmt, DBOperation.DBConn);
#endif            
            object ret = chkCmd.ExecuteScalar();
            int iRet = Convert.ToInt32(ret.ToString());
            if (iRet == 0)
            {
               SqlStmt = "INSERT INTO " + DBOperation.formatTabName("BIMRL_ELEMENT") + " (ELEMENTID, ELEMENTTYPE, MODELID) VALUES ("
               + "'" + project.GlobalId.ToString() + "', 'IFCPROJECT', " + BIMRLProcessModel.currModelID.ToString() + ")";
#if ORACLE
               OracleCommand cmd = new OracleCommand(SqlStmt, DBOperation.DBConn);
#endif
#if POSTGRES
               NpgsqlCommand cmd = new NpgsqlCommand(SqlStmt, DBOperation.DBConn);
#endif               
               cmd.ExecuteNonQuery();
               cmd.Dispose();
            }

            IEnumerable<IIfcProduct> elements = _model.Instances.OfType<IIfcProduct>(true).Where
               (et => !(et is IIfcSpatialStructureElement || et is IIfcPort || et is IIfcVirtualElement
                  || et is IIfcAnnotation || et is IIfcGrid));
            foreach (IIfcProduct el in elements)
            {
               IIfcElement elem = el as IIfcElement;
               string guid = el.GlobalId;
               string typeName = el.GetType().Name.ToUpper();
               // IFC Type
               string typGuid = string.Empty;
               IEnumerable<IIfcRelDefinesByType> relTyp = el.IsTypedBy;
               if (relTyp != null || relTyp.Count() > 0)
               {
                  // Only one Type can be assigned to an Element based on IFC schema WR1
                  IIfcRelDefinesByType typ = relTyp.FirstOrDefault();
                  if (typ != null)
                        typGuid = typ.RelatingType.GlobalId.ToString();
               }
               // Owner History, skip for now

               string elName = BIMRLUtils.checkSingleQuote(el.Name);
               string elObjectType = BIMRLUtils.checkSingleQuote(el.ObjectType);
               string elDesc = BIMRLUtils.checkSingleQuote(el.Description);
               int IfcLineNo = el.EntityLabel;
               string tag = elem.Tag.ToString();
               IIfcRelContainedInSpatialStructure relContainer = elem.ContainedInStructure.FirstOrDefault();
               if (relContainer == null)
                  container = string.Empty;
               else
                  container = relContainer.RelatingStructure.GlobalId.ToString();

               // from el, we can get all IFC related attributes (property set?), including relationships. But first we need to populate BIMRL_ELEMENT table first before building the relationships
               // Keep a mapping between IFC guid used as a key in BIMRL and the IFC line no of the entity
               _refBIMRLCommon.guidLineNoMappingAdd(BIMRLProcessModel.currModelID, IfcLineNo, guid);

               string columnSpec = "Elementid, LineNo, ElementType, ModelID";
               string valueList = "'" + guid + "'," + IfcLineNo.ToString() + ",'" + typeName + "'," + BIMRLProcessModel.currModelID.ToString();

               if (!string.IsNullOrEmpty(typGuid))
               {
                  columnSpec += ", TypeID";
                  valueList += ", '" + typGuid + "'";
               }
               if (!string.IsNullOrEmpty(elName))
               {
                  columnSpec += ", Name";
                  valueList += ", '" + elName + "'";
               }
               if (!string.IsNullOrEmpty(elDesc))
               {
                  columnSpec += ", Description";
                  valueList += ", '" + elDesc + "'";
               }
               if (!string.IsNullOrEmpty(elObjectType))
               {
                  columnSpec += ", ObjectType";
                  valueList += ", '" + elObjectType + "'";
               }
               if (!string.IsNullOrEmpty(tag))
               {
                  columnSpec += ", Tag";
                  valueList += ", '" + tag + "'";
               }
               if (!string.IsNullOrEmpty(container))
               {
                  columnSpec += ", Container";
                  valueList += ", '" + container + "'";
               }

               Tuple<int, int> ownHEntry = new Tuple<int, int>(Math.Abs(el.OwnerHistory.EntityLabel), BIMRLProcessModel.currModelID);
               if (_refBIMRLCommon.OwnerHistoryExist(ownHEntry))
               {
                  columnSpec += ", OwnerHistoryID";
                  valueList += ", " + Math.Abs(el.OwnerHistory.EntityLabel);
               }

               SqlStmt = "Insert into " + DBOperation.formatTabName("BIMRL_Element") + "(" + columnSpec + ") Values (" + valueList + ")";
               command.CommandText = SqlStmt;
               currStep = SqlStmt;
               commandStatus = command.ExecuteNonQuery();

               // Add intormation of the product label (LineNo into a List for the use later to update the Geometry
               _refBIMRLCommon.insEntityLabelListAdd(Math.Abs(IfcLineNo));
               currInsertCount++;

               if (currInsertCount % DBOperation.commitInterval == 0)
               {
                  //Do commit at interval but keep the long transaction (reopen)
                  DBOperation.commitTransaction();
               }
            }

            //// Process Group objects (IfcSystem and IfcZone)
            //IEnumerable<IIfcGroup> groups = _model.Instances.OfType<IIfcGroup>(true).Where
            //   (et => (et is IIfcSystem || et is IIfcZone));
            IEnumerable<IIfcGroup> groups = _model.Instances.OfType<IIfcGroup>(true);
            foreach (IIfcGroup el in groups)
            {
               string guid = el.GlobalId;
               string typeName = el.GetType().Name.ToUpper();
               string elName = BIMRLUtils.checkSingleQuote(el.Name);
               string elObjectType = BIMRLUtils.checkSingleQuote(el.ObjectType);
               string elDesc = BIMRLUtils.checkSingleQuote(el.Description);
               int IfcLineNo = el.EntityLabel;

               _refBIMRLCommon.guidLineNoMappingAdd(BIMRLProcessModel.currModelID, IfcLineNo, guid);

               string columnSpec = "Elementid, LineNo, ElementType, ModelID";
               string valueList = "'" + guid + "'," + IfcLineNo.ToString() + ",'" + typeName + "'," + BIMRLProcessModel.currModelID.ToString();

               if (!string.IsNullOrEmpty(elName))
               {
                  columnSpec += ", Name";
                  valueList += ", '" + elName + "'";
               }
               if (!string.IsNullOrEmpty(elDesc))
               {
                  columnSpec += ", Description";
                  valueList += ", '" + elDesc + "'";
               }
               if (!string.IsNullOrEmpty(elObjectType))
               {
                  columnSpec += ", ObjectType";
                  valueList += ", '" + elObjectType + "'";
               }

               Tuple<int, int> ownHEntry = new Tuple<int, int>(Math.Abs(el.OwnerHistory.EntityLabel), BIMRLProcessModel.currModelID);
               if (_refBIMRLCommon.OwnerHistoryExist(ownHEntry))
               {
                  columnSpec += ", OwnerHistoryID";
                  valueList += ", " + Math.Abs(el.OwnerHistory.EntityLabel);
               }

               SqlStmt = "Insert into " + DBOperation.formatTabName("BIMRL_ELEMENT") + "(" + columnSpec + ") Values (" + valueList + ")";

               command.CommandText = SqlStmt;
               currStep = SqlStmt;
               commandStatus = command.ExecuteNonQuery();

               currInsertCount++;

               if (currInsertCount % DBOperation.commitInterval == 0)
               {
                  //Do commit at interval but keep the long transaction (reopen)
                  DBOperation.commitTransaction();
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
               string excStr = "%%Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushError(excStr);
               command.Dispose();
               throw;
         }
         catch (SystemException e)
         {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushError(excStr);
               throw;
         }

         DBOperation.commitTransaction();
         command.Dispose();

         // After all elements are processed, proceed with Geometries
         processGeometries();

         processProperties();
      }

      private void processProperties()
      {
         BIMRLProperties bimrlProp = new BIMRLProperties(_refBIMRLCommon);
#if ORACLE
         OracleCommand command = new OracleCommand(" ", DBOperation.DBConn);

         string SqlStmt = "Insert into " + DBOperation.formatTabName("BIMRL_ElementProperties") + "(ElementId, PropertyGroupName, PropertyName, PropertyValue, PropertyDataType"
            + ", PropertyUnit) Values (:1, :2, :3, :4, :5, :6)";
         command.CommandText = SqlStmt;
         string currStep = SqlStmt;

         OracleParameter[] Param = new OracleParameter[6];
         for (int i = 0; i < 6; i++)
         {
               Param[i] = command.Parameters.Add(i.ToString(), OracleDbType.Varchar2);
               Param[i].Direction = ParameterDirection.Input;
         }

         List<string> arrEleGuid = new List<string>();
         List<string> arrPGrpName = new List<string>();
         List<string> arrPropName = new List<string>();
         List<string> arrPropVal = new List<string>();
         List<OracleParameterStatus> arrPropValBS = new List<OracleParameterStatus>();
         List<string> arrPDatatyp = new List<string>();
         List<string> arrPUnit = new List<string>();
         List<OracleParameterStatus> arrPUnitBS = new List<OracleParameterStatus>();

         // Process Project "properties"
         IEnumerable<IIfcProject> projects = _model.Instances.OfType<IIfcProject>();
         // Insert only ONE project from the first one. Therefore needs to check its existence first
         IIfcProject project = projects.First();
         SqlStmt = "SELECT COUNT(*) FROM " + DBOperation.formatTabName("BIMRL_PROPERTIES") + " WHERE ELEMENTID='" + project.GlobalId.ToString() + "'";
         OracleCommand chkCmd = new OracleCommand(SqlStmt, DBOperation.DBConn);
#endif
#if POSTGRES
         NpgsqlCommand command = new NpgsqlCommand(" ", DBOperation.DBConn);

         string SqlStmt = "Insert into " + DBOperation.formatTabName("BIMRL_ElementProperties") + "(ElementId, PropertyGroupName, PropertyName, PropertyValue, PropertyDataType"
            + ", PropertyUnit) Values (@eid, @gname, @pname, @pvalue, @pdtyp, @punit)";
         command.CommandText = SqlStmt;
         string currStep = SqlStmt;

         command.Parameters.Add("@eid", NpgsqlDbType.Text);
         command.Parameters.Add("@gname", NpgsqlDbType.Text);
         command.Parameters.Add("@pname", NpgsqlDbType.Text);
         command.Parameters.Add("@pvalue", NpgsqlDbType.Text);
         command.Parameters.Add("@pdtyp", NpgsqlDbType.Text);
         command.Parameters.Add("@punit", NpgsqlDbType.Text);
         command.Prepare();

         // Process Project "properties"
         IEnumerable<IIfcProject> projects = _model.Instances.OfType<IIfcProject>();
         // Insert only ONE project from the first one. Therefore needs to check its existence first
         IIfcProject project = projects.First();
         SqlStmt = "SELECT COUNT(*) FROM " + DBOperation.formatTabName("BIMRL_PROPERTIES") + " WHERE ELEMENTID='" + project.GlobalId.ToString() + "'";
         NpgsqlCommand chkCmd = new NpgsqlCommand(SqlStmt, DBOperation.DBConn);
#endif

         object ret = chkCmd.ExecuteScalar();
         int iRet = Convert.ToInt32(ret.ToString());
         if (iRet == 0)
         {
            Vector3D trueNorthDir = new Vector3D();
            foreach (IIfcGeometricRepresentationContext repCtx in project.RepresentationContexts)
            {
               if (repCtx.TrueNorth != null)
               {
#if ORACLE
                  trueNorthDir.X = repCtx.TrueNorth.X;
                  trueNorthDir.Y = repCtx.TrueNorth.Y;
                  trueNorthDir.Z = repCtx.TrueNorth.Z;
                  insertProperty(ref arrEleGuid, project.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "TRUENORTH",
                     ref arrPropVal, ref arrPropValBS, trueNorthDir.ToString(), ref arrPDatatyp, "VECTOR", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
                  trueNorthDir.X = repCtx.TrueNorth.X;
                  trueNorthDir.Y = repCtx.TrueNorth.Y;
                  trueNorthDir.Z = repCtx.TrueNorth.Z;
                  insertProperty(command, project.GlobalId, "IFCATTRIBUTES", "TRUENORTH", trueNorthDir.ToString(), "VECTOR", string.Empty);
#endif
                  break;
               }
            }
            // Other project properties
            if (project.Description != null)
            {
#if ORACLE
               insertProperty(ref arrEleGuid, project.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "DESCRIPTION",
                  ref arrPropVal, ref arrPropValBS, project.Description.Value, ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               insertProperty(command, project.GlobalId, "IFCATTRIBUTES", "DESCRIPTION", project.Description.Value, "STRING", string.Empty);
#endif
            }
            if (project.ObjectType != null)
            {
#if ORACLE
               insertProperty(ref arrEleGuid, project.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "OBJECTTYPE",
                  ref arrPropVal, ref arrPropValBS, project.ObjectType.Value, ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               insertProperty(command, project.GlobalId, "IFCATTRIBUTES", "OBJECTTYPE", project.ObjectType.Value, "STRING", string.Empty);
#endif
            }
            if (project.LongName != null)
            {
#if ORACLE
               insertProperty(ref arrEleGuid, project.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "LONGNAME",
                  ref arrPropVal, ref arrPropValBS, project.LongName, ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               insertProperty(command, project.GlobalId, "IFCATTRIBUTES", "LONGNAME", project.LongName, "STRING", string.Empty);
#endif
            }
            if (project.Phase != null)
            {
#if ORACLE
               insertProperty(ref arrEleGuid, project.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PHASE",
                  ref arrPropVal, ref arrPropValBS, project.Phase, ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               insertProperty(command, project.GlobalId, "IFCATTRIBUTES", "PHASE", project.Phase, "STRING", string.Empty);
#endif
            }

            // Process project units
            int uDefCnt = 1;
            BIMRLUtils.setupUnitRep();
            IEnumerable<IIfcUnit> units = project.UnitsInContext.Units;
            Dictionary<string, UnitSetting> unitItems = new Dictionary<string, UnitSetting>();
            foreach (IIfcUnit unit in units)
            {
               BIMRLUtils.AddIfcProjectUnitDict(unit);
               string unitRepStr = BIMRLUtils.getIfcUnitStr(unit);
               if (unit is IIfcSIUnit)
               {
                  IIfcSIUnit unitSI = unit as IIfcSIUnit;
                  string unitType = unitSI.UnitType.ToString();
                  UnitSetting unitS = new UnitSetting();
                  unitS.unitName = unitSI.Name.ToString();
                  if (unitSI.Prefix != null)
                        unitS.unitName = unitSI.Prefix.ToString() + " " + unitS.unitName;
                  unitS.unitType = "METRIC";
                  unitS.unitOfMeasure = unitRepStr;
                  unitItems.Add(unitType, unitS);
               }
               else if (unit is IIfcConversionBasedUnit)
               {
                  IIfcConversionBasedUnit cUnit = unit as IIfcConversionBasedUnit;
                  string unitType = cUnit.UnitType.ToString();
                  string name = cUnit.Name;
                  double conversionFactor = (double)cUnit.ConversionFactor.ValueComponent.Value;
                  UnitSetting unitS;
                  if (unitItems.TryGetValue(unitType, out unitS))
                  {
                        unitS.unitName = name;
                        unitS.unitType = "IMPERIAL";
                        unitS.conversionFactor = conversionFactor;
                     unitS.unitOfMeasure = unitRepStr;
                  }
                  else
                  {
                        unitS = new UnitSetting();
                        unitS.unitName = name;
                        unitS.unitType = "IMPERIAL";
                        unitS.conversionFactor = conversionFactor;
                     unitS.unitOfMeasure = unitRepStr;
                     unitItems.Add(unitType, unitS);
                  }
               }
               else if (unit is IIfcDerivedUnit)
               {
                  UnitSetting unitSetting = new UnitSetting();
                  IIfcDerivedUnit dUnit = unit as IIfcDerivedUnit;
                  string unitType;
                  if (dUnit.UnitType == IfcDerivedUnitEnum.USERDEFINED)
                  {
                     if (dUnit.UserDefinedType.HasValue)
                        unitType = dUnit.UserDefinedType.ToString();
                     else
                        unitType = dUnit.UnitType.ToString() + uDefCnt++.ToString();
                  }
                  else
                     unitType = dUnit.UnitType.ToString();

                  unitSetting.unitName = unitRepStr;
                  unitSetting.unitType = "DERIVED";
                  unitSetting.unitOfMeasure = unitRepStr;
                  unitItems.Add(unitType, unitSetting);
               }
            }
            // Now we collect all the dictionary entries and set them into array for property insertion
            foreach (KeyValuePair<string, UnitSetting> unitItem in unitItems)
            {
#if ORACLE
               insertProperty(ref arrEleGuid, project.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, unitItem.Key,
                  ref arrPropVal, ref arrPropValBS, unitItem.Value.unitName, ref arrPDatatyp, unitItem.Value.unitType, ref arrPUnit, ref arrPUnitBS, unitItem.Value.conversionFactor.ToString());
#endif
#if POSTGRES
               insertProperty(command, project.GlobalId, "IFCATTRIBUTES", unitItem.Key, unitItem.Value.unitName, unitItem.Value.unitType, unitItem.Value.conversionFactor.ToString());
#endif
            }

            //// Process the rest of the project properties in property sets (if any)
            //bimrlProp.processElemProperties(project);
         }

            IEnumerable<IIfcProduct> elements = _model.Instances.OfType<IIfcProduct>().Where
               (et => !(et is IIfcPort || et is IIfcVirtualElement || et is IIfcAnnotation || et is IIfcGrid));

         string uom = string.Empty;
         foreach (IIfcProduct el in elements)
         {
            if (el is IIfcSite)
            {
               IIfcSite sse_s = el as IIfcSite;

#if ORACLE
               if (sse_s.RefLatitude != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "REFLATITUDE",
                     ref arrPropVal, ref arrPropValBS, sse_s.RefLatitude.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(sse_s.RefLatitude.GetType()));

               if (sse_s.RefLongitude != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "REFLONGITUDE",
                     ref arrPropVal, ref arrPropValBS, sse_s.RefLongitude.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(sse_s.RefLongitude.GetType()));

               if (sse_s.RefElevation != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "REFELEVATION",
                     ref arrPropVal, ref arrPropValBS, sse_s.RefElevation.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(sse_s.RefElevation.GetType()));

               if (sse_s.SiteAddress != null)
               {
                  BIMRLAddressData addrData = new BIMRLAddressData(sse_s.SiteAddress);
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "SITEADDRESS",
                     ref arrPropVal, ref arrPropValBS, addrData.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }

               if (sse_s.LandTitleNumber != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "LANDTITLENUMBER",
                     ref arrPropVal, ref arrPropValBS, sse_s.LandTitleNumber.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);

               if (sse_s.CompositionType != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "COMPOSITIONTYPE",
                     ref arrPropVal, ref arrPropValBS, sse_s.CompositionType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               if (sse_s.RefLatitude != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "REFLATITUDE", sse_s.RefLatitude.ToString(), 
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(sse_s.RefLatitude.GetType()));

               if (sse_s.RefLongitude != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "REFLONGITUDE", sse_s.RefLongitude.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(sse_s.RefLongitude.GetType()));

               if (sse_s.RefElevation != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "REFELEVATION", sse_s.RefElevation.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(sse_s.RefElevation.GetType()));

               if (sse_s.LandTitleNumber != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "LANDTITLENUMBER", sse_s.LandTitleNumber.ToString(),
                     "STRING", string.Empty);

               if (sse_s.SiteAddress != null)
               {
                  BIMRLAddressData addrData = new BIMRLAddressData(sse_s.SiteAddress);
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "SITEADDRESS", addrData.ToString(),
                     "STRING", string.Empty);
               }

               if (sse_s.CompositionType != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "COMPOSITIONTYPE", sse_s.CompositionType.ToString(),
                   "STRING", string.Empty);
#endif
            }

            else if (el is IIfcBuilding)
            {
               IIfcBuilding sse_b = el as IIfcBuilding;

#if ORACLE
               if (sse_b.ElevationOfRefHeight != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "ELEVATIONOFREFHEIGHT",
                     ref arrPropVal, ref arrPropValBS, sse_b.ElevationOfRefHeight.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(sse_b.ElevationOfRefHeight.GetType()));

               if (sse_b.ElevationOfTerrain != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "ELEVATIONOFTERRAIN",
                     ref arrPropVal, ref arrPropValBS, sse_b.ElevationOfTerrain.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(sse_b.ElevationOfTerrain.GetType()));

               if (sse_b.BuildingAddress != null)
               {
                  BIMRLAddressData addrData = new BIMRLAddressData(sse_b.BuildingAddress);
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "BUILDINGADDRESS",
                     ref arrPropVal, ref arrPropValBS, addrData.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }

               if (sse_b.CompositionType != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "COMPOSITIONTYPE",
                     ref arrPropVal, ref arrPropValBS, sse_b.CompositionType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               if (sse_b.ElevationOfRefHeight != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "ELEVATIONOFREFHEIGHT", sse_b.ElevationOfRefHeight.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(sse_b.ElevationOfRefHeight.GetType()));

               if (sse_b.ElevationOfTerrain != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "ELEVATIONOFREFHEIGHT", sse_b.ElevationOfTerrain.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(sse_b.ElevationOfTerrain.GetType()));

               if (sse_b.BuildingAddress != null)
               {
                  BIMRLAddressData addrData = new BIMRLAddressData(sse_b.BuildingAddress);
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "BUILDINGADDRESS", addrData.ToString(),
                     "STRING", string.Empty);
               }

               if (sse_b.CompositionType != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "COMPOSITIONTYPE", sse_b.CompositionType.ToString(),
                     "STRING", string.Empty);
#endif
            }

            else if (el is IIfcBuildingStorey)
            {
               IIfcBuildingStorey sse_bs = el as IIfcBuildingStorey;

#if ORACLE
               if (sse_bs.Elevation != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "ELEVATION",
                     ref arrPropVal, ref arrPropValBS, sse_bs.Elevation.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(sse_bs.Elevation.GetType()));

               if (sse_bs.CompositionType != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "COMPOSITIONTYPE",
                     ref arrPropVal, ref arrPropValBS, sse_bs.CompositionType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               if (sse_bs.Elevation != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "ELEVATION", sse_bs.Elevation.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(sse_bs.Elevation.GetType()));

               if (sse_bs.CompositionType != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "COMPOSITIONTYPE", sse_bs.CompositionType.ToString(),
                     "STRING", string.Empty);
#endif
            }

            /* Various Element specific attributes to be inserted into Property tables as group IFCATTRIBUTES
            */
            else if (el is IIfcBuildingElementProxy)
            {
               IIfcBuildingElementProxy elem = el as IIfcBuildingElementProxy;

#if ORACLE
               if (isIfc2x3)
               {
                  Xbim.Ifc2x3.Interfaces.IIfcBuildingElementProxy elem2x3 = elem as Xbim.Ifc2x3.Interfaces.IIfcBuildingElementProxy;
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "COMPOSITIONTYPE",
                     ref arrPropVal, ref arrPropValBS, elem2x3.CompositionType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }
               else
               {
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                     ref arrPropVal, ref arrPropValBS, elem.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }
#endif
#if POSTGRES
               if (isIfc2x3)
               {
                  Xbim.Ifc2x3.Interfaces.IIfcBuildingElementProxy elem2x3 = elem as Xbim.Ifc2x3.Interfaces.IIfcBuildingElementProxy;
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "COMPOSITIONTYPE", elem2x3.CompositionType.ToString(),
                     "STRING", string.Empty);
               }
               else
               {
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elem.PredefinedType.ToString(),
                     "STRING", string.Empty);
               }
#endif
            }
                
            else if (el is IIfcCovering)
            {
               IIfcCovering elem = el as IIfcCovering;

#if ORACLE
               insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                  ref arrPropVal, ref arrPropValBS, elem.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elem.PredefinedType.ToString(),
                  "STRING", string.Empty);
#endif
            }

            else if (el is IIfcDistributionControlElement)
            {
               IIfcDistributionControlElement elem = el as IIfcDistributionControlElement;
               Xbim.Ifc2x3.Interfaces.IIfcDistributionControlElement elem2x3 = el as Xbim.Ifc2x3.Interfaces.IIfcDistributionControlElement;

#if ORACLE
               if (isIfc2x3)
               {
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "CONTROLELEMENTID",
                     ref arrPropVal, ref arrPropValBS, elem2x3.ControlElementId.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }
               else
               {
                  // It is IFC4, use dynamic cast to the actual type to get the PredefinedType attribute
                  dynamic elemDet = Convert.ChangeType(elem, elem.GetType());
                  if (!(elemDet is Xbim.Ifc4.SharedBldgServiceElements.IfcDistributionControlElement) && elemDet.PredefinedType != null)
                     insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                        ref arrPropVal, ref arrPropValBS, elemDet.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }
#endif
#if POSTGRES
               if (isIfc2x3)
               {
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "CONTROLELEMENTID", elem2x3.ControlElementId.ToString(),
                        "STRING", string.Empty);
               }
               else
               {
                  // It is IFC4, use dynamic cast to the actual type to get the PredefinedType attribute
                  dynamic elemDet = Convert.ChangeType(elem, elem.GetType());
                  if (!(elemDet is Xbim.Ifc4.SharedBldgServiceElements.IfcDistributionControlElement) && elemDet.PredefinedType != null)
                     insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elemDet.PredefinedType.ToString(),
                        "STRING", string.Empty);
               }
#endif
            }

            else if (el is IIfcDoor)
            {
               IIfcDoor elem = el as IIfcDoor;

#if ORACLE
               if (elem.OverallHeight != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "OVERALLHEIGHT",
                     ref arrPropVal, ref arrPropValBS, elem.OverallHeight.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.OverallHeight.GetType()));

               if (elem.OverallWidth != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "OVERALLWIDTH",
                     ref arrPropVal, ref arrPropValBS, elem.OverallWidth.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.OverallWidth.GetType()));
#endif
#if POSTGRES
               if (elem.OverallHeight != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "OVERALLHEIGHT", elem.OverallHeight.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.OverallHeight.GetType()));

               if (elem.OverallWidth != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "OVERALLWIDTH", elem.OverallWidth.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.OverallWidth.GetType()));
#endif
            } 

            else if (el is Xbim.Ifc2x3.ElectricalDomain.IfcElectricDistributionPoint)
            {
               Xbim.Ifc2x3.Interfaces.IIfcElectricDistributionPoint elem2x3 = el as Xbim.Ifc2x3.Interfaces.IIfcElectricDistributionPoint;

#if ORACLE
               if (elem2x3.UserDefinedFunction != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "USERDEFINEDFUNCTION",
                     ref arrPropVal, ref arrPropValBS, elem2x3.UserDefinedFunction.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);

               insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "DISTRIBUTIONPOINTFUNCTION",
                  ref arrPropVal, ref arrPropValBS, elem2x3.DistributionPointFunction.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               if (elem2x3.UserDefinedFunction != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "USERDEFINEDFUNCTION", elem2x3.UserDefinedFunction.ToString(),
                     "STRING", string.Empty);

               insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "DISTRIBUTIONPOINTFUNCTION", elem2x3.DistributionPointFunction.ToString(),
                  "STRING", string.Empty);
#endif
            }

            else if (el is IIfcElementAssembly)
            {
               IIfcElementAssembly elem = el as IIfcElementAssembly;

#if ORACLE
               if (elem.AssemblyPlace != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "ASSEMBLYPLACE",
                     ref arrPropVal, ref arrPropValBS, elem.AssemblyPlace.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);

               if (elem.PredefinedType != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                     ref arrPropVal, ref arrPropValBS, elem.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               if (elem.AssemblyPlace != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "ASSEMBLYPLACE", elem.AssemblyPlace.ToString(),
                     "STRING", string.Empty);

               if (elem.PredefinedType != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elem.PredefinedType.ToString(),
                     "STRING", string.Empty);
#endif
            }

            else if (el is IIfcFooting)
            {
               IIfcFooting elem = el as IIfcFooting;

#if ORACLE
               arrPropName.Add("PREDEFINEDTYPE");
               if (elem.PredefinedType != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                     ref arrPropVal, ref arrPropValBS, elem.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               if (elem.PredefinedType != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elem.PredefinedType.ToString(),
                     "STRING", string.Empty);
#endif
            }

            else if (el is IIfcPile)
            {
               IIfcPile elem = el as IIfcPile;

#if ORACLE
               if (elem.ConstructionType != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "CONSTRUCTIONTYPE",
                     ref arrPropVal, ref arrPropValBS, elem.ConstructionType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);

               if (elem.PredefinedType != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                     ref arrPropVal, ref arrPropValBS, elem.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               if (elem.ConstructionType != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "CONSTRUCTIONTYPE", elem.ConstructionType.ToString(),
                     "STRING", string.Empty);

               if (elem.PredefinedType != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elem.PredefinedType.ToString(),
                     "STRING", string.Empty);
#endif
            }

            else if (el is IIfcRailing)
            {
               IIfcRailing elem = el as IIfcRailing;

#if ORACLE
               if (elem.PredefinedType != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                     ref arrPropVal, ref arrPropValBS, elem.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               if (elem.PredefinedType != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elem.PredefinedType.ToString(),
                     "STRING", string.Empty);
#endif
            }

            else if (el is IIfcRamp)
            {
               IIfcRamp elem = el as IIfcRamp;
               Xbim.Ifc2x3.Interfaces.IIfcRamp elem2x3 = el as Xbim.Ifc2x3.Interfaces.IIfcRamp;

#if ORACLE
               if (isIfc2x3)
               {
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "SHAPETYPE",
                     ref arrPropVal, ref arrPropValBS, elem2x3.ShapeType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }
               else
               {
                  if (elem.PredefinedType != null)
                     insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                        ref arrPropVal, ref arrPropValBS, elem.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }
#endif
#if POSTGRES
               if (isIfc2x3)
               {
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "SHAPETYPE", elem2x3.ShapeType.ToString(),
                     "STRING", string.Empty);
               }
               else
               {
                  if (elem.PredefinedType != null)
                     insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elem.PredefinedType.ToString(),
                        "STRING", string.Empty);
               }
#endif            
            }

            else if (el is IIfcRampFlight)
            {
               IIfcRampFlight elem = el as IIfcRampFlight;
               Xbim.Ifc2x3.Interfaces.IIfcRampFlight elem2x3 = el as Xbim.Ifc2x3.Interfaces.IIfcRampFlight;

#if ORACLE
               if (isIfc2x3)
               {
               }
               else
               {
                  if (elem.PredefinedType != null)
                     insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                        ref arrPropVal, ref arrPropValBS, elem.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }
#endif
#if POSTGRES
               if (isIfc2x3)
               {
               }
               else
               {
                  if (elem.PredefinedType != null)
                     insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elem.PredefinedType.ToString(),
                        "STRING", string.Empty);
               }
#endif
            }

            else if (el is IIfcReinforcingBar)
            {
               IIfcReinforcingBar elem = el as IIfcReinforcingBar;
               Xbim.Ifc2x3.Interfaces.IIfcReinforcingBar elem2x3 = el as Xbim.Ifc2x3.Interfaces.IIfcReinforcingBar;

#if ORACLE
               if (isIfc2x3)
               {
                  if (elem2x3.SteelGrade != null)
                     insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "STEELGRADE",
                        ref arrPropVal, ref arrPropValBS, elem2x3.SteelGrade.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);

                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "BARROLE",
                     ref arrPropVal, ref arrPropValBS, elem2x3.BarRole.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }
               else
               {
                  if (elem.PredefinedType!= null)
                     insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                        ref arrPropVal, ref arrPropValBS, elem.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }

               if (elem.NominalDiameter != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "NOMINALDIAMETER",
                     ref arrPropVal, ref arrPropValBS, elem.NominalDiameter.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.NominalDiameter.GetType()));

               if (elem.CrossSectionArea != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "CROSSSECTIONAREA",
                     ref arrPropVal, ref arrPropValBS, elem.CrossSectionArea.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.CrossSectionArea.GetType()));

               if (elem.BarLength != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "BARLENGTH",
                     ref arrPropVal, ref arrPropValBS, elem.BarLength.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.BarLength.GetType()));

               if (elem.BarSurface != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "BARSURFACE",
                     ref arrPropVal, ref arrPropValBS, elem.BarSurface.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               if (isIfc2x3)
               {
                  if (elem2x3.SteelGrade != null)
                     insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "STEELGRADE", elem2x3.SteelGrade.ToString(),
                        "STRING", string.Empty);

                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "BARROLE", elem2x3.BarRole.ToString(),
                     "STRING", string.Empty);
               }
               else
               {
                  if (elem.PredefinedType != null)
                     insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elem.PredefinedType.ToString(),
                        "STRING", string.Empty);
               }

               if (elem.NominalDiameter != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "NOMINALDIAMETER", elem.NominalDiameter.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.NominalDiameter.GetType()));

               if (elem.CrossSectionArea != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "CROSSSECTIONAREA", elem.CrossSectionArea.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.CrossSectionArea.GetType()));

               if (elem.BarLength != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "BARLENGTH", elem.BarLength.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.BarLength.GetType()));

               if (elem.BarSurface != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "BARSURFACE", elem.BarSurface.ToString(),
                     "STRING", string.Empty);
#endif
            }

            else if (el is IIfcReinforcingMesh)
            {
               IIfcReinforcingMesh elem = el as IIfcReinforcingMesh;

#if ORACLE
               if (elem.SteelGrade != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "STEELGRADE",
                     ref arrPropVal, ref arrPropValBS, elem.SteelGrade.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);

               if (elem.MeshLength != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "MESHLENGTH",
                     ref arrPropVal, ref arrPropValBS, elem.MeshLength.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.MeshLength.GetType()));

               if (elem.MeshWidth != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "MESHWIDTH",
                     ref arrPropVal, ref arrPropValBS, elem.MeshWidth.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.MeshWidth.GetType()));

               if (elem.LongitudinalBarNominalDiameter != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "LONGITUDINALBARNOMINALDIAMETER",
                     ref arrPropVal, ref arrPropValBS, elem.LongitudinalBarNominalDiameter.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.LongitudinalBarNominalDiameter.GetType()));

               if (elem.TransverseBarNominalDiameter != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "TRANSVERSEBARNOMINALDIAMETER",
                     ref arrPropVal, ref arrPropValBS, elem.TransverseBarNominalDiameter.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.TransverseBarNominalDiameter.GetType()));

               if (elem.LongitudinalBarCrossSectionArea != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "LONGITUDINALBARCROSSSECTIONAREA",
                     ref arrPropVal, ref arrPropValBS, elem.LongitudinalBarCrossSectionArea.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.LongitudinalBarCrossSectionArea.GetType()));

               if (elem.TransverseBarCrossSectionArea != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "TRANSVERSEBARCROSSSECTIONAREA",
                     ref arrPropVal, ref arrPropValBS, elem.TransverseBarCrossSectionArea.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.TransverseBarCrossSectionArea.GetType()));

               if (elem.LongitudinalBarSpacing != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "LONGITUDINALBARSPACING",
                     ref arrPropVal, ref arrPropValBS, elem.LongitudinalBarSpacing.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.LongitudinalBarSpacing.GetType()));

               if (elem.TransverseBarSpacing != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "TRANSVERSEBARSPACING",
                     ref arrPropVal, ref arrPropValBS, elem.TransverseBarSpacing.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.TransverseBarSpacing.GetType()));
#endif
#if POSTGRES
               if (elem.SteelGrade != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "STEELGRADE", elem.SteelGrade.ToString(),
                     "STRING", string.Empty);

               if (elem.MeshLength != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "MESHLENGTH", elem.MeshLength.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.MeshLength.GetType()));

               if (elem.MeshWidth != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "MESHWIDTH", elem.MeshWidth.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.MeshWidth.GetType()));

               if (elem.LongitudinalBarNominalDiameter != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "LONGITUDINALBARNOMINALDIAMETER", elem.LongitudinalBarNominalDiameter.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.LongitudinalBarNominalDiameter.GetType()));

               if (elem.TransverseBarNominalDiameter != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "TRANSVERSEBARNOMINALDIAMETER", elem.TransverseBarNominalDiameter.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.TransverseBarNominalDiameter.GetType()));

               if (elem.LongitudinalBarCrossSectionArea != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "LONGITUDINALBARCROSSSECTIONAREA", elem.LongitudinalBarCrossSectionArea.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.LongitudinalBarCrossSectionArea.GetType()));

               if (elem.TransverseBarCrossSectionArea != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "TRANSVERSEBARCROSSSECTIONAREA", elem.TransverseBarCrossSectionArea.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.TransverseBarCrossSectionArea.GetType()));

               if (elem.LongitudinalBarSpacing != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "LONGITUDINALBARSPACING", elem.LongitudinalBarSpacing.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.LongitudinalBarSpacing.GetType()));

               if (elem.TransverseBarSpacing != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "TRANSVERSEBARSPACING", elem.TransverseBarSpacing.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.TransverseBarSpacing.GetType()));
#endif
            }

            else if (el is IIfcRoof)
            {
               IIfcRoof elem = el as IIfcRoof;
               Xbim.Ifc2x3.Interfaces.IIfcRoof elem2x3 = el as Xbim.Ifc2x3.Interfaces.IIfcRoof;

#if ORACLE
               if (isIfc2x3)
               {
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "SHAPETYPE",
                     ref arrPropVal, ref arrPropValBS, elem2x3.ShapeType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }
               else
               {
                  if (elem.PredefinedType != null)
                     insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                        ref arrPropVal, ref arrPropValBS, elem.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }
#endif
#if POSTGRES
               if (isIfc2x3)
               {
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "SHAPETYPE", elem2x3.ShapeType.ToString(),
                     "STRING", string.Empty);
               }
               else
               {
                  if (elem.PredefinedType != null)
                     insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elem.PredefinedType.ToString(),
                        "STRING", string.Empty);
               }
#endif
            } 

            else if (el is IIfcSlab)
            {
               IIfcSlab elem = el as IIfcSlab;

#if ORACLE
               if (elem.PredefinedType != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                     ref arrPropVal, ref arrPropValBS, elem.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               if (elem.PredefinedType != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elem.PredefinedType.ToString(),
                     "STRING", string.Empty);
#endif
            }

            else if (el is IIfcStair)
            {
               IIfcStair elem = el as IIfcStair;
               Xbim.Ifc2x3.Interfaces.IIfcStair elem2x3 = el as Xbim.Ifc2x3.Interfaces.IIfcStair;

#if ORACLE
               if (isIfc2x3)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "SHAPETYPE",
                     ref arrPropVal, ref arrPropValBS, elem2x3.ShapeType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
#endif
#if POSTGRES
               if (isIfc2x3)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "SHAPETYPE", elem2x3.ShapeType.ToString(),
                     "STRING", string.Empty);
#endif
            } 

            else if (el is IIfcStairFlight)
            {
               IIfcStairFlight elem = el as IIfcStairFlight;
               Xbim.Ifc2x3.Interfaces.IIfcStairFlight elem2x3 = el as Xbim.Ifc2x3.Interfaces.IIfcStairFlight;

#if ORACLE
               if (isIfc2x3)
               {
                  if (elem2x3.NumberOfRiser != null)
                     insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "NUMBEROFRISER",
                        ref arrPropVal, ref arrPropValBS, elem2x3.NumberOfRiser.ToString(), ref arrPDatatyp, "INTEGER", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem2x3.NumberOfRiser.GetType()));
               }
               else
               {
                  if (elem.NumberOfRisers != null)
                     insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "NUMBEROFRISERS",
                        ref arrPropVal, ref arrPropValBS, elem.NumberOfRisers.ToString(), ref arrPDatatyp, "INTEGER", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.NumberOfRisers.GetType()));
               }

               if (elem.NumberOfTreads != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "NUMBEROFTHREADS",
                     ref arrPropVal, ref arrPropValBS, elem.NumberOfTreads.ToString(), ref arrPDatatyp, "INTEGER", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.NumberOfTreads.GetType()));

               if (elem.RiserHeight != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "RISERHEIGHT",
                     ref arrPropVal, ref arrPropValBS, elem.RiserHeight.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.RiserHeight.GetType()));

               if (elem.TreadLength!= null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "TREADLENGTH",
                     ref arrPropVal, ref arrPropValBS, elem.TreadLength.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.TreadLength.GetType()));
#endif
#if POSTGRES
               if (isIfc2x3)
               {
                  if (elem2x3.NumberOfRiser != null)
                     insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "NUMBEROFRISER", elem2x3.NumberOfRiser.ToString(),
                        "INTEGER", BIMRLUtils.getDefaultIfcUnitStr(elem2x3.NumberOfRiser.GetType()));
               }
               else
               {
                  if (elem.NumberOfRisers != null)
                     insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "NUMBEROFRISERS", elem.NumberOfRisers.ToString(),
                        "INTEGER", BIMRLUtils.getDefaultIfcUnitStr(elem.NumberOfRisers.GetType()));
               }

               if (elem.NumberOfTreads != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "NUMBEROFTHREADS", elem.NumberOfTreads.ToString(),
                     "INTEGER", BIMRLUtils.getDefaultIfcUnitStr(elem.NumberOfTreads.GetType()));

               if (elem.RiserHeight != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "RISERHEIGHT", elem.RiserHeight.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.RiserHeight.GetType()));

               if (elem.TreadLength != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "TREADLENGTH", elem.TreadLength.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.TreadLength.GetType()));
#endif
            }

            else if (el is IIfcTendon)
            {
               IIfcTendon elem = el as IIfcTendon;

#if ORACLE
               if (elem.PredefinedType != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                     ref arrPropVal, ref arrPropValBS, elem.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);

               if (elem.NominalDiameter != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "NOMINALDIAMETER",
                     ref arrPropVal, ref arrPropValBS, elem.NominalDiameter.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.NominalDiameter.GetType()));

               if (elem.CrossSectionArea != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "CROSSSECTIONAREA",
                     ref arrPropVal, ref arrPropValBS, elem.CrossSectionArea.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.CrossSectionArea.GetType()));

               if (elem.TensionForce != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "TENSIONFORCE",
                     ref arrPropVal, ref arrPropValBS, elem.TensionForce.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.TensionForce.GetType()));

               if (elem.PreStress != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PRESTRESS",
                     ref arrPropVal, ref arrPropValBS, elem.PreStress.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.PreStress.GetType()));

               if (elem.FrictionCoefficient != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "FRICTIONCOEFFICIENT",
                     ref arrPropVal, ref arrPropValBS, elem.FrictionCoefficient.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.FrictionCoefficient.GetType()));

               if (elem.AnchorageSlip != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "ANCHORAGESLIP",
                     ref arrPropVal, ref arrPropValBS, elem.AnchorageSlip.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.AnchorageSlip.GetType()));

               if (elem.MinCurvatureRadius != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "MINCURVATURERADIUS",
                     ref arrPropVal, ref arrPropValBS, elem.MinCurvatureRadius.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.MinCurvatureRadius.GetType()));
#endif
#if POSTGRES
               if (elem.PredefinedType != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elem.PredefinedType.ToString(),
                     "STRING", string.Empty);

               if (elem.NominalDiameter != null)
               insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "NOMINALDIAMETER", elem.NominalDiameter.ToString(),
                  "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.NominalDiameter.GetType()));

               if (elem.CrossSectionArea != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "CROSSSECTIONAREA", elem.CrossSectionArea.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.CrossSectionArea.GetType()));

               if (elem.TensionForce != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "TENSIONFORCE", elem.TensionForce.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.TensionForce.GetType()));

               if (elem.PreStress != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PRESTRESS", elem.PreStress.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.PreStress.GetType()));

               if (elem.FrictionCoefficient != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "FRICTIONCOEFFICIENT", elem.FrictionCoefficient.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.FrictionCoefficient.GetType()));

               if (elem.AnchorageSlip != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "ANCHORAGESLIP", elem.AnchorageSlip.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.AnchorageSlip.GetType()));

               if (elem.MinCurvatureRadius != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "MINCURVATURERADIUS", elem.MinCurvatureRadius.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.MinCurvatureRadius.GetType()));
#endif
            }

            else if (el is IIfcTendonAnchor)
            {
               IIfcTendonAnchor elem = el as IIfcTendonAnchor;

#if ORACLE
               if (elem.SteelGrade != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "STEELGRADE",
                     ref arrPropVal, ref arrPropValBS, elem.SteelGrade.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);

               if (!isIfc2x3)
               {
                  if (elem.PredefinedType != null)
                     insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                        ref arrPropVal, ref arrPropValBS, elem.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }
#endif
#if POSTGRES
               if (elem.SteelGrade != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "STEELGRADE", elem.SteelGrade.ToString(),
                     "STRING", string.Empty);

               if (!isIfc2x3)
                  if (elem.PredefinedType != null)
                     insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elem.PredefinedType.ToString(),
                        "STRING", string.Empty);
#endif
            }

            else if (el is IIfcTransportElement)
            {
               IIfcTransportElement elem = el as IIfcTransportElement;
               Xbim.Ifc2x3.Interfaces.IIfcTransportElement elem2x3 = el as Xbim.Ifc2x3.Interfaces.IIfcTransportElement;

#if ORACLE
               if (isIfc2x3)
               {
                  arrPropName.Add("OPERATIONTYPE");
                  arrPropName.Add("CAPACITYBYWEIGHT");
                  arrPropName.Add("CAPACITYBYNUMBER");

                  if (elem2x3.OperationType != null)
                     insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "OPERATIONTYPE",
                        ref arrPropVal, ref arrPropValBS, elem2x3.OperationType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);

                  if (elem2x3.CapacityByWeight != null)
                     insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "CAPACITYBYWEIGHT",
                        ref arrPropVal, ref arrPropValBS, elem2x3.CapacityByWeight.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem2x3.CapacityByWeight.GetType()));

                  if (elem2x3.CapacityByNumber != null)
                     insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "CAPACITYBYNUMBER",
                        ref arrPropVal, ref arrPropValBS, elem2x3.CapacityByNumber.ToString(), ref arrPDatatyp, "INTEGER", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem2x3.CapacityByNumber.GetType()));
               }
               else
               {
                  if (elem.PredefinedType != null)
                     insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "PREDEFINEDTYPE",
                        ref arrPropVal, ref arrPropValBS, elem.PredefinedType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, string.Empty);
               }
#endif
#if POSTGRES
               if (isIfc2x3)
               {
                  if (elem2x3.OperationType != null)
                     insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "OPERATIONTYPE", elem2x3.OperationType.ToString(),
                        "STRING", string.Empty);

                  if (elem2x3.CapacityByWeight != null)
                     insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "CAPACITYBYWEIGHT", elem2x3.CapacityByWeight.ToString(),
                        "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem2x3.CapacityByWeight.GetType()));

                  if (elem2x3.CapacityByNumber != null)
                     insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "CAPACITYBYNUMBER", elem2x3.CapacityByNumber.ToString(),
                        "INTEGER", BIMRLUtils.getDefaultIfcUnitStr(elem2x3.CapacityByNumber.GetType()));
               }
               else
                  if (elem.PredefinedType != null)
                     insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "PREDEFINEDTYPE", elem.PredefinedType.ToString(),
                        "STRING", string.Empty);
#endif
            }

            else if (el is IIfcWindow)
            {
               IIfcWindow elem = el as IIfcWindow;

#if ORACLE
               if (elem.OverallHeight != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "OVERALLHEIGHT",
                     ref arrPropVal, ref arrPropValBS, elem.OverallHeight.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.OverallHeight.GetType()));

               if (elem.OverallWidth != null)
                  insertProperty(ref arrEleGuid, el.GlobalId, ref arrPGrpName, "IFCATTRIBUTES", ref arrPropName, "OVERALLWIDTH",
                     ref arrPropVal, ref arrPropValBS, elem.OverallWidth.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, BIMRLUtils.getDefaultIfcUnitStr(elem.OverallWidth.GetType()));
#endif
#if POSTGRES
               if (elem.OverallHeight != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "OVERALLHEIGHT", elem.OverallHeight.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.OverallHeight.GetType()));

               if (elem.OverallWidth != null)
                  insertProperty(command, el.GlobalId, "IFCATTRIBUTES", "OVERALLWIDTH", elem.OverallWidth.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(elem.OverallWidth.GetType()));
#endif
            }

            else if (el is IIfcDistributionPort)
            {
               // We will deal with IfcDistributionPort in a special way. We temporarily keep the port information in a dict.
               // Later one we will match the port to the element when processing IfcRelConenctsPortToElement, and update the dictionary value
               // The actual relationship will be inserted upon processing IfcRelConnectsPorts
               // --> This is done because using inverse (even it works) is too SLOW!!
               IIfcDistributionPort dPort = el as IIfcDistributionPort;

               Dictionary<string,string> portElemVal = new Dictionary<string,string>();
               if (dPort.FlowDirection != null)
               {
                  portElemVal.Add("ATTRIBUTENAME", "FlowDirection");
                  portElemVal.Add("ATTRIBUTEVALUE", dPort.FlowDirection.ToString());
               }
               _refBIMRLCommon.PortToElemAdd(dPort.GlobalId.ToString(), portElemVal);
            }
            else
            {
               // not supported Type
            }

            /* 
            **** Now process all other properties from property set(s)
            */
            // bimrlProp.processElemProperties(el);

#if ORACLE
            if (arrEleGuid.Count >= DBOperation.commitInterval)
            {

               Param[0].Value = arrEleGuid.ToArray();
               Param[0].Size = arrEleGuid.Count;
               Param[1].Value = arrPGrpName.ToArray();
               Param[1].Size = arrPGrpName.Count;
               Param[2].Value = arrPropName.ToArray();
               Param[2].Size = arrPropName.Count;
               Param[3].Value = arrPropVal.ToArray();
               Param[3].Size = arrPropVal.Count;
               Param[3].ArrayBindStatus = arrPropValBS.ToArray();
               Param[4].Value = arrPDatatyp.ToArray();
               Param[4].Size = arrPDatatyp.Count;
               Param[5].Value = arrPUnit.ToArray();
               Param[5].Size = arrPUnit.Count;
               Param[5].ArrayBindStatus = arrPUnitBS.ToArray();

               try
               {
                  command.ArrayBindCount = arrEleGuid.Count;    // No of values in the array to be inserted
                  int commandStatus = command.ExecuteNonQuery();
                  DBOperation.commitTransaction();
                  arrEleGuid.Clear();
                  arrPGrpName.Clear();
                  arrPropName.Clear();
                  arrPropVal.Clear();
                  arrPropValBS.Clear();
                  arrPDatatyp.Clear();
                  arrPUnit.Clear();
                  arrPUnitBS.Clear();
               }
               catch (OracleException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushIgnorableError(excStr);
                  // Ignore any error
                  arrEleGuid.Clear();
                  arrPGrpName.Clear();
                  arrPropName.Clear();
                  arrPropVal.Clear();
                  arrPropValBS.Clear();
                  arrPDatatyp.Clear();
                  arrPUnit.Clear();
                  arrPUnitBS.Clear();
                  continue;
               }
               catch (SystemException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushError(excStr);
                  throw;
               }
            }
         }

         if (arrEleGuid.Count > 0)
         {
            Param[0].Value = arrEleGuid.ToArray();
            Param[0].Size = arrEleGuid.Count;
            Param[1].Value = arrPGrpName.ToArray();
            Param[1].Size = arrPGrpName.Count;
            Param[2].Value = arrPropName.ToArray();
            Param[2].Size = arrPropName.Count;
            Param[3].Value = arrPropVal.ToArray();
            Param[3].Size = arrPropVal.Count;
            Param[3].ArrayBindStatus = arrPropValBS.ToArray();
            Param[4].Value = arrPDatatyp.ToArray();
            Param[4].Size = arrPDatatyp.Count;
            Param[5].Value = arrPUnit.ToArray();
            Param[5].Size = arrPUnit.Count;
            Param[5].ArrayBindStatus = arrPUnitBS.ToArray();

            try
            {
               command.ArrayBindCount = arrEleGuid.Count;    // No of values in the array to be inserted
               int commandStatus = command.ExecuteNonQuery();
               DBOperation.commitTransaction();
            }
            catch (OracleException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushIgnorableError(excStr);
               // Ignore any error
            }
            catch (SystemException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushError(excStr);
               throw;
            }
#endif
         }

         DBOperation.commitTransaction();
         command.Dispose();

         // Now Process all properties from property sets (and quantity sets) in one go for performance reason
         bimrlProp.processAllElemProperties(_model);
      }

#if ORACLE
      private void insertProperty(ref List<string> arrGUID, string elementid, ref List<string> arrPGroup, string propGroup, 
                                 ref List<string> arrPName, string propName, ref List<string> arrPValue, ref List<OracleParameterStatus> arrPropValBS, string propValue, 
                                 ref List<string> arrPDataType, string propDataType, ref List<string> arrUOM, ref List<OracleParameterStatus> arrPUnitBS, string uom)
      {
         arrGUID.Add(elementid);
         arrPGroup.Add(propGroup);
         arrPName.Add(propName);
         arrPValue.Add(propValue);
         if (string.IsNullOrEmpty(propValue))
            arrPropValBS.Add(OracleParameterStatus.NullInsert);
         else
            arrPropValBS.Add(OracleParameterStatus.Success);
         arrPDataType.Add(propDataType);
         arrUOM.Add(uom);
         if (string.IsNullOrEmpty(uom))
            arrPUnitBS.Add(OracleParameterStatus.NullInsert);
         else
            arrPUnitBS.Add(OracleParameterStatus.Success);
      }
#endif
#if POSTGRES
      private void insertProperty(NpgsqlCommand command, string elementid, string propGroup, string propName, string propValue, string propDataType, string uom)
      {
         command.Parameters["@eid"].Value = elementid;
         command.Parameters["@gname"].Value = propGroup;
         command.Parameters["@pname"].Value = propName;
         if (string.IsNullOrEmpty(propValue))
            command.Parameters["@pvalue"].Value = DBNull.Value;
         else
            command.Parameters["@pvalue"].Value = propValue;
         if (string.IsNullOrEmpty(propDataType))
            command.Parameters["@pdtyp"].Value = DBNull.Value;
         else
            command.Parameters["@pdtyp"].Value = propDataType;
         if (string.IsNullOrEmpty(uom))
            command.Parameters["@punit"].Value = DBNull.Value;
         else
            command.Parameters["@punit"].Value = uom;

         try
         {
            DBOperation.CurrTransaction.Save(DBOperation.def_savepoint);
            int commandStatus = command.ExecuteNonQuery();
            DBOperation.CurrTransaction.Release(DBOperation.def_savepoint);
         }
         catch (NpgsqlException e)
         {
            // Ignore error and continue
            _refBIMRLCommon.StackPushIgnorableError(string.Format("Error inserting (\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\"); {6})", elementid, propGroup, propName,
               propValue, propDataType, uom, e.Message));
            DBOperation.CurrTransaction.Rollback(DBOperation.def_savepoint);
         }
      }
#endif
   }
}
