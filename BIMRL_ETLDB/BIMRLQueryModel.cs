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
    public struct BIMRLModelInfo
    {
        public int ModelID { get; set; }
        public string ModelName { get; set; }
        public string Source { get; set; }
        public XbimPoint3D? Location { get; set; }
        public XbimMatrix3D? Transformation { get; set; }
        public XbimVector3D? Scale { get; set; }
        public int NumberOfElement { get; set; }
    }

   public class BIMRLQueryModel
   {
      BIMRLCommon _refBIMRLCommon;

      public BIMRLQueryModel(BIMRLCommon refBIMRLCommon)
      {
         _refBIMRLCommon = refBIMRLCommon;
      }

      public List<FederatedModelInfo> getFederatedModels()
      {
         List<FederatedModelInfo> fedModels = new List<FederatedModelInfo>();

         DBOperation.beginTransaction();
         string currStep = string.Empty;
                     
         try
         {
            string sqlStmt = "select federatedID, ModelName, ProjectNumber, ProjectName, WORLDBBOX, MAXOCTREELEVEL, LastUpdateDate, Owner, DBConnection from BIMRL_FEDERATEDMODEL order by federatedID";
#if ORACLE
            OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
            OracleDataReader reader = command.ExecuteReader();
            SdoGeometry worldBB = new SdoGeometry();
#endif
#if POSTGRES
            NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
            NpgsqlConnection.MapCompositeGlobally<Point3D>("point3d");
            NpgsqlConnection.MapCompositeGlobally<GeometryTypeEnum>("geom3dtype");
            command.Prepare();
            NpgsqlDataReader reader = command.ExecuteReader();
#endif
            while (reader.Read())
            {
               FederatedModelInfo fedModel = new FederatedModelInfo();
               fedModel.FederatedID = reader.GetInt32(0);
               fedModel.ModelName = reader.GetString(1);
               fedModel.ProjectNumber = reader.GetString(2);
               fedModel.ProjectName = reader.GetString(3);
               if (!reader.IsDBNull(4))
               {
#if ORACLE
                  worldBB = reader.GetValue(4) as SdoGeometry;
                  Point3D LLB = new Point3D(worldBB.OrdinatesArrayOfDoubles[0], worldBB.OrdinatesArrayOfDoubles[1], worldBB.OrdinatesArrayOfDoubles[2]);
                  Point3D URT = new Point3D(worldBB.OrdinatesArrayOfDoubles[3], worldBB.OrdinatesArrayOfDoubles[4], worldBB.OrdinatesArrayOfDoubles[5]);
                  fedModel.WorldBoundingBox = LLB.ToString() + " " + URT.ToString();
#endif
#if POSTGRES
                  Point3D[] wBbox = reader.GetFieldValue<Point3D[]>(4);
                  BoundingBox3D worldBB = new BoundingBox3D(wBbox[0], wBbox[1]);
                  fedModel.WorldBoundingBox = worldBB.LLB.ToString() + " " + worldBB.URT.ToString();
#endif
               }
               if (!reader.IsDBNull(5))
                  fedModel.OctreeMaxDepth = reader.GetInt16(5);
               if (!reader.IsDBNull(6))
                  fedModel.LastUpdateDate = reader.GetDateTime(6);
               if (!reader.IsDBNull(7))
                  fedModel.Owner = reader.GetString(7);
               if (!reader.IsDBNull(8))
                  fedModel.DBConnection = reader.GetString(8);

               fedModels.Add(fedModel);
            }
            reader.Close();
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

         return fedModels;
      }

      public List<BIMRLModelInfo> getModelInfos(int fedModelID)
      {
         List<BIMRLModelInfo> modelInfos = new List<BIMRLModelInfo>();

         DBOperation.beginTransaction();
         string currStep = string.Empty;

         try
         {
            string sqlStmt = "Select b.ModelID, a.ModelName, a.Source, count(b.modelid) as \"No. Element\" from " + DBOperation.formatTabName("BIMRL_MODELINFO", fedModelID)
                           + " a, " + DBOperation.formatTabName("BIMRL_ELEMENT", fedModelID) + " b WHERE b.modelid=a.modelid "
                           + "group by b.modelid, modelname, source order by ModelID";
#if ORACLE
            OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
            OracleDataReader reader = command.ExecuteReader();
            SdoGeometry worldBB = new SdoGeometry();
#endif
#if POSTGRES
            NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
            command.Prepare();
            NpgsqlDataReader reader = command.ExecuteReader();
#endif
            while (reader.Read())
            {
               BIMRLModelInfo modelInfo = new BIMRLModelInfo();
               modelInfo.ModelID = reader.GetInt32(0);
               modelInfo.ModelName = reader.GetString(1);
               modelInfo.Source = reader.GetString(2);
               modelInfo.NumberOfElement = reader.GetInt32(3);
               //if (!reader.IsDBNull(3))
                  //modelInfo.Location = (XbimPoint3D) reader.GetValue(3);
               //if (!reader.IsDBNull(4))
                  //modelInfo.Transformation = (XbimMatrix3D)reader.GetValue(4);
               //if (!reader.IsDBNull(5))
                  //modelInfo.Scale = (XbimVector3D)reader.GetValue(5);

               modelInfos.Add(modelInfo);
            }
            reader.Close();
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

         return modelInfos;
      }

      public void deleteModel (int federatedModelID)
      {
         string currStep = "Dropping existing model tables (ID: " + federatedModelID.ToString("X4") + ")";
         try
         {
            int retStat = DBOperation.dropModelTables(federatedModelID);

            string sqlStmt = "delete from BIMRL_FEDERATEDMODEL where FEDERATEDID=" + federatedModelID;
#if ORACLE
            OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
#endif
#if POSTGRES
            NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
            command.Prepare();
#endif
            DBOperation.beginTransaction();
            //OracleTransaction txn = DBOperation.DBconnShort.BeginTransaction();
            command.ExecuteNonQuery();
            DBOperation.commitTransaction();
            //txn.Commit();
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
      }

      public void deleteModel (string modelName, string projectName, string projectNumber)
      {
         object federatedModelID = null;
         string sqlStmt = "Select FEDERATEDID from BIMRL_FEDERATEDMODEL where MODELNAME = '" + modelName + "' and PROJECTNAME = '" + projectName + "' and PROJECTNUMBER = '" + projectNumber + "'";
         try
         {
#if ORACLE
            OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
#endif
#if POSTGRES
            NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
            command.Prepare();
#endif
            federatedModelID = command.ExecuteScalar();
            int? fedID = federatedModelID as int?;
            if (fedID.HasValue)
               deleteModel(fedID.Value);
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
      }

      public DataTable checkModelExists (int fedID)
      {
         string whereClause = "FEDERATEDID=" + fedID;
         DataTable modelInfo = new DataTable();
         bool exist = checkModelExists(whereClause, out modelInfo);
         return modelInfo;
      }

      public DataTable checkModelExists(string projectName, string projectNumber)
      {
         string whereClause = "PROJECTNAME='" + projectName + "'" + " AND PROJECTNUMBER='" + projectNumber + "'";
         DataTable modelInfo = new DataTable();
         bool exist = checkModelExists(whereClause, out modelInfo);
         return modelInfo;
      }

      bool checkModelExists(string whereClause, out DataTable modelInfo)
      {
         DataTable qResult = new DataTable();
         modelInfo = qResult;

         string sqlStmt = "Select * from BIMRL_FEDERATEDMODEL where " + whereClause;
         try
         {
#if ORACLE
            OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
            OracleDataAdapter qAdapter = new OracleDataAdapter(command);
#endif
#if POSTGRES
            NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
            command.Prepare();
            NpgsqlDataAdapter qAdapter = new NpgsqlDataAdapter(command);
#endif
            qAdapter.Fill(qResult);
            if (qResult != null)
               if (qResult.Rows.Count > 0)
                  return true;
            return false;
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
            return false;
         }
      }
   }
}
