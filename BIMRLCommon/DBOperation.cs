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
using System.Collections;
using System.IO;
using System.Reflection;
#if ORACLE
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;
using NetSdoGeometry;
#endif
#if POSTGRES
using Npgsql.PostgresTypes;
using NpgsqlTypes;
using Npgsql;
#endif


namespace BIMRL.Common
{
   public static class DBOperation
   {
      private static string m_connStr;
#if ORACLE
      public static readonly string ScriptPath = "script_ora";
      private static OracleTransaction m_longTrans;
#endif
#if POSTGRES
      public static readonly string ScriptPath = "script_pg";
      private static NpgsqlTransaction m_longTrans;
      public static NpgsqlTransaction CurrTransaction
      {
         get
         {
            if (!transactionActive || m_longTrans.IsCompleted)
               beginTransaction();
            return m_longTrans;
         }
      }
      public static string def_savepoint = "def_savepoint";
#endif
      private static bool transactionActive = false;
      private static int currInsertCount = 0;
      public static int commitInterval { get; set; } = 1;
      public static string operatorToUse { get; set; }
      public static string DBUserID { get; set; }
      public static string DBPassword { get; set; }
      public static string DBConnectstring { get; set; }
      public static BIMRLCommon refBIMRLCommon { get; set; }
      public static projectUnit currModelProjectUnitLength = projectUnit.SIUnit_Length_Meter;
      public static Dictionary<string, bool> objectForSpaceBoundary = new Dictionary<string, bool>();
      public static Dictionary<string, bool> objectForConnection = new Dictionary<string, bool>();
      public static int currSelFedID { get; set; }
      private static Dictionary<int, Tuple<Point3D, Point3D, int>> worldBBInfo = new Dictionary<int, Tuple<Point3D, Point3D, int>>();
      public static bool UIMode {get; set;} = true;
      static FederatedModelInfo _FederatedModelInfo;

      public static void ConnectToDB(string username, string password, string connectstring)
      {
         try
         {
            m_DBconn = Connect(username, password, connectstring);
         }
#if ORACLE
         catch (OracleException e)
#endif
#if POSTGRES
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Error - " + e.Message + "\n\t";
            refBIMRLCommon.StackPushError(excStr);
#if POSTGRES
            DBOperation.rollbackTransaction();
#endif
            throw;
         }
      }

#if ORACLE
      static OracleConnection Connect(string username, string password, string DBconnectstring)
      { 
         username = username.ToUpper();
#endif
#if POSTGRES
      static NpgsqlConnection Connect(string username, string password, string DBconnectstring)
      {
#endif
         if (!username.Equals(DBUserID) && m_DBconn != null)
               Disconnect();     // Disconnected first if previously connected but with different user

#if ORACLE
         DBUserID = username.ToUpper();
#endif
#if POSTGRES
         DBUserID = username;
#endif
         DBPassword = password;
         DBConnectstring = DBconnectstring;

         if (m_DBconn != null)
         {
            if (m_DBconn.State != ConnectionState.Open)
               m_DBconn.Open();

            return m_DBconn;             // already connected
         }

#if ORACLE
         string constr = "User Id=" + username + ";Password=" + password + ";Data Source=" + DBconnectstring;
#endif
#if POSTGRES
         // The DBconnectstring is expected in form of: server=<server name, e.g. localhost>; port=<port no, e.g. 5432>; database=<db name, e.g. postgres>
         string constr = "User Id=" + username + ";Password=" + password + ";" + DBconnectstring;
#endif
         string currStep = string.Empty;
         try
         {
            currStep = "Connecting to DB using: " + constr;
#if ORACLE
            m_DBconn = new OracleConnection(constr);
#endif
#if POSTGRES
            // This must be placed before connection is called
            NpgsqlConnection.MapCompositeGlobally<Point3D>("point3d");
            //NpgsqlConnection.MapCompositeGlobally<CoordSystem>("coordsystem");
            NpgsqlConnection.MapEnumGlobally<GeometryTypeEnum>("geom3dtype");
            m_DBconn = new NpgsqlConnection(constr);
#endif
            currStep = "Opening DB connection using: " + constr;
            m_DBconn.Open();
               m_connStr = constr;
            transactionActive = false;
            m_longTrans = null;

            // Need to create the temporary tables for each session
            try
            {
               beginTransaction();
               ExecuteSystemScript(0, "bimrl_addgeom.sql");
               commitTransaction();
            }
            catch
            {
               rollbackTransaction();
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
            refBIMRLCommon.StackPushError(excStr);
#if POSTGRES
            DBOperation.rollbackTransaction();
#endif
            throw;
         }
         return m_DBconn;
      }

      public static void ExistingOrDefaultConnection()
      {
         if (m_DBconn != null)
               return ;             // already connected

         // default connection
#if ORACLE
         string defaultConnecstring = "pdborcl";
         string defaultUser = "BIMRL";
         string defaultPassword = "bimrl";
#endif
#if POSTGRES
         string defaultConnecstring = "server=localhost; port=5432; database=postgres; CommandTimeout=0";
         string defaultUser = "bimrl";
         string defaultPassword = "bimrl";
#endif

         try
         {
            if (string.IsNullOrEmpty(DBUserID))
            {
               m_DBconn = Connect(defaultUser, defaultPassword, defaultConnecstring);
            }
         }
#if ORACLE
         catch (OracleException e)
#endif
#if POSTGRES
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + defaultUser + "@" + defaultConnecstring;
            refBIMRLCommon.StackPushError(excStr);
#if POSTGRES
            DBOperation.rollbackTransaction();
#endif
            throw;
          }
        }

#if ORACLE
      private static OracleConnection m_DBconn;
      public static OracleConnection DBConn
#endif

#if POSTGRES
#region arbitraryConnection
         public static NpgsqlConnection arbitraryConnection()
         {
            if (!string.IsNullOrEmpty(DBUserID) && !string.IsNullOrEmpty(DBPassword) && !string.IsNullOrEmpty(DBConnectstring))
            {
               string constr = "User Id=" + DBUserID + ";Password=" + DBPassword + ";" + DBConnectstring + ";CommandTimeout = 0";
               try
               {
                  NpgsqlConnection arbConn = new NpgsqlConnection(constr);
                  arbConn.Open();
                  return arbConn;
               }
               catch (NpgsqlException e)
               {
                  string excStr = "%%Error - " + e.Message + "\n\t" + constr;
                  refBIMRLCommon.StackPushError(excStr);
                  DBOperation.rollbackTransaction();
                  throw;
               }
            }
            else
               return null;
         }
#endregion

#region 2ndConnection
         private static NpgsqlConnection m_DBconn2 = null;
         private static NpgsqlConnection DBConn2
         {
            get
            {
               if (m_DBconn2 == null)
               {
                  if (!string.IsNullOrEmpty(DBUserID) && !string.IsNullOrEmpty(DBPassword) && !string.IsNullOrEmpty(DBConnectstring))
                  {
                     string constr = "User Id=" + DBUserID + ";Password=" + DBPassword + ";" + DBConnectstring + ";CommandTimeout=0";
                     m_DBconn2 = new NpgsqlConnection(constr);
                     m_DBconn2.Open();
                     try
                     {
                        beginTransaction();
                        NpgsqlCommand cmd = new NpgsqlCommand("create temporary sequence seq_geomid", m_DBconn2);
                        cmd.ExecuteNonQuery();
                        commitTransaction();
                     }
                     catch
                     {
                        rollbackTransaction();
                     }
                  }
               }
               else if (m_DBconn2.State != ConnectionState.Open)
                  m_DBconn2.Open();

               return m_DBconn2;
            }
         }


      /// <summary>
      /// This method allows execution of a scalar statement that will be executed by a 2nd transaction (usually short duration)
      /// </summary>
      /// <param name="stmt">the SQL statement to be executed. If there are parameters specified, the "name" should be using the index, e.g. "@0", "@1", etc.</param>
      /// <param name="stmtParams">parameter values</param>
      /// <param name="commit">commit or rollback</param>
      /// <returns></returns>
      public static object ExecuteScalar(string stmt, IList<object> stmtParams = null, IList<NpgsqlDbType> paramTypeList = null, bool commit = false)
      {
         bool paramHasType = false;
         NpgsqlCommand cmd = new NpgsqlCommand(stmt, DBConn);
         if (stmtParams != null)
         {
            if (stmtParams.Count > 0)
            {
               if (paramTypeList != null)
                  paramHasType = (stmtParams.Count == paramTypeList.Count);
               for (int i = 0; i < stmtParams.Count; ++i)
               {
                  if (paramHasType && paramTypeList[i] != NpgsqlDbType.Unknown)
                     cmd.Parameters.AddWithValue("@" + i.ToString(), paramTypeList[i], stmtParams[i]);
                  else
                     cmd.Parameters.AddWithValue("@" + i.ToString(), stmtParams[i]);
               }
            }
         }
         try
         {
            object retVal = cmd.ExecuteScalar();
            if (commit)
               DBOperation.commitTransaction();
            else
               DBOperation.rollbackTransaction();
            cmd.Dispose();
            return retVal;
         }
         catch (NpgsqlException e)
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + stmt;
            refBIMRLCommon.StackPushError(excStr);
            DBOperation.rollbackTransaction();
            cmd.Dispose();
            throw;
         }
      }

      /// <summary>
      /// This method allows execution of a scalar statement that will be executed by a 2nd transaction (usually short duration)
      /// </summary>
      /// <param name="stmt">the SQL statement to be executed. If there are parameters specified, the "name" should be using the index, e.g. "@0", "@1", etc.</param>
      /// <param name="stmtParams">parameter values</param>
      /// <param name="commit">commit or rollback</param>
      /// <returns></returns>
      public static object ExecuteScalarWithTrans2(string stmt, IList<object> stmtParams = null, IList<NpgsqlDbType> paramTypeList = null, bool commit = false)
         {
            bool paramHasType = false;
            NpgsqlTransaction shortTrans = DBConn2.BeginTransaction();
            NpgsqlCommand cmdShort = new NpgsqlCommand(stmt, DBConn2);
            if (stmtParams != null)
            {
               if (stmtParams.Count > 0)
               {
                  if (paramTypeList != null)
                     paramHasType = (stmtParams.Count == paramTypeList.Count);
                  for (int i = 0; i < stmtParams.Count; ++i)
                  {
                     if (paramHasType && paramTypeList[i] != NpgsqlDbType.Unknown)
                        cmdShort.Parameters.AddWithValue("@" + i.ToString(), paramTypeList[i], stmtParams[i]);
                     else
                        cmdShort.Parameters.AddWithValue("@" + i.ToString(), stmtParams[i]);
                  }
               }
            }
            try
            { 
               object retVal = cmdShort.ExecuteScalar();
               if (commit)
                  shortTrans.Commit();
               else
                  shortTrans.Rollback();
               cmdShort.Dispose();
               return retVal;
            }
            catch (NpgsqlException e)
            {
               string excStr = "%%Error - " + e.Message + "\n\t" + stmt;
               refBIMRLCommon.StackPushError(excStr);
               shortTrans.Rollback();
               cmdShort.Dispose();
               throw;
            }
         }

      /// <summary>
      /// This method allows execution of an atomic statement (non query)
      /// </summary>
      /// <param name="stmt">the SQL statement to be executed. If there are parameters specified, the "name" should be using the index, e.g. "@0", "@1", etc.</param>
      /// <param name="stmtParams">parameter values</param>
      /// <returns>success or fail</returns>
      public static void ExecuteNonQuery(string stmt, IList<object> stmtParams = null, IList<NpgsqlDbType> paramTypeList = null, bool commit = true)
      {
         NpgsqlCommand cmd = new NpgsqlCommand(stmt, DBConn);
         bool paramHasType = false;

         if (stmtParams != null)
         {
            if (stmtParams.Count > 0)
            {
               if (paramTypeList != null)
                  paramHasType = (stmtParams.Count == paramTypeList.Count);

               for (int i = 0; i < stmtParams.Count; ++i)
               {
                  if (paramHasType && paramTypeList[i] != NpgsqlDbType.Unknown)
                     cmd.Parameters.AddWithValue("@" + i.ToString(), paramTypeList[i], stmtParams[i]);
                  else
                     cmd.Parameters.AddWithValue("@" + i.ToString(), stmtParams[i]);
               }
            }
         }
         try
         {
            int cmdStatus = cmd.ExecuteNonQuery();
            if (commit)
               DBOperation.commitTransaction();
            else
               DBOperation.rollbackTransaction();
            cmd.Dispose();
            return;
         }
         catch (NpgsqlException e)
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + stmt;
            refBIMRLCommon.StackPushError(excStr);
            DBOperation.rollbackTransaction();
            cmd.Dispose();
            //throw;
         }
      }

      /// <summary>
      /// This method allows execution of an atomic statement (non query) that will be executed by a 2nd transaction (usually short duration)
      /// </summary>
      /// <param name="stmt">the SQL statement to be executed. If there are parameters specified, the "name" should be using the index, e.g. "@0", "@1", etc.</param>
      /// <param name="stmtParams">parameter values</param>
      /// <returns>success or fail</returns>
      public static void ExecuteNonQueryWithTrans2(string stmt, IList<object> stmtParams=null, IList<NpgsqlDbType> paramTypeList=null, bool commit=true)
      {
         NpgsqlTransaction shortTrans = DBConn2.BeginTransaction();
         NpgsqlCommand cmdShort = new NpgsqlCommand(stmt, DBConn2);
         bool paramHasType = false;

         if (stmtParams != null)
         {
            if (stmtParams.Count > 0)
            {
               if (paramTypeList != null)
                  paramHasType = (stmtParams.Count == paramTypeList.Count);
                 
               for (int i=0; i<stmtParams.Count; ++i)
               {
                  if (paramHasType && paramTypeList[i] != NpgsqlDbType.Unknown)
                     cmdShort.Parameters.AddWithValue("@" + i.ToString(), paramTypeList[i] ,stmtParams[i]);
                  else
                     cmdShort.Parameters.AddWithValue("@" + i.ToString(), stmtParams[i]);
               }
            }
         }
         try
         {
            int cmdStatus = cmdShort.ExecuteNonQuery();
            if (commit)
               shortTrans.Commit();
            else
               shortTrans.Rollback();
            cmdShort.Dispose();
            return;
         }
         catch (NpgsqlException e)
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + stmt;
            refBIMRLCommon.StackPushError(excStr);
            shortTrans.Rollback();
            cmdShort.Dispose();
            //throw;
         }
      }

      /// <summary>
      /// This method allows execution of an atomic statement into a DataTable
      /// </summary>
      /// <param name="stmt">the SQL statement to be executed. If there are parameters specified, the "name" should be using the index, e.g. "@0", "@1", etc.</param>
      /// <param name="stmtParams">parameter values</param>
      /// <returns>return DataTable or null</returns>
      public static DataTable ExecuteToDataTable(string stmt, IList<object> stmtParams = null, IList<NpgsqlDbType> paramTypeList = null)
      {
         bool paramHasType = false;
         DataTable qResult = new DataTable();
         NpgsqlCommand cmd = new NpgsqlCommand(stmt, DBOperation.DBConn);
         if (stmtParams != null)
         {
            if (stmtParams.Count > 0)
            {
               if (paramTypeList != null)
                  paramHasType = (stmtParams.Count == paramTypeList.Count);

               for (int i = 0; i < stmtParams.Count; ++i)
               {
                  if (paramHasType && paramTypeList[i] != NpgsqlDbType.Unknown)
                     cmd.Parameters.AddWithValue("@" + i.ToString(), paramTypeList[i], stmtParams[i]);
                  else
                     cmd.Parameters.AddWithValue("@" + i.ToString(), stmtParams[i]);
               }
            }
         }
         try
         {
            cmd.Prepare();
            NpgsqlDataAdapter qAdapter = new NpgsqlDataAdapter(cmd);
            qAdapter.Fill(qResult);
            cmd.Dispose();
            if (qResult != null)
               if (qResult.Rows.Count > 0)
                  return qResult;
            return null;
         }
         catch (NpgsqlException e)
         {
            string excStr = "%%Read Error - " + e.Message + "\n\t" + stmt;
            refBIMRLCommon.StackPushError(excStr);
            return null;
         }
      }

      /// <summary>
      /// This method allows execution of an atomic statement that will be executed by a 2nd transaction (usually short duration)
      /// </summary>
      /// <param name="stmt">the SQL statement to be executed. If there are parameters specified, the "name" should be using the index, e.g. "@0", "@1", etc.</param>
      /// <param name="stmtParams">parameter values</param>
      /// <returns>return DataTable or null</returns>
      public static DataTable ExecuteToDataTableWithTrans2(string stmt, IList<object> stmtParams = null, IList<NpgsqlDbType> paramTypeList=null)
      {
         bool paramHasType = false;
         NpgsqlTransaction shortTrans = DBConn2.BeginTransaction();
         DataTable qResult = new DataTable();
         NpgsqlCommand cmdShort = new NpgsqlCommand(stmt, DBOperation.DBConn2);
         if (stmtParams != null)
         {
            if (stmtParams.Count > 0)
            {
               if (paramTypeList != null)
                  paramHasType = (stmtParams.Count == paramTypeList.Count);

               for (int i = 0; i < stmtParams.Count; ++i)
               {
                  if (paramHasType && paramTypeList[i] != NpgsqlDbType.Unknown)
                     cmdShort.Parameters.AddWithValue("@" + i.ToString(), paramTypeList[i], stmtParams[i]);
                  else
                     cmdShort.Parameters.AddWithValue("@" + i.ToString(), stmtParams[i]);
               }
            }
         }
         try
         {
            cmdShort.Prepare();
            NpgsqlDataAdapter qAdapter = new NpgsqlDataAdapter(cmdShort);
            qAdapter.Fill(qResult);
            shortTrans.Rollback();
            cmdShort.Dispose();
            if (qResult != null)
               if (qResult.Rows.Count > 0)
                  return qResult;
            return null;
         }
         catch (NpgsqlException e)
         {
            string excStr = "%%Read Error - " + e.Message + "\n\t" + stmt;
            refBIMRLCommon.StackPushError(excStr);
            shortTrans.Rollback();
            cmdShort.Dispose();
            return null;
         }
      }
#endregion

      private static NpgsqlConnection m_DBconn;
      public static NpgsqlConnection DBConn
#endif
      {
         get 
         {
            if (m_DBconn == null)
               ExistingOrDefaultConnection();
            if (m_DBconn.State != ConnectionState.Open)
               m_DBconn.Open();

            return m_DBconn; 
         }
      }

      private static int m_OctreeSubdivLevel = 6;    // default value
      public static int OctreeSubdivLevel
      {
         get { return m_OctreeSubdivLevel; }
         set { m_OctreeSubdivLevel = value; }
      }

      private static bool m_OnepushETL = false;
      public static bool OnepushETL
      {
         get { return m_OnepushETL; }
         set { m_OnepushETL = value; }
      }

      public static int executeSingleStmt(string sqlStmt, bool commit=true)
      {
         int commandStatus = -1;
#if ORACLE
         OracleCommand command = new OracleCommand(sqlStmt, DBConn);
#endif
#if POSTGRES
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBConn);
#endif
         DBOperation.beginTransaction();
         try
         {
            commandStatus = command.ExecuteNonQuery();
            if (commit)
               DBOperation.commitTransaction();
         }
#if ORACLE
         catch (OracleException e)
#endif
#if POSTGRES
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + sqlStmt;
            refBIMRLCommon.StackPushError(excStr);
            command.Dispose();
         }
         command.Dispose();
         return commandStatus;
      }

      // these 3 methods must be done in series of 3: start with beginTransaction and ends with endTransaction
      public static void beginTransaction()
      {
#if ORACLE
         if (!transactionActive)
#endif
#if POSTGRES
         if (!transactionActive || m_longTrans.IsCompleted)
#endif
         {
            m_longTrans = DBConn.BeginTransaction();
               transactionActive = true;
         }
         currInsertCount = 0;    // reset the insert count
      }

      public static void commitTransaction()
      {
#if ORACLE
         if (transactionActive)
#endif
#if POSTGRES
         if (transactionActive && !m_longTrans.IsCompleted)
#endif
         {
               m_longTrans.Commit();
               m_longTrans = DBConn.BeginTransaction();
         }
      }

      public static void rollbackTransaction()
      {
#if ORACLE
         if (transactionActive)
#endif
#if POSTGRES
         if (transactionActive && !m_longTrans.IsCompleted)
#endif
         {
            m_longTrans.Rollback();
               m_longTrans = DBConn.BeginTransaction();
         }
      }

      public static int insertRow(string sqlStmt)
      {
#if ORACLE
         if (!transactionActive)
#endif
#if POSTGRES
         if (!transactionActive || m_longTrans.IsCompleted)
#endif
         {
            beginTransaction();
               //return -1;
               // no transaction opened
         }
         int commandStatus = -1;
#if ORACLE
         OracleCommand command = new OracleCommand(sqlStmt, DBConn);
#endif
#if POSTGRES
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBConn);
#endif
         string currStep = sqlStmt;

         try
         {
            commandStatus = command.ExecuteNonQuery();
            currInsertCount++;          // increment insert count

            if (currInsertCount % commitInterval == 0)
            {
            //Do commit at interval but keep the long transaction (reopen)
            commitTransaction();
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
            refBIMRLCommon.StackPushError(excStr);
#if POSTGRES
            DBOperation.rollbackTransaction();
#endif
            command.Dispose();
            throw;
         }
         command.Dispose();
         return commandStatus;
      }

      public static void endTransaction(bool commit)
      {
#if ORACLE
         if (transactionActive)
#endif
#if POSTGRES
         if (transactionActive && !m_longTrans.IsCompleted)
#endif
         {
            if (commit)
               m_longTrans.Commit();
            else
               m_longTrans.Rollback();
         }
         transactionActive = false;
         currInsertCount = 0;
      }

      public static void Disconnect()
      {
         if (m_DBconn != null)
         {
            m_DBconn.Close();
            //m_DBconn.Dispose();
            m_DBconn = null;
         }
      }

      public static FederatedModelInfo getFederatedModelByID (int FedID)
      {
         FederatedModelInfo fedModel = new FederatedModelInfo();
         string currStep = "Getting federated ID";

         // Create separate connection with a short duration

         string sqlStmt = "Select FEDERATEDID federatedID, ModelName, ProjectNumber, ProjectName, WORLDBBOX, MAXOCTREELEVEL, LastUpdateDate, Owner, DBConnection from BIMRL_FEDERATEDMODEL where FederatedID=" + FedID.ToString();
#if ORACLE
         OracleCommand fidCmd = new OracleCommand(sqlStmt, DBConn);
         OracleDataReader fidreader = fidCmd.ExecuteReader();

         try
         {
            if (!fidreader.Read())
            {
               fidreader.Close();
               return null;
            }

            fedModel.FederatedID = fidreader.GetInt32(0);
            fedModel.ModelName = fidreader.GetString(1);
            fedModel.ProjectNumber = fidreader.GetString(2);
            fedModel.ProjectName = fidreader.GetString(3);
            if (!fidreader.IsDBNull(4))
            {
               SdoGeometry worldBB = fidreader.GetValue(4) as SdoGeometry;
               Point3D LLB = new Point3D(worldBB.OrdinatesArrayOfDoubles[0], worldBB.OrdinatesArrayOfDoubles[1], worldBB.OrdinatesArrayOfDoubles[2]);
               Point3D URT = new Point3D(worldBB.OrdinatesArrayOfDoubles[3], worldBB.OrdinatesArrayOfDoubles[4], worldBB.OrdinatesArrayOfDoubles[5]);
               fedModel.WorldBoundingBox = LLB.ToString() + " " + URT.ToString();
            }
            if (!fidreader.IsDBNull(5))
               fedModel.OctreeMaxDepth = fidreader.GetInt16(5);
            if (!fidreader.IsDBNull(6))
               fedModel.LastUpdateDate = fidreader.GetDateTime(6);
            if (!fidreader.IsDBNull(7))
               fedModel.Owner = fidreader.GetString(7);
            if (!fidreader.IsDBNull(8))
               fedModel.DBConnection = fidreader.GetString(8);

            fidreader.Close();
         }
         catch (OracleException e)
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + currStep;
            refBIMRLCommon.StackPushError(excStr);
            fidCmd.Dispose();
            throw;
         }

         fidreader.Dispose();
         fidCmd.Dispose();
         return fedModel;
#endif
#if POSTGRES
         try
         {
            DataTable dt = ExecuteToDataTableWithTrans2(sqlStmt);
            if (dt == null || dt.Rows == null || dt.Rows.Count == 0)
               return null;

            fedModel.FederatedID = (int) dt.Rows[0]["federatedid"];
            fedModel.ModelName = (string) dt.Rows[0]["modelname"];
            fedModel.ProjectNumber = (string) dt.Rows[0]["projectnumber"];
            fedModel.ProjectName = (string) dt.Rows[0]["projectname"];
            if (!(dt.Rows[0]["worldbbox"] is DBNull))
            {
               Point3D[] worldBB = (Point3D[]) dt.Rows[0]["worldbbox"];
               Point3D LLB = worldBB[0];
               Point3D URT = worldBB[1];
               fedModel.WorldBoundingBox = LLB.ToString() + " " + URT.ToString();
            }
            if (!(dt.Rows[0]["maxoctreelevel"] is DBNull))
               fedModel.OctreeMaxDepth = (int) dt.Rows[0]["maxoctreelevel"];
            if (!(dt.Rows[0]["lastupdatedate"] is DBNull))
               fedModel.LastUpdateDate = (DateTime) dt.Rows[0]["lastupdatedate"];
            if (!(dt.Rows[0]["owner"] is DBNull))
               fedModel.Owner = (string) dt.Rows[0]["owner"];
            if (!(dt.Rows[0]["dbconnection"] is DBNull))
               fedModel.DBConnection = (string) dt.Rows[0]["dbconnection"];

            return fedModel;
         }
         catch (NpgsqlException e)
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + currStep;
            refBIMRLCommon.StackPushError(excStr);
#if POSTGRES
            DBOperation.rollbackTransaction();
#endif
            throw;
         }
#endif
      }

      public static FedIDStatus getFederatedModel (string modelName, string projName, string projNumber, out FederatedModelInfo fedModel)
      {
         fedModel = new FederatedModelInfo();
         FedIDStatus stat = FedIDStatus.FedIDExisting;
         string currStep = "Getting federated ID";

         // Create separate connection with a short duration

         string sqlStmt = "Select FEDERATEDID federatedID, ModelName, ProjectNumber, ProjectName, WORLDBBOX, MAXOCTREELEVEL, LastUpdateDate, Owner, DBConnection from BIMRL_FEDERATEDMODEL where MODELNAME = '" + modelName + "' and PROJECTNAME = '" + projName + "' and PROJECTNUMBER = '" + projNumber + "'";
#if ORACLE
         OracleCommand command = new OracleCommand(sqlStmt, DBConn);
         OracleDataReader reader = command.ExecuteReader();
#endif
#if POSTGRES
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBConn);
         NpgsqlDataReader reader = command.ExecuteReader();
#endif
         try
         {
            if (!reader.Read())
            {
               reader.Close();
               // Create a new record
               command.CommandText = "Insert into BIMRL_FEDERATEDMODEL (MODELNAME, PROJECTNAME, PROJECTNUMBER) values ('" + modelName + "', '" + projName + "', '" + projNumber + "')";
               DBOperation.beginTransaction();
               command.ExecuteNonQuery();
               DBOperation.commitTransaction();
               stat = FedIDStatus.FedIDNew;

               command.CommandText = sqlStmt;
               reader = command.ExecuteReader();
               reader.Read();
            }

            fedModel.FederatedID = reader.GetInt32(0);
            fedModel.ModelName = reader.GetString(1);
            fedModel.ProjectNumber = reader.GetString(2);
            fedModel.ProjectName = reader.GetString(3);
            if (!reader.IsDBNull(4))
            {
#if ORACLE
               SdoGeometry worldBB = reader.GetValue(4) as SdoGeometry;
               Point3D LLB = new Point3D(worldBB.OrdinatesArrayOfDoubles[0], worldBB.OrdinatesArrayOfDoubles[1], worldBB.OrdinatesArrayOfDoubles[2]);
               Point3D URT = new Point3D(worldBB.OrdinatesArrayOfDoubles[3], worldBB.OrdinatesArrayOfDoubles[4], worldBB.OrdinatesArrayOfDoubles[5]);
               fedModel.WorldBoundingBox = LLB.ToString() + " " + URT.ToString();
#endif
#if POSTGRES
               Point3D[] worldBBPntArray = reader.GetFieldValue<Point3D[]>(4);
               Point3D LLB = worldBBPntArray[0];
               Point3D URT = worldBBPntArray[1];
               fedModel.WorldBoundingBox = LLB.ToString() + " " + URT.ToString();
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

            reader.Close();
         }
#if ORACLE
         catch (OracleException e)
#endif
#if POSTGRES
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Error - " + e.Message + "\n\t" + currStep;
            refBIMRLCommon.StackPushError(excStr);
#if POSTGRES
            DBOperation.rollbackTransaction();
#endif
            command.Dispose();
            throw;
         }
         command.Dispose();

         return stat;
      }

      public static int getModelID (int fedID)
      {
#if ORACLE
         string sqlStmt = "Select " + DBOperation.formatTabName("SEQ_BIMRL_MODELINFO", fedID) + ".nextval from dual";
         OracleCommand command = new OracleCommand(sqlStmt, DBConn);
#endif
#if POSTGRES
         string sqlStmt = "Select nextval('" + DBOperation.formatTabName("SEQ_BIMRL_MODELINFO", fedID) + "')";
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBConn);
#endif
         int newModelID = Convert.ToInt32(command.ExecuteScalar().ToString());

         command.Dispose();
         return newModelID;
      }

      public static int createModelTables (int ID)
      {
         //var location = new Uri(Assembly.GetEntryAssembly().GetName().CodeBase);
         //string exePath = new FileInfo(location.AbsolutePath).Directory.FullName;
         //exePath = exePath.Replace("%20", " ");
         //string crtabScript = Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_crtab.sql");
         //return executeScript(crtabScript, ID);
         ExecuteSystemScript(ID, "BIMRL_crtab.sql");
         return 0;
      }

      public static int executeScript(string filename, int ID)
      {
         int cmdStat = 0;
         string line;
         string stmt = string.Empty;
         string idStr = ID.ToString("X4");
#if ORACLE
         OracleCommand cmd = new OracleCommand(" ", DBConn);
#endif
#if POSTGRES
         beginTransaction();
         NpgsqlCommand cmd = new NpgsqlCommand(" ", DBConn);
#endif
         string currStep = string.Empty;

         bool commentStart = false;
         StreamReader reader = new StreamReader(filename);
         while ((line = reader.ReadLine()) != null)
         {
            line.Trim();
            if (line.StartsWith("/*"))
            {
               commentStart = true;
               continue;
            }
            if (line.EndsWith("*/"))
            {
               commentStart = false;
               continue;
            }
            if (line.StartsWith("//") || line.StartsWith("--") || line.StartsWith("/") || commentStart) continue;  // PLSQL end line, skip

            line = line.Replace("&1", idStr);
            stmt += " " + line;
            if (line.EndsWith(";"))
            {
               try
               {
                  cmd.CommandText = stmt.Remove(stmt.Length - 1);   // remove the ;
                  currStep = cmd.CommandText;
#if POSTGRES
                  CurrTransaction.Save(def_savepoint);
#endif
                  cmdStat = cmd.ExecuteNonQuery();
                  stmt = string.Empty;    // reset stmt
#if POSTGRES
                  CurrTransaction.Release(def_savepoint);
#endif
               }
#if ORACLE
               catch (OracleException e)
#endif
#if POSTGRES
               catch (NpgsqlException e)
#endif
               {
                  string excStr = "%%Error - " + e.Message + "\n\t" + currStep;
                  refBIMRLCommon.StackPushIgnorableError(excStr);
                  stmt = string.Empty;    // reset stmt
#if POSTGRES
                  CurrTransaction.Rollback(def_savepoint);
#endif
                  continue;
               }
            }
         }
         reader.Close();
         commitTransaction();
         cmd.Dispose();

         return cmdStat;
      }

      public static int dropModelTables(int ID)
      {
         //var location = new Uri(Assembly.GetEntryAssembly().GetName().CodeBase);
         //string exePath = new FileInfo(location.AbsolutePath).Directory.FullName;
         //exePath = exePath.Replace("%20", " ");
         //string drtabScript = Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_drtab.sql");
         //return executeScript(drtabScript, ID);
         ExecuteSystemScript(ID, "BIMRL_drtab.sql");
         return 0;
      }

      public static projectUnit getProjectUnitLength(int fedID)
      {
         projectUnit projectUnit = projectUnit.SIUnit_Length_Meter;

         string sqlStmt = "Select PROPERTYVALUE from " + DBOperation.formatTabName("BIMRL_PROPERTIES", fedID) + " P, " + DBOperation.formatTabName("BIMRL_ELEMENT", fedID) + " E"
                           + " where P.ELEMENTID=E.ELEMENTID and upper(E.ELEMENTTYPE)='IFCPROJECT' AND upper(PROPERTYGROUPNAME)='IFCATTRIBUTES' AND upper(PROPERTYNAME)='LENGTHUNIT'";
         string currStep = sqlStmt;
#if ORACLE
         OracleCommand command = new OracleCommand(sqlStmt, DBConn);
#endif
#if POSTGRES
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBConn);
#endif

         try
         {
            object unitS = command.ExecuteScalar();
            if (unitS != null)
            {
               string unitString = unitS as string;
               if (string.Compare(unitString, "MILLI METRE",true) == 0)
                  projectUnit = projectUnit.SIUnit_Length_MilliMeter;
               else if (string.Compare(unitString, "INCH", true) == 0)
                  projectUnit = projectUnit.Imperial_Length_Inch;
               else if (string.Compare(unitString, "FOOT", true) == 0)
                  projectUnit = projectUnit.Imperial_Length_Foot;
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
            refBIMRLCommon.StackPushError(excStr);
            DBOperation.rollbackTransaction();
            command.Dispose();
         }
         command.Dispose();
         currModelProjectUnitLength = projectUnit;
         return projectUnit;
      }

      public static bool getWorldBB(int federatedId, out Point3D llb, out Point3D urt)
      {
         Tuple<Point3D,Point3D,int> bbinfo;

         // If the information is already in the dictionary, return the info
         if (worldBBInfo.TryGetValue(federatedId, out bbinfo))
         {
            llb = bbinfo.Item1;
            urt = bbinfo.Item2;
            Octree.WorldBB = new BoundingBox3D(llb, urt);
            Octree.MaxDepth = bbinfo.Item3;
            return true;
         }

         llb = null;
         urt = null;
         string sqlStmt = "select WORLDBBOX, MAXOCTREELEVEL from BIMRL_FEDERATEDMODEL WHERE FEDERATEDID=" + federatedId.ToString();
#if ORACLE
         OracleCommand command = new OracleCommand(sqlStmt, DBConn);

         try
         {
            OracleDataReader reader = command.ExecuteReader();
#endif
#if POSTGRES
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBConn);

         try
         {
            NpgsqlDataReader reader = command.ExecuteReader();
#endif
            while (reader.Read())
            {
               if (!reader.IsDBNull(0))
               {
#if ORACLE
               SdoGeometry worldBB = reader.GetValue(0) as SdoGeometry;
               llb = new Point3D(worldBB.OrdinatesArrayOfDoubles[0], worldBB.OrdinatesArrayOfDoubles[1], worldBB.OrdinatesArrayOfDoubles[2]);
               urt = new Point3D(worldBB.OrdinatesArrayOfDoubles[3], worldBB.OrdinatesArrayOfDoubles[4], worldBB.OrdinatesArrayOfDoubles[5]);
#endif
#if POSTGRES
                  Point3D[] worldBBPntArray = reader.GetFieldValue<Point3D[]>(0);
                  llb = worldBBPntArray[0];
                  urt = worldBBPntArray[1];
#endif
               }
               int maxDepth = reader.GetInt32(1);

               Octree.WorldBB = new BoundingBox3D(llb, urt);
               Octree.MaxDepth = maxDepth;

               // Add the new info into the dictionary
               worldBBInfo.Add(federatedId, new Tuple<Point3D,Point3D,int>(llb,urt,maxDepth));

               reader.Dispose();
               command.Dispose();
               return true;
            }
            reader.Dispose();
            command.Dispose();
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
            refBIMRLCommon.StackPushError(excStr);
         }
         catch (SystemException e)
         {
            string excStr = "%%Read Error - " + e.Message + "\n\t" + sqlStmt;
            refBIMRLCommon.StackPushError(excStr);
#if POSTGRES
            DBOperation.rollbackTransaction();
#endif
            throw;
         }
         return false;
      }

      public static int computeRecomOctreeLevel(int fedID)
      {
         Point3D llb;
         Point3D urt;

         if (getWorldBB(fedID, out llb, out urt))
         {
            double dX = urt.X - llb.X;
            double dY = urt.Y - llb.Y;
            double dZ = urt.Z - llb.Z;
            double largestEdge = dX;        // set this as initial value

            if (dY > dX && dY > dZ)
               largestEdge = dY;
            else if (dZ > dY && dZ > dX)
               largestEdge = dZ;

            //double defaultTh = 200;     // value in mm
            //double threshold = defaultTh;
            //projectUnit pjUnit = getProjectUnitLength(fedID);
            //if (pjUnit == projectUnit.SIUnit_Length_MilliMeter)
            //{
            //    threshold = defaultTh;
            //}
            //else if (pjUnit == projectUnit.SIUnit_Length_Meter)
            //{
            //    threshold =  defaultTh / 1000;
            //}
            //else if (pjUnit == projectUnit.Imperial_Length_Foot)
            //{
            //    threshold = defaultTh / 304.8;
            //}
            //else if (pjUnit == projectUnit.Imperial_Length_Inch)
            //{
            //    threshold = defaultTh / 25.4;
            //}

            // Model now is always stored in Meter. Here the treshold should be set to use M unit
            double threshold = 0.2;     // Default base of 200mm

            double calcV = largestEdge;
            int level = 0;
            while (calcV > threshold)
            {
               calcV = calcV / 2;
               level++;
            }
            OctreeSubdivLevel = level;
            return level;
         }
         else
            return -1;
      }

      public static FederatedModelInfo currFedModel
      {
         get { return _FederatedModelInfo; }
         set {
            _FederatedModelInfo = value;
            currSelFedID = _FederatedModelInfo.FederatedID;
         }
      }

      public static string formatTabName(string rawTabName)
      {
         return (currFedModel.Owner + "." + rawTabName + "_" + currFedModel.FederatedID.ToString("X4")).ToUpper();
      }

      public static string formatTabName(string rawTabName, int FedID)
      {
         FederatedModelInfo fedInfo = getFederatedModelByID(FedID);
         if (fedInfo == null)
            return null;
         return (fedInfo.Owner + "." + rawTabName + "_" + fedInfo.FederatedID.ToString("X4")).ToUpper();
      }

      public static void CloseActiveConnection()
      {
         if (m_DBconn != null)
            if (m_DBconn.State == ConnectionState.Open)
               m_DBconn.Close();
#if ORACLE
#endif
#if POSTGRES
         if (m_DBconn2 != null)
            if (m_DBconn2.State == ConnectionState.Open)
               m_DBconn2.Close();
#endif
      }

      /// <summary>
      /// Execute script that is located in the BIMRL folder
      /// </summary>
      /// <param name="scriptFileName"></param>
      /// <returns>false if command result is < 0</returns>
      public static void ExecuteSystemScript(int IDtoSubs, params string[] scriptFileNames)
      {
         var location = new Uri(Assembly.GetEntryAssembly().GetName().CodeBase);
         string exePath = new FileInfo(location.AbsolutePath).Directory.FullName.Replace("%20", " ");

         foreach (string scriptName in scriptFileNames)
            executeScript(Path.Combine(exePath, DBOperation.ScriptPath, scriptName), IDtoSubs);
      }
   }
}
