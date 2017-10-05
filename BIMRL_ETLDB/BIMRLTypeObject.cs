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
using Xbim.Ifc;
using Xbim.Ifc4.Interfaces;
#if ORACLE
using Oracle.DataAccess.Types;
using Oracle.DataAccess.Client;
#endif
#if POSTGRES
using Npgsql;
using NpgsqlTypes;
#endif
using BIMRL.Common;

namespace BIMRL
{
   public class BIMRLTypeObject
   {
      BIMRLCommon _refBIMRLCommon;
      IfcStore _model;

      public BIMRLTypeObject(IfcStore m, BIMRLCommon refBIMRLCommon)
      {
         _refBIMRLCommon = refBIMRLCommon;
         _model = m;
      }

      public void processTypeObject()
      {
#if ORACLE
         string sqlStmt = "Insert into " + DBOperation.formatTabName("BIMRL_TYPE") + "(ElementId, IfcType, Name, Description, ApplicableOccurrence"
                           + ", Tag, ElementType, PredefinedType, AssemblyPlace, OperationType, ConstructionType, OwnerHistoryID, ModelID)"
                           + " Values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)";
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);

         string currStep = sqlStmt;

         IEnumerable<IIfcTypeProduct> types = _model.Instances.OfType<IIfcTypeProduct>();
         foreach (IIfcTypeProduct typ in types)
         {
            string guid = typ.GlobalId.ToString();
            string ifcType = typ.GetType().Name.ToUpper();

            string name = "Default Type";
            if (typ.Name.HasValue)
               name = typ.Name.Value;

            string desc = null;
            if (typ.Description.HasValue)
               desc = typ.Description.Value;

            string appO = null;
            if (typ.ApplicableOccurrence.HasValue)
               appO = typ.ApplicableOccurrence.Value;

            string tag = null;
            if (typ.Tag.HasValue)
               tag = typ.Tag.Value;

            string eType = null;
            string PDType = null;
            string APl = null;
            string opType = null;
            string consType = null;
            int ownerHist = -1;
            int modelID = -1;

            dynamic dynTyp = typ;
            if (!(typ is IIfcDoorStyle || typ is IIfcWindowStyle))
            {
               if (dynTyp.ElementType != null)
                  eType = dynTyp.ElementType;
            }

            if (typ is IIfcFurnitureType)
            {
               // these entities do not have PredefinedType
               // This entity has a different attribute: AssemblyPlace. This must be placed ahead of its supertype IfcFurnishingElementType
               IIfcFurnitureType ftyp = typ as IIfcFurnitureType;
               APl = ftyp.AssemblyPlace.ToString();
               if (ftyp.PredefinedType.HasValue)
                  PDType = ftyp.PredefinedType.Value.ToString();
            }
            else if (typ is IIfcSystemFurnitureElementType)
            {
               IIfcSystemFurnitureElementType fstyp = typ as IIfcSystemFurnitureElementType;
               if (fstyp.PredefinedType.HasValue)
                  PDType = fstyp.PredefinedType.Value.ToString();
            }
            else if (typ is IIfcFurnishingElementType)
            {
               // these entities do not have PredefinedType. Xbim also has not implemented IfcCurtainWallType and therefore no PredefinedType yet!!
            }

            // These entities do not have predefinedtype, but OperationType and ConstructionType
            // We ignore ParameterTakesPrecedence and Sizeable are only useful for object construction
            else if (typ is IIfcDoorStyle)
            {
               IIfcDoorStyle dst = typ as IIfcDoorStyle;
               opType = dst.OperationType.ToString();
               consType = dst.OperationType.ToString();
            }
            else if (typ is IIfcWindowStyle)
            {
               // these entities do not have PredefinedType
               IIfcWindowStyle wst = typ as IIfcWindowStyle;
               opType = wst.OperationType.ToString();
               consType = wst.OperationType.ToString();
            }
            else
            {
               PDType = dynTyp.PredefinedType.ToString();
            }

            Tuple<int, int> ownHEntry = new Tuple<int, int>(Math.Abs(typ.OwnerHistory.EntityLabel), BIMRLProcessModel.currModelID);
            if (_refBIMRLCommon.OwnerHistoryExist(ownHEntry))
               ownerHist = Math.Abs(typ.OwnerHistory.EntityLabel);

            modelID = BIMRLProcessModel.currModelID;

            insType(command, guid, ifcType, name, desc, appO, tag, eType, PDType, APl, opType, consType, ownerHist, modelID);
#endif
#if POSTGRES
         string sqlStmt = "Insert into " + DBOperation.formatTabName("BIMRL_TYPE") + "(ElementId, IfcType, Name, Description, ApplicableOccurrence"
                           + ", Tag, ElementType, PredefinedType, AssemblyPlace, OperationType, ConstructionType, OwnerHistoryID, ModelID)"
                           + " Values (@eid, @ifctyp, @name, @desc, @applo, @tag, @etyp, @pdtyp, @asspl, @optyp, @ctyp, @ownh, @modid)";
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);

         command.Parameters.Add("@eid", NpgsqlDbType.Varchar);
         command.Parameters.Add("@ifctyp", NpgsqlDbType.Varchar);
         command.Parameters.Add("@name", NpgsqlDbType.Varchar);
         command.Parameters.Add("@desc", NpgsqlDbType.Varchar);
         command.Parameters.Add("@applo", NpgsqlDbType.Varchar);
         command.Parameters.Add("@tag", NpgsqlDbType.Varchar);
         command.Parameters.Add("@etyp", NpgsqlDbType.Varchar);
         command.Parameters.Add("@pdtyp", NpgsqlDbType.Varchar);
         command.Parameters.Add("@asspl", NpgsqlDbType.Varchar);
         command.Parameters.Add("@optyp", NpgsqlDbType.Varchar);
         command.Parameters.Add("@ctyp", NpgsqlDbType.Varchar);
         command.Parameters.Add("@ownh", NpgsqlDbType.Integer);
         command.Parameters.Add("@modid", NpgsqlDbType.Integer);
         command.Prepare();

         IEnumerable<IIfcTypeProduct> types = _model.Instances.OfType<IIfcTypeProduct>();
         foreach (IIfcTypeProduct typ in types)
         {
            string guid = typ.GlobalId.ToString();
            string ifcType = typ.GetType().Name.ToUpper();

            string name = "Default Type";
            if (typ.Name.HasValue)
               name = typ.Name.Value;

            string desc = null;
            if (typ.Description.HasValue)
               desc = typ.Description.Value;

            string appO = null;
            if (typ.ApplicableOccurrence.HasValue)
               appO = typ.ApplicableOccurrence.Value;

            string tag = null;
            if (typ.Tag.HasValue)
               tag = typ.Tag.Value;

            string eType = null;
            string PDType = null;
            string APl = null;
            string opType = null;
            string consType = null;
            int ownerHist = -1;
            int modelID = -1;

            dynamic dynTyp = typ;
            if (!(typ is IIfcDoorStyle || typ is IIfcWindowStyle))
            {
               if (dynTyp.ElementType != null)
                  eType = dynTyp.ElementType;
            }

            if (typ is IIfcFurnitureType)
            {
               // these entities do not have PredefinedType
               // This entity has a different attribute: AssemblyPlace. This must be placed ahead of its supertype IfcFurnishingElementType
               IIfcFurnitureType ftyp = typ as IIfcFurnitureType;
               APl = ftyp.AssemblyPlace.ToString();
               if (ftyp.PredefinedType.HasValue)
                  PDType = ftyp.PredefinedType.Value.ToString();
            }
            else if (typ is IIfcSystemFurnitureElementType)
            {
               IIfcSystemFurnitureElementType fstyp = typ as IIfcSystemFurnitureElementType;
               if (fstyp.PredefinedType.HasValue)
                  PDType = fstyp.PredefinedType.Value.ToString();
            }
            else if (typ is IIfcFurnishingElementType)
            {
               // these entities do not have PredefinedType. Xbim also has not implemented IfcCurtainWallType and therefore no PredefinedType yet!!
            }

            // These entities do not have predefinedtype, but OperationType and ConstructionType
            // We ignore ParameterTakesPrecedence and Sizeable are only useful for object construction
            else if (typ is IIfcDoorStyle)
            {
               IIfcDoorStyle dst = typ as IIfcDoorStyle;
               opType = dst.OperationType.ToString();
               consType = dst.OperationType.ToString();
            }
            else if (typ is IIfcWindowStyle)
            {
               // these entities do not have PredefinedType
               IIfcWindowStyle wst = typ as IIfcWindowStyle;
               opType = wst.OperationType.ToString();
               consType = wst.OperationType.ToString();
            }
            else
            {
               PDType = dynTyp.PredefinedType.ToString();
            }

            Tuple<int, int> ownHEntry = new Tuple<int, int>(Math.Abs(typ.OwnerHistory.EntityLabel), BIMRLProcessModel.currModelID);
            if (_refBIMRLCommon.OwnerHistoryExist(ownHEntry))
               ownerHist = Math.Abs(typ.OwnerHistory.EntityLabel);

            modelID = BIMRLProcessModel.currModelID;

            insType(command, guid, ifcType, name, desc, appO, tag, eType, PDType, APl, opType, consType, ownerHist, modelID);

#endif
            DBOperation.commitTransaction();
            BIMRLProperties tProps = new BIMRLProperties(_refBIMRLCommon);
            tProps.processTypeProperties(typ);
         }
         DBOperation.commitTransaction();
      }

#if ORACLE
      private void insType(OracleCommand command, string guid, string ifcType, string name, string desc, string appO, string tag, string eType,
                        string PDType, string APl, string opType, string consType, int ownerHist, int modelID)
      {
         OracleParameter[] Param = new OracleParameter[13];
         command.Parameters.Clear();
         for (int i = 0; i < 11; i++)
         {
            Param[i] = command.Parameters.Add(i.ToString(), OracleDbType.Varchar2);
            Param[i].Direction = ParameterDirection.Input;
         }
         Param[11] = command.Parameters.Add("11", OracleDbType.Int32);
         Param[11].Direction = ParameterDirection.Input;
         Param[12] = command.Parameters.Add("12", OracleDbType.Int32);
         Param[12].Direction = ParameterDirection.Input;

         Param[0].Value = guid;
         Param[1].Value = ifcType;
         Param[2].Value = name;
         Param[3].Value = desc;
         Param[4].Value = appO;
         Param[5].Value = tag;
         Param[6].Value = eType;
         Param[7].Value = PDType;
         Param[8].Value = APl;
         Param[9].Value = opType;
         Param[10].Value = consType;
         if (ownerHist >= 0)
            Param[11].Value = ownerHist;
         else
            Param[11].Value = DBNull.Value;

         if (modelID >= 0)
            Param[12].Value = modelID;
         else
            Param[12].Value = DBNull.Value;

         try
         {
            int commandStatus = command.ExecuteNonQuery();
         }
         catch (OracleException e)
         {
            string excStr = "%%Insert Error - " + e.Message + "\n\t" + command.CommandText;
            _refBIMRLCommon.StackPushIgnorableError(excStr);
         }
         catch (SystemException e)
         {
            string excStr = "%%Insert Error - " + e.Message + "\n\t" + command.CommandText;
            _refBIMRLCommon.StackPushError(excStr);
            throw;
         }
      }
#endif
#if POSTGRES
      private void insType(NpgsqlCommand command, string guid, string ifcType, string name, string desc, string appO, string tag, string eType,
                        string PDType, string APl, string opType, string consType, int ownerHist, int modelID)
      {
         command.Parameters["@eid"].Value = guid;
         command.Parameters["@ifctyp"].Value = ifcType;
         command.Parameters["@name"].Value = name;
         
         if (string.IsNullOrEmpty(desc))
            command.Parameters["@desc"].Value = DBNull.Value;
         else
            command.Parameters["@desc"].Value = desc;

         if (string.IsNullOrEmpty(appO))
            command.Parameters["@applo"].Value = DBNull.Value;
         else
            command.Parameters["@applo"].Value = appO;

         if (string.IsNullOrEmpty(tag))
            command.Parameters["@tag"].Value = DBNull.Value;
         else
            command.Parameters["@tag"].Value = tag;

         if (string.IsNullOrEmpty(eType))
            command.Parameters["@etyp"].Value = DBNull.Value;
         else
            command.Parameters["@etyp"].Value = eType;

         if (string.IsNullOrEmpty(PDType))
            command.Parameters["@pdtyp"].Value = DBNull.Value;
         else
            command.Parameters["@pdtyp"].Value = PDType;

         if (string.IsNullOrEmpty(APl))
            command.Parameters["@asspl"].Value = DBNull.Value;
         else
            command.Parameters["@asspl"].Value = APl;

         if (string.IsNullOrEmpty(opType))
            command.Parameters["@optyp"].Value = DBNull.Value;
         else
            command.Parameters["@optyp"].Value = opType;

         if (string.IsNullOrEmpty(consType))
            command.Parameters["@ctyp"].Value = DBNull.Value;
         else
            command.Parameters["@ctyp"].Value = consType;

         if (ownerHist >= 0)
            command.Parameters["@ownh"].Value = ownerHist;
         else
            command.Parameters["@ownh"].Value = DBNull.Value;

         if (modelID >= 0)
            command.Parameters["@modid"].Value = modelID;
         else
            command.Parameters["@modid"].Value = DBNull.Value;

         try
         {
            DBOperation.CurrTransaction.Save(DBOperation.def_savepoint);
            int commandStatus = command.ExecuteNonQuery();
            DBOperation.CurrTransaction.Release(DBOperation.def_savepoint);
         }
         catch (NpgsqlException e)
         {
            string excStr = "%%Insert Error - " + e.Message + "\n\t" + command.CommandText;
            _refBIMRLCommon.StackPushIgnorableError(excStr);
            DBOperation.CurrTransaction.Rollback(DBOperation.def_savepoint);
         }
         catch (SystemException e)
         {
            string excStr = "%%Insert Error - " + e.Message + "\n\t" + command.CommandText;
            _refBIMRLCommon.StackPushError(excStr);
            throw;
         }

      }
#endif
   }
}
