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
   public class BIMRLRelationship
   {
      private IfcStore _model;
      private BIMRLCommon _refBIMRLCommon;

      public BIMRLRelationship(IfcStore m, BIMRLCommon refBIMRLCommon)
      {
         _model = m;
         _refBIMRLCommon = refBIMRLCommon;
      }

      public void processRelationships()
      {
         processRelContainedInSpatialStructure();
         processRelAggregation();
         processRelConnections();
         processRelSpaceBoundary();
         processRelGroup();
         processElemDependency();
      }

      private void processRelAggregation()
      {
         string sqlStmt;
         string currStep = string.Empty;

         DBOperation.beginTransaction();

         int commandStatus = -1;
         string parentGuid = string.Empty;
         string parentType = string.Empty;

#if ORACLE
         List<string> arrMastGuids = new List<string>();
         List<string> arrMastTypes = new List<string>();
         List<string> arrAggrGuids = new List<string>();
         List<string> arrAggrTypes = new List<string>();
         
         sqlStmt = "insert into " + DBOperation.formatTabName("BIMRL_RELAGGREGATION")
                     + " (MASTERELEMENTID, MASTERELEMENTTYPE, AGGREGATEELEMENTID, AGGREGATEELEMENTTYPE) values (:mGuids, :mType, :aGuids, :aType )";
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);

         OracleParameter[] Param = new OracleParameter[4];
         Param[0] = command.Parameters.Add("mGuids", OracleDbType.Varchar2);
         Param[0].Direction = ParameterDirection.Input;
         Param[1] = command.Parameters.Add("mType", OracleDbType.Varchar2);
         Param[1].Direction = ParameterDirection.Input;
         Param[2] = command.Parameters.Add("aGuids", OracleDbType.Varchar2);
         Param[2].Direction = ParameterDirection.Input;
         Param[3] = command.Parameters.Add("aType", OracleDbType.Varchar2);
         Param[3].Direction = ParameterDirection.Input;
#endif
#if POSTGRES
         sqlStmt = "insert into " + DBOperation.formatTabName("BIMRL_RELAGGREGATION")
                     + " (MASTERELEMENTID, MASTERELEMENTTYPE, AGGREGATEELEMENTID, AGGREGATEELEMENTTYPE) values (@mGuids, @mType, @aGuids, @aType )";
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);

         command.Parameters.Add("@mGuids", NpgsqlDbType.Varchar);
         command.Parameters.Add("@mType", NpgsqlDbType.Varchar);
         command.Parameters.Add("@aGuids", NpgsqlDbType.Varchar);
         command.Parameters.Add("@aType", NpgsqlDbType.Varchar);
         command.Prepare();
#endif
         currStep = sqlStmt;

         IEnumerable<IIfcRelAggregates> rels = _model.Instances.OfType<IIfcRelAggregates>();
         foreach (IIfcRelAggregates aggr in rels)
         {
            string aggrGuid = aggr.RelatingObject.GlobalId.ToString();
            string aggrType = aggr.RelatingObject.GetType().Name.ToUpper();
            if (_refBIMRLCommon.getLineNoFromMapping(aggrGuid) == null)
               continue;   // skip relationship that involves "non" element Guids

            IEnumerable<IIfcObjectDefinition> relObjects = aggr.RelatedObjects;
            foreach (IIfcObjectDefinition relObj in relObjects)
            {
#if ORACLE
               string relObjGuid = relObj.GlobalId.ToString();
               string relObjType = relObj.GetType().Name.ToUpper();
               arrMastGuids.Add(aggrGuid);
               arrMastTypes.Add(aggrType);
               arrAggrGuids.Add(relObjGuid);
               arrAggrTypes.Add(relObjType);
            }

            if (arrMastGuids.Count >= DBOperation.commitInterval)
            {
               Param[0].Size = arrMastGuids.Count();
               Param[0].Value = arrMastGuids.ToArray();
               Param[1].Size = arrMastTypes.Count();
               Param[1].Value = arrMastTypes.ToArray();
               Param[2].Size = arrAggrGuids.Count();
               Param[2].Value = arrAggrGuids.ToArray();
               Param[3].Size = arrAggrTypes.Count();
               Param[3].Value = arrAggrTypes.ToArray();
               command.ArrayBindCount = arrMastGuids.Count;    // No of values in the array to be inserted
               try
               {
                  commandStatus = command.ExecuteNonQuery();
                  //Do commit at interval but keep the long transaction (reopen)
                  DBOperation.commitTransaction();
                  arrMastGuids.Clear();
                  arrMastTypes.Clear();
                  arrAggrGuids.Clear();
                  arrAggrTypes.Clear();
               }
               catch (OracleException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushIgnorableError(excStr);
                  // Ignore any error
                  arrMastGuids.Clear();
                  arrMastTypes.Clear();
                  arrAggrGuids.Clear();
                  arrAggrTypes.Clear();
                  continue;
               }
               catch (SystemException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushError(excStr);
                  throw;
               }
#endif
#if POSTGRES
               command.Parameters["@mGuids"].Value = aggrGuid;
               command.Parameters["@mType"].Value = aggrType;
               command.Parameters["@aGuids"].Value = relObj.GlobalId.ToString();
               command.Parameters["@aType"].Value = relObj.GetType().Name.ToUpper();

               try
               {
                  DBOperation.CurrTransaction.Save(DBOperation.def_savepoint);
                  commandStatus = command.ExecuteNonQuery();
                  //Do commit at interval but keep the long transaction (reopen)
                  DBOperation.commitTransaction();
               }
               catch (NpgsqlException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushIgnorableError(excStr);
                  DBOperation.CurrTransaction.Rollback(DBOperation.def_savepoint);
                  continue;
               }
               catch (SystemException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushError(excStr);
                  throw;
               }
#endif
            }
         }

#if ORACLE
         if (arrMastGuids.Count > 0)
         {
            Param[0].Size = arrMastGuids.Count();
            Param[0].Value = arrMastGuids.ToArray();
            Param[1].Size = arrMastTypes.Count();
            Param[1].Value = arrMastTypes.ToArray();
            Param[2].Size = arrAggrGuids.Count();
            Param[2].Value = arrAggrGuids.ToArray();
            Param[3].Size = arrAggrTypes.Count();
            Param[3].Value = arrAggrTypes.ToArray();

            try
            {
               command.ArrayBindCount = arrMastGuids.Count;    // No of values in the array to be inserted
               commandStatus = command.ExecuteNonQuery();
               //Do commit at interval but keep the long transaction (reopen)
               DBOperation.commitTransaction();
            }
            catch (OracleException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushIgnorableError(excStr);
            }
            catch (SystemException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushError(excStr);
               throw;
            }
         }
#endif

         DBOperation.commitTransaction();
         command.Dispose();
      }

      private void processRelConnections()
      {
         string sqlStmt;
         string currStep = string.Empty;

         DBOperation.beginTransaction();

#if ORACLE
         sqlStmt = "insert into " + DBOperation.formatTabName("BIMRL_RELCONNECTION")
                     + " (CONNECTINGELEMENTID, CONNECTINGELEMENTTYPE, CONNECTINGELEMENTATTRNAME, CONNECTINGELEMENTATTRVALUE, "
                     + "CONNECTEDELEMENTID, CONNECTEDELEMENTTYPE, CONNECTEDELEMENTATTRNAME, CONNECTEDELEMENTATTRVALUE, "
                     + "CONNECTIONATTRNAME, CONNECTIONATTRVALUE, REALIZINGELEMENTID, REALIZINGELEMENTTYPE, RELATIONSHIPTYPE) "
                     + "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)";
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);

         OracleParameter[] Param = new OracleParameter[13];
         for (int i = 0; i < 13; i++)
         {
            Param[i] = command.Parameters.Add((i + 1).ToString(), OracleDbType.Varchar2);
            Param[i].Direction = ParameterDirection.Input;
         }

         List<string> cIngEle = new List<string>();
         List<string> cIngEleTyp = new List<string>();
         List<string> cIngAttrN = new List<string>();
         List<string> cIngAttrV = new List<string>();
         List<string> cEdEle = new List<string>();
         List<string> cEdEleTyp = new List<string>();
         List<string> cEdAttrN = new List<string>();
         List<string> cEdAttrV = new List<string>();
         List<string> cAttrN = new List<string>();
         List<string> cAttrV = new List<string>();
         List<string> realEl = new List<string>();
         List<string> realElTyp = new List<string>();
         List<string> relTyp = new List<string>();
         List<OracleParameterStatus> cIngAttrNBS = new List<OracleParameterStatus>();
         List<OracleParameterStatus> cIngAttrVBS = new List<OracleParameterStatus>();
         List<OracleParameterStatus> cEdAttrNBS = new List<OracleParameterStatus>();
         List<OracleParameterStatus> cEdAttrVBS = new List<OracleParameterStatus>();
         List<OracleParameterStatus> cAttrNBS = new List<OracleParameterStatus>();
         List<OracleParameterStatus> cAttrVBS = new List<OracleParameterStatus>();
         List<OracleParameterStatus> realElBS = new List<OracleParameterStatus>();
         List<OracleParameterStatus> realElTBS = new List<OracleParameterStatus>();
#endif
#if POSTGRES
         sqlStmt = "insert into " + DBOperation.formatTabName("BIMRL_RELCONNECTION")
            + " (CONNECTINGELEMENTID, CONNECTINGELEMENTTYPE, CONNECTINGELEMENTATTRNAME, CONNECTINGELEMENTATTRVALUE, "
            + "CONNECTEDELEMENTID, CONNECTEDELEMENTTYPE, CONNECTEDELEMENTATTRNAME, CONNECTEDELEMENTATTRVALUE, "
            + "CONNECTIONATTRNAME, CONNECTIONATTRVALUE, REALIZINGELEMENTID, REALIZINGELEMENTTYPE, RELATIONSHIPTYPE) "
            + "VALUES (@ieid, @ietyp, @iattrn, @iattrv, @deid, @detyp, @dattrn, @dattrv, @cattrn, @cattrv, @reid, @retyp, @rtyp)";
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);

         command.Parameters.Add("@ieid", NpgsqlDbType.Varchar);
         command.Parameters.Add("@ietyp", NpgsqlDbType.Varchar);
         command.Parameters.Add("@iattrn", NpgsqlDbType.Varchar);
         command.Parameters.Add("@iattrv", NpgsqlDbType.Varchar);
         command.Parameters.Add("@deid", NpgsqlDbType.Varchar);
         command.Parameters.Add("@detyp", NpgsqlDbType.Varchar);
         command.Parameters.Add("@dattrn", NpgsqlDbType.Varchar);
         command.Parameters.Add("@dattrv", NpgsqlDbType.Varchar);
         command.Parameters.Add("@cattrn", NpgsqlDbType.Varchar);
         command.Parameters.Add("@cattrv", NpgsqlDbType.Varchar);
         command.Parameters.Add("@reid", NpgsqlDbType.Varchar);
         command.Parameters.Add("@retyp", NpgsqlDbType.Varchar);
         command.Parameters.Add("@rtyp", NpgsqlDbType.Varchar);
         command.Prepare();
#endif
         currStep = sqlStmt;

         // Do speacial step first by processing the IfcRelConnecsPortToElement to match FccDistributionPort and IfcElement for MEP connectivity
         IEnumerable<IIfcRelConnectsPortToElement> ptes = _model.Instances.OfType<IIfcRelConnectsPortToElement>();
         foreach (IIfcRelConnectsPortToElement pte in ptes)
         {
            Dictionary<string, string> portElemVal;
            portElemVal = _refBIMRLCommon.PortToElem_GetValue(pte.RelatingPort.GlobalId.ToString());
            if (portElemVal == null)
               portElemVal = new Dictionary<string, string>();
            portElemVal.Add("RELATEDELEMENT", pte.RelatedElement.GlobalId.ToString());
            portElemVal.Add("RELATEDELEMENTTYPE", pte.RelatedElement.GetType().Name.ToUpper());
            _refBIMRLCommon.PortToElemAdd(pte.RelatingPort.GlobalId.ToString(), portElemVal);
         }

         IEnumerable<IIfcRelConnects> rels = _model.Instances.OfType<IIfcRelConnects>().Where
                  (re => !(re is IIfcRelConnectsPortToElement || re is IIfcRelContainedInSpatialStructure || re is IIfcRelConnectsStructuralActivity || re is IIfcRelConnectsStructuralMember
                           || re is IIfcRelFillsElement || re is IIfcRelVoidsElement || re is IIfcRelSequence || re is IIfcRelSpaceBoundary));
         foreach (IIfcRelConnects conn in rels)
         {
            if (conn is IIfcRelConnectsPathElements)
            {
               IIfcRelConnectsPathElements connPE = conn as IIfcRelConnectsPathElements;
               if (_refBIMRLCommon.getLineNoFromMapping(connPE.RelatingElement.GlobalId.ToString()) == null)
                  continue;       // skip "non" element guid in the relationship object

#if ORACLE
               insRelConnect(ref cIngEle, connPE.RelatingElement.GlobalId.ToString(), ref cIngEleTyp, connPE.RelatingElement.GetType().Name.ToUpper(),
                              ref cIngAttrN, ref cIngAttrNBS, "RELATINGCONNECTIONTYPE", ref cIngAttrV, ref cIngAttrVBS, connPE.RelatingConnectionType.ToString(),
                              ref cEdEle, connPE.RelatedElement.GlobalId.ToString(), ref cEdEleTyp, connPE.RelatedElement.GetType().Name.ToUpper(),
                              ref cEdAttrN, ref cEdAttrNBS, "RELATEDCONNECTIONTYPE", ref cEdAttrV, ref cEdAttrVBS, connPE.RelatedConnectionType.ToString(),
                              ref cAttrN, ref cAttrNBS, null, ref cAttrV, ref cAttrVBS, null, 
                              ref realEl, ref realElBS, null, ref realElTyp, ref realElTBS, null, 
                              ref relTyp, connPE.GetType().Name.ToUpper());
#endif
#if POSTGRES
               insRelConnect(command, connPE.RelatingElement.GlobalId.ToString(), connPE.RelatingElement.GetType().Name.ToUpper(),
                              "RELATINGCONNECTIONTYPE", connPE.RelatingConnectionType.ToString(),
                              connPE.RelatedElement.GlobalId.ToString(), connPE.RelatedElement.GetType().Name.ToUpper(),
                              "RELATEDCONNECTIONTYPE", connPE.RelatedConnectionType.ToString(),
                              null, null, 
                              null, null, 
                              connPE.GetType().Name.ToUpper());
#endif
            }
            else if (conn is IIfcRelConnectsWithRealizingElements)
            {
               IIfcRelConnectsWithRealizingElements connPE = conn as IIfcRelConnectsWithRealizingElements;
               if (_refBIMRLCommon.getLineNoFromMapping(connPE.RelatingElement.GlobalId.ToString()) == null)
                  continue;       // skip "non" element guid in the relationship object

               //Iterate for each Realizing element. One record for each realizing element
               foreach (IIfcElement realElem in connPE.RealizingElements)
               {
                  string cAttrNstr = string.Empty;
                  string cAttrVstr = string.Empty;
                  if (connPE.ConnectionType != null)
                  {
                     cAttrNstr = "CONNECTIONTYPE";
                     cAttrVstr = connPE.ConnectionType.ToString();
                  }
#if ORACLE
                  insRelConnect(ref cIngEle, connPE.RelatingElement.GlobalId.ToString(), ref cIngEleTyp, connPE.RelatingElement.GetType().Name.ToUpper(),
                                 ref cIngAttrN, ref cIngAttrNBS, null, ref cIngAttrV, ref cIngAttrVBS, null,
                                 ref cEdEle, connPE.RelatedElement.GlobalId.ToString(), ref cEdEleTyp, connPE.RelatedElement.GetType().Name.ToUpper(),
                                 ref cEdAttrN, ref cEdAttrNBS, null, ref cEdAttrV, ref cEdAttrVBS, null,
                                 ref cAttrN, ref cAttrNBS, cAttrNstr, ref cAttrV, ref cAttrVBS, cAttrVstr,
                                 ref realEl, ref realElBS, realElem.GlobalId.ToString(), ref realElTyp, ref realElTBS, realElem.GetType().Name.ToUpper(),
                                 ref relTyp, connPE.GetType().Name.ToUpper());
#endif
#if POSTGRES
                  insRelConnect(command, connPE.RelatingElement.GlobalId.ToString(), connPE.RelatingElement.GetType().Name.ToUpper(),
                                 null, null,
                                 connPE.RelatedElement.GlobalId.ToString(), connPE.RelatedElement.GetType().Name.ToUpper(),
                                 null, null,
                                 cAttrNstr, cAttrVstr,
                                 realElem.GlobalId.ToString(), realElem.GetType().Name.ToUpper(), 
                                 connPE.GetType().Name.ToUpper());
#endif
               }
            }
            else if (conn is IIfcRelConnectsElements)
            {
               IIfcRelConnectsElements connPE = conn as IIfcRelConnectsElements;
               if (_refBIMRLCommon.getLineNoFromMapping(connPE.RelatingElement.GlobalId.ToString()) == null)
                  continue;       // skip "non" element guid in the relationship object

#if ORACLE
               insRelConnect(ref cIngEle, connPE.RelatingElement.GlobalId.ToString(), ref cIngEleTyp, connPE.RelatingElement.GetType().Name.ToUpper(),
                              ref cIngAttrN, ref cIngAttrNBS, null, ref cIngAttrV, ref cIngAttrVBS, null,
                              ref cEdEle, connPE.RelatedElement.GlobalId.ToString(), ref cEdEleTyp, connPE.RelatedElement.GetType().Name.ToUpper(),
                              ref cEdAttrN, ref cEdAttrNBS, null, ref cEdAttrV, ref cEdAttrVBS, null,
                              ref cAttrN, ref cAttrNBS, null, ref cAttrV, ref cAttrVBS, null,
                              ref realEl, ref realElBS, null, ref realElTyp, ref realElTBS, null,
                              ref relTyp, connPE.GetType().Name.ToUpper());
#endif
#if POSTGRES
               insRelConnect(command, connPE.RelatingElement.GlobalId.ToString(), connPE.RelatingElement.GetType().Name.ToUpper(),
                              null, null,
                              connPE.RelatedElement.GlobalId.ToString(), connPE.RelatedElement.GetType().Name.ToUpper(),
                              null, null,
                              null, null, 
                              null, null, 
                              connPE.GetType().Name.ToUpper());
#endif
            }
            else if (conn is IIfcRelConnectsPorts)
            {
               // Handle MEP connections through Port connections IfcRelConnectsPorts (Port itself won't be created)
               // From Port connection, we will find both ends of the connection (we will get ports) and
               // from there through IfcRelConnectsPortToElement, we will get both ends of the element connected

               IIfcRelConnectsPorts connPE = conn as IIfcRelConnectsPorts;

               IIfcDistributionPort port1 = connPE.RelatingPort as IIfcDistributionPort;
               IIfcDistributionPort port2 = connPE.RelatedPort as IIfcDistributionPort;

               Dictionary<string, string> portElemVal1;
               Dictionary<string, string> portElemVal2;

               portElemVal1 = _refBIMRLCommon.PortToElem_GetValue(port1.GlobalId.ToString());
               portElemVal2 = _refBIMRLCommon.PortToElem_GetValue(port2.GlobalId.ToString());
               if (portElemVal1 == null || portElemVal2 == null)
               {
                  // This should not happen. If somehow happen, skip such a hanging port
                  continue;
               }

               string eleGuid1;
               string eleType1;
               string eleGuid2;
               string eleType2;
               string attrName1;
               string attrVal1;
               string attrName2;
               string attrVal2;

               portElemVal1.TryGetValue("RELATEDELEMENT", out eleGuid1);
               portElemVal2.TryGetValue("RELATEDELEMENT", out eleGuid2);
               if (String.IsNullOrEmpty(eleGuid1) || String.IsNullOrEmpty(eleGuid2))
                  continue;   // Should not happen!


               // We will insert 2 record for each relationship to represent the both directional of the relationship
               portElemVal1.TryGetValue("RELATEDELEMENTTYPE", out eleType1);
               portElemVal2.TryGetValue("RELATEDELEMENTTYPE", out eleType2);
               portElemVal1.TryGetValue("ATTRIBUTENAME", out attrName1);
               portElemVal2.TryGetValue("ATTRIBUTENAME", out attrName2);
               portElemVal1.TryGetValue("ATTRIBUTEVALUE", out attrVal1);
               portElemVal2.TryGetValue("ATTRIBUTEVALUE", out attrVal2);

               // RealizingElement if any
               string realElStr = string.Empty;
               string realElTypStr = string.Empty;
               if (connPE.RealizingElement != null)
               {
                  realElStr = connPE.RealizingElement.GlobalId.ToString();
                  realElTypStr = connPE.RealizingElement.GetType().Name.ToUpper();
               }

               // Create 2 records for bi-directional relationship
#if ORACLE
               insRelConnect(ref cIngEle, eleGuid1, ref cIngEleTyp, eleType1,
                              ref cIngAttrN, ref cIngAttrNBS, attrName1, ref cIngAttrV, ref cIngAttrVBS, attrVal1,
                              ref cEdEle, eleGuid2, ref cEdEleTyp, eleType2,
                              ref cEdAttrN, ref cEdAttrNBS, attrName2, ref cEdAttrV, ref cEdAttrVBS, attrVal2,
                              ref cAttrN, ref cAttrNBS, null, ref cAttrV, ref cAttrVBS, null,
                              ref realEl, ref realElBS, realElStr, ref realElTyp, ref realElTBS, realElTypStr,
                              ref relTyp, connPE.GetType().Name.ToUpper());
               insRelConnect(ref cIngEle, eleGuid2, ref cIngEleTyp, eleType2,
                              ref cIngAttrN, ref cIngAttrNBS, attrName2, ref cIngAttrV, ref cIngAttrVBS, attrVal2,
                              ref cEdEle, eleGuid1, ref cEdEleTyp, eleType1,
                              ref cEdAttrN, ref cEdAttrNBS, attrName1, ref cEdAttrV, ref cEdAttrVBS, attrVal1,
                              ref cAttrN, ref cAttrNBS, null, ref cAttrV, ref cAttrVBS, null,
                              ref realEl, ref realElBS, realElStr, ref realElTyp, ref realElTBS, realElTypStr,
                              ref relTyp, connPE.GetType().Name.ToUpper());
#endif
#if POSTGRES
               insRelConnect(command, eleGuid1, eleType1,
                              attrName1, attrVal1,
                              eleGuid2, eleType2,
                              attrName2, attrVal2,
                              null, null,
                              realElStr, realElTypStr,
                              connPE.GetType().Name.ToUpper());
               insRelConnect(command, eleGuid2, eleType2,
                              attrName2, attrVal2,
                              eleGuid1, eleType1,
                              attrName1, attrVal1,
                              null, null,
                              realElStr, realElTypStr,
                              connPE.GetType().Name.ToUpper());
#endif
            }

            // Handle covering for both covering for spaces and building element
            else if (conn is IIfcRelCoversSpaces)
            {
               IIfcRelCoversSpaces covS = conn as IIfcRelCoversSpaces;
               Xbim.Ifc2x3.Interfaces.IIfcRelCoversSpaces covS2x3 = conn as Xbim.Ifc2x3.Interfaces.IIfcRelCoversSpaces;
               string guid;
               string relType;
               if (covS2x3 != null)
               {
                  guid = covS2x3.RelatedSpace.GlobalId.ToString();
                  relType = covS2x3.RelatedSpace.GetType().Name.ToUpper();
               }
               else
               {
                  guid = covS.RelatingSpace.GlobalId.ToString();
                  relType = covS.RelatingSpace.GetType().Name.ToUpper();
               }
               if (_refBIMRLCommon.getLineNoFromMapping(guid) == null)
                  continue;       // skip "non" element guid in the relationship object

               IEnumerable<IIfcCovering> relCovs = covS.RelatedCoverings;
               foreach (IIfcCovering cov in relCovs)
               {
#if ORACLE
                  insRelConnect(ref cIngEle, guid, ref cIngEleTyp, relType,
                                 ref cIngAttrN, ref cIngAttrNBS, null, ref cIngAttrV, ref cIngAttrVBS, null,
                                 ref cEdEle, cov.GlobalId.ToString(), ref cEdEleTyp, cov.GetType().Name.ToUpper(),
                                 ref cEdAttrN, ref cEdAttrNBS, null, ref cEdAttrV, ref cEdAttrVBS, null,
                                 ref cAttrN, ref cAttrNBS, null, ref cAttrV, ref cAttrVBS, null,
                                 ref realEl, ref realElBS, null, ref realElTyp, ref realElTBS, null,
                                 ref relTyp, covS.GetType().Name.ToUpper());
#endif
#if POSTGRES
                  insRelConnect(command, guid, relType,
                                 null, null,
                                 cov.GlobalId.ToString(), cov.GetType().Name.ToUpper(),
                                 null, null,
                                 null, null,
                                 null, null,
                                 covS.GetType().Name.ToUpper());
#endif
               }
            }

            else if (conn is IIfcRelCoversBldgElements)
            {
               IIfcRelCoversBldgElements covE = conn as IIfcRelCoversBldgElements;
               if (_refBIMRLCommon.getLineNoFromMapping(covE.RelatingBuildingElement.GlobalId.ToString()) == null)
                  continue;       // skip "non" element guid in the relationship object

               IEnumerable<IIfcCovering> relCovs = covE.RelatedCoverings;
               foreach (IIfcCovering cov in relCovs)
               {
#if ORACLE
                  insRelConnect(ref cIngEle, covE.RelatingBuildingElement.GlobalId.ToString(), ref cIngEleTyp, covE.RelatingBuildingElement.GetType().Name.ToUpper(),
                                 ref cIngAttrN, ref cIngAttrNBS, null, ref cIngAttrV, ref cIngAttrVBS, null,
                                 ref cEdEle, cov.GlobalId.ToString(), ref cEdEleTyp, cov.GetType().Name.ToUpper(),
                                 ref cEdAttrN, ref cEdAttrNBS, null, ref cEdAttrV, ref cEdAttrVBS, null,
                                 ref cAttrN, ref cAttrNBS, null, ref cAttrV, ref cAttrVBS, null,
                                 ref realEl, ref realElBS, null, ref realElTyp, ref realElTBS, null,
                                 ref relTyp, covE.GetType().Name.ToUpper());
#endif
#if POSTGRES
                  insRelConnect(command, covE.RelatingBuildingElement.GlobalId.ToString(), covE.RelatingBuildingElement.GetType().Name.ToUpper(),
                                 null, null,
                                 cov.GlobalId.ToString(), cov.GetType().Name.ToUpper(),
                                 null, null,
                                 null, null,
                                 null, null,
                                 covE.GetType().Name.ToUpper());
#endif
               }
            }

            else if (conn is IIfcRelFlowControlElements)
            {
               // Handle Flow Control
               IIfcRelFlowControlElements connPE = conn as IIfcRelFlowControlElements;
               if (_refBIMRLCommon.getLineNoFromMapping(connPE.RelatingFlowElement.GlobalId.ToString()) == null)
                  continue;       // skip "non" element guid in the relationship object

               foreach (IIfcDistributionControlElement dist in connPE.RelatedControlElements)
               {
#if ORACLE
                  insRelConnect(ref cIngEle, connPE.RelatingFlowElement.GlobalId.ToString(), ref cIngEleTyp, connPE.RelatingFlowElement.GetType().Name.ToUpper(),
                                 ref cIngAttrN, ref cIngAttrNBS, null, ref cIngAttrV, ref cIngAttrVBS, null,
                                 ref cEdEle, dist.GlobalId.ToString(), ref cEdEleTyp, dist.GetType().Name.ToUpper(),
                                 ref cEdAttrN, ref cEdAttrNBS, null, ref cEdAttrV, ref cEdAttrVBS, null,
                                 ref cAttrN, ref cAttrNBS, null, ref cAttrV, ref cAttrVBS, null,
                                 ref realEl, ref realElBS, null, ref realElTyp, ref realElTBS, null,
                                 ref relTyp, connPE.GetType().Name.ToUpper());
#endif
#if POSTGRES
                  insRelConnect(command, connPE.RelatingFlowElement.GlobalId.ToString(), connPE.RelatingFlowElement.GetType().Name.ToUpper(),
                                 null, null,
                                 dist.GlobalId.ToString(), dist.GetType().Name.ToUpper(),
                                 null, null,
                                 null, null,
                                 null, null,
                                 connPE.GetType().Name.ToUpper());
#endif
               }
            }

            else if (conn is IIfcRelInterferesElements)
            {
               // Handle Interference
               IIfcRelInterferesElements connPE = conn as IIfcRelInterferesElements;
               if (_refBIMRLCommon.getLineNoFromMapping(connPE.RelatingElement.GlobalId.ToString()) == null)
                  continue;       // skip "non" element guid in the relationship object

               string interfN = null;
               string interfV = null;
               if (connPE.InterferenceType != null)
               {
                  interfN = "INTERFERENCETYPE";
                  interfV = connPE.InterferenceType.ToString();
               }

#if ORACLE
               insRelConnect(ref cIngEle, connPE.RelatingElement.GlobalId.ToString(), ref cIngEleTyp, connPE.RelatingElement.GetType().Name.ToUpper(),
                              ref cIngAttrN, ref cIngAttrNBS, null, ref cIngAttrV, ref cIngAttrVBS, null,
                              ref cEdEle, connPE.RelatedElement.GlobalId.ToString(), ref cEdEleTyp, connPE.RelatedElement.GetType().Name.ToUpper(),
                              ref cEdAttrN, ref cEdAttrNBS, null, ref cEdAttrV, ref cEdAttrVBS, null,
                              ref cAttrN, ref cAttrNBS, interfN, ref cAttrV, ref cAttrVBS, interfV,
                              ref realEl, ref realElBS, null, ref realElTyp, ref realElTBS, null,
                              ref relTyp, connPE.GetType().Name.ToUpper());
#endif
#if POSTGRES
               insRelConnect(command, connPE.RelatingElement.GlobalId.ToString(), connPE.RelatingElement.GetType().Name.ToUpper(),
                              null, null,
                              connPE.RelatedElement.GlobalId.ToString(), connPE.RelatedElement.GetType().Name.ToUpper(),
                              null, null,
                              interfN, interfV,
                              null, null,
                              connPE.GetType().Name.ToUpper());
#endif
            }

            else if (conn is IIfcRelReferencedInSpatialStructure)
            {
               // Handle referenced by Spatial Structure
               IIfcRelReferencedInSpatialStructure connPE = conn as IIfcRelReferencedInSpatialStructure;
               if (_refBIMRLCommon.getLineNoFromMapping(connPE.RelatingStructure.GlobalId.ToString()) == null)
                  continue;       // skip "non" element guid in the relationship object

               foreach (IIfcProduct elem in connPE.RelatedElements)
               {
#if ORACLE
                  insRelConnect(ref cIngEle, connPE.RelatingStructure.GlobalId.ToString(), ref cIngEleTyp, connPE.RelatingStructure.GetType().Name.ToUpper(),
                                 ref cIngAttrN, ref cIngAttrNBS, null, ref cIngAttrV, ref cIngAttrVBS, null,
                                 ref cEdEle, elem.GlobalId.ToString(), ref cEdEleTyp, elem.GetType().Name.ToUpper(),
                                 ref cEdAttrN, ref cEdAttrNBS, null, ref cEdAttrV, ref cEdAttrVBS, null,
                                 ref cAttrN, ref cAttrNBS, null, ref cAttrV, ref cAttrVBS, null,
                                 ref realEl, ref realElBS, null, ref realElTyp, ref realElTBS, null,
                                 ref relTyp, connPE.GetType().Name.ToUpper());
#endif
#if POSTGRES
                  insRelConnect(command, connPE.RelatingStructure.GlobalId.ToString(), connPE.RelatingStructure.GetType().Name.ToUpper(),
                                 null, null,
                                 elem.GlobalId.ToString(), elem.GetType().Name.ToUpper(),
                                 null, null,
                                 null, null,
                                 null, null,
                                 connPE.GetType().Name.ToUpper());
#endif
               }
            }

            else if (conn is IIfcRelServicesBuildings)
            {
               // Handle MEP connections through IfcDistributionPorts (Port will not be captured)
               IIfcRelServicesBuildings connPE = conn as IIfcRelServicesBuildings;
               if (_refBIMRLCommon.getLineNoFromMapping(connPE.RelatingSystem.GlobalId.ToString()) == null)
                  continue;       // skip "non" element guid in the relationship object

               foreach (IIfcSpatialStructureElement bldg in connPE.RelatedBuildings)
               {
#if ORACLE
                  insRelConnect(ref cIngEle, connPE.RelatingSystem.GlobalId.ToString(), ref cIngEleTyp, connPE.RelatingSystem.GetType().Name.ToUpper(),
                                 ref cIngAttrN, ref cIngAttrNBS, null, ref cIngAttrV, ref cIngAttrVBS, null,
                                 ref cEdEle, bldg.GlobalId.ToString(), ref cEdEleTyp, bldg.GetType().Name.ToUpper(),
                                 ref cEdAttrN, ref cEdAttrNBS, null, ref cEdAttrV, ref cEdAttrVBS, null,
                                 ref cAttrN, ref cAttrNBS, null, ref cAttrV, ref cAttrVBS, null,
                                 ref realEl, ref realElBS, null, ref realElTyp, ref realElTBS, null,
                                 ref relTyp, connPE.GetType().Name.ToUpper());
#endif
#if POSTGRES
                  insRelConnect(command, connPE.RelatingSystem.GlobalId.ToString(), connPE.RelatingSystem.GetType().Name.ToUpper(),
                                 null, null,
                                 bldg.GlobalId.ToString(), bldg.GetType().Name.ToUpper(),
                                 null, null,
                                 null, null,
                                 null, null,
                                 connPE.GetType().Name.ToUpper());
#endif
               }
            }
            else if (conn is Xbim.Ifc2x3.StructuralAnalysisDomain.IfcRelConnectsStructuralElement)
            {
               Xbim.Ifc2x3.Interfaces.IIfcRelConnectsStructuralElement connPE = conn as Xbim.Ifc2x3.Interfaces.IIfcRelConnectsStructuralElement;
               if (_refBIMRLCommon.getLineNoFromMapping(connPE.RelatingElement.GlobalId.ToString()) == null)
                     continue;       // skip "non" element guid in the relationship object

               IIfcStructuralMember stru = connPE.RelatedStructuralMember as IIfcStructuralMember;
#if ORACLE
               insRelConnect(ref cIngEle, connPE.RelatingElement.GlobalId.ToString(), ref cIngEleTyp, connPE.RelatingElement.GetType().Name.ToUpper(),
                              ref cIngAttrN, ref cIngAttrNBS, null, ref cIngAttrV, ref cIngAttrVBS, null,
                              ref cEdEle, stru.GlobalId.ToString(), ref cEdEleTyp, stru.GetType().Name.ToUpper(),
                              ref cEdAttrN, ref cEdAttrNBS, null, ref cEdAttrV, ref cEdAttrVBS, null,
                              ref cAttrN, ref cAttrNBS, null, ref cAttrV, ref cAttrVBS, null,
                              ref realEl, ref realElBS, null, ref realElTyp, ref realElTBS, null,
                              ref relTyp, connPE.GetType().Name.ToUpper());
#endif
#if POSTGRES
               insRelConnect(command, connPE.RelatingElement.GlobalId.ToString(), connPE.RelatingElement.GetType().Name.ToUpper(),
                              null, null,
                              stru.GlobalId.ToString(), stru.GetType().Name.ToUpper(),
                              null, null,
                              null, null,
                              null, null,
                              connPE.GetType().Name.ToUpper());
#endif
            }

            else
            {
               // Unsupported type!
            }

#if ORACLE
            if (cIngEle.Count >= DBOperation.commitInterval)
            {
               Param[0].Value = cIngEle.ToArray();
               Param[0].Size = cIngEle.Count;
               Param[1].Value = cIngEleTyp.ToArray();
               Param[1].Size = cIngEleTyp.Count;
               Param[2].Value = cIngAttrN.ToArray();
               Param[2].Size = cIngAttrN.Count;
               Param[2].ArrayBindStatus = cIngAttrNBS.ToArray();
               Param[3].Value = cIngAttrV.ToArray();
               Param[3].Size = cIngAttrV.Count;
               Param[3].ArrayBindStatus = cIngAttrVBS.ToArray();
               Param[4].Value = cEdEle.ToArray();
               Param[4].Size = cEdEle.Count;
               Param[5].Value = cEdEleTyp.ToArray();
               Param[5].Size = cEdEleTyp.Count;
               Param[6].Value = cEdAttrN.ToArray();
               Param[6].Size = cEdAttrN.Count;
               Param[6].ArrayBindStatus = cEdAttrNBS.ToArray();
               Param[7].Value = cEdAttrV.ToArray();
               Param[7].Size = cEdAttrV.Count;
               Param[7].ArrayBindStatus = cEdAttrVBS.ToArray();
               Param[8].Value = cAttrN.ToArray();
               Param[8].Size = cAttrN.Count;
               Param[8].ArrayBindStatus = cAttrNBS.ToArray();
               Param[9].Value = cAttrV.ToArray();
               Param[9].Size = cAttrV.Count;
               Param[9].ArrayBindStatus = cAttrVBS.ToArray();
               Param[10].Value = realEl.ToArray();
               Param[10].Size = realEl.Count;
               Param[10].ArrayBindStatus = realElBS.ToArray();
               Param[11].Value = realElTyp.ToArray();
               Param[11].Size = realElTyp.Count;
               Param[11].ArrayBindStatus = realElTBS.ToArray();
               Param[12].Value = relTyp.ToArray();
               Param[12].Size = relTyp.Count;
               try
               {
                  command.ArrayBindCount = cIngEle.Count;    // No of values in the array to be inserted
                  command.ExecuteNonQuery();
                  DBOperation.commitTransaction();

                  cIngEle.Clear();
                  cIngEleTyp.Clear();
                  cIngAttrN.Clear();
                  cIngAttrV.Clear();
                  cEdEle.Clear();
                  cEdEleTyp.Clear();
                  cEdAttrN.Clear();
                  cEdAttrV.Clear();
                  cAttrN.Clear();
                  cAttrV.Clear();
                  realEl.Clear();
                  realElTyp.Clear();
                  cIngAttrNBS.Clear();
                  cIngAttrVBS.Clear();
                  cEdAttrNBS.Clear();
                  cEdAttrVBS.Clear();
                  cAttrNBS.Clear();
                  cAttrVBS.Clear();
                  realElBS.Clear();
                  realElTBS.Clear();
                  relTyp.Clear();
               }
               catch (OracleException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushIgnorableError(excStr);
                  // Ignore any error
                  cIngEle.Clear();
                  cIngEleTyp.Clear();
                  cIngAttrN.Clear();
                  cIngAttrV.Clear();
                  cEdEle.Clear();
                  cEdEleTyp.Clear();
                  cEdAttrN.Clear();
                  cEdAttrV.Clear();
                  cAttrN.Clear();
                  cAttrV.Clear();
                  realEl.Clear();
                  realElTyp.Clear();
                  cIngAttrNBS.Clear();
                  cIngAttrVBS.Clear();
                  cEdAttrNBS.Clear();
                  cEdAttrVBS.Clear();
                  cAttrNBS.Clear();
                  cAttrVBS.Clear();
                  realElBS.Clear();
                  realElTBS.Clear();
                  relTyp.Clear();
                  continue;
               }
               catch (SystemException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushError(excStr);
                  throw;
               }
            }
#endif
         }

#if ORACLE
         if (cIngEle.Count > 0)
         {
            Param[0].Value = cIngEle.ToArray();
            Param[0].Size = cIngEle.Count;
            Param[1].Value = cIngEleTyp.ToArray();
            Param[1].Size = cIngEleTyp.Count;
            Param[2].Value = cIngAttrN.ToArray();
            Param[2].Size = cIngAttrN.Count;
            Param[2].ArrayBindStatus = cIngAttrNBS.ToArray();
            Param[3].Value = cIngAttrV.ToArray();
            Param[3].Size = cIngAttrV.Count;
            Param[3].ArrayBindStatus = cIngAttrVBS.ToArray();
            Param[4].Value = cEdEle.ToArray();
            Param[4].Size = cEdEle.Count;
            Param[5].Value = cEdEleTyp.ToArray();
            Param[5].Size = cEdEleTyp.Count;
            Param[6].Value = cEdAttrN.ToArray();
            Param[6].Size = cEdAttrN.Count;
            Param[6].ArrayBindStatus = cEdAttrNBS.ToArray();
            Param[7].Value = cEdAttrV.ToArray();
            Param[7].Size = cEdAttrV.Count;
            Param[7].ArrayBindStatus = cEdAttrVBS.ToArray();
            Param[8].Value = cAttrN.ToArray();
            Param[8].Size = cAttrN.Count;
            Param[8].ArrayBindStatus = cAttrNBS.ToArray();
            Param[9].Value = cAttrV.ToArray();
            Param[9].Size = cAttrV.Count;
            Param[9].ArrayBindStatus = cAttrVBS.ToArray();
            Param[10].Value = realEl.ToArray();
            Param[10].Size = realEl.Count;
            Param[10].ArrayBindStatus = realElBS.ToArray();
            Param[11].Value = realElTyp.ToArray();
            Param[11].Size = realElTyp.Count;
            Param[11].ArrayBindStatus = realElTBS.ToArray();
            Param[12].Value = relTyp.ToArray();
            Param[12].Size = relTyp.Count;

            try
            {
               command.ArrayBindCount = cIngEle.Count;    // No of values in the array to be inserted
               command.ExecuteNonQuery();
               DBOperation.commitTransaction();
            }
            catch (OracleException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushIgnorableError(excStr);
            }
            catch (SystemException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushError(excStr);
               throw;
            }
         }
#endif
         DBOperation.commitTransaction();
         command.Dispose();
      }

      /// <summary>
      /// Special treatment on Space boundary:
      /// 1. No duplicate of space, boundary is inserted. the pair is checked first from the local dictionary before value is setup
      /// 2. Virtual element is resolved to become Space1 - Space2 space and boundary relationship 
      /// </summary>
      private void processRelSpaceBoundary()
      {
         string sqlStmt;
         string currStep = string.Empty;
         var spBIndex = new Dictionary<Tuple<string, string>, int>();    // Keep the index pair in the dictionary to avoid duplicate

         DBOperation.beginTransaction();
#if ORACLE
         sqlStmt = "insert into " + DBOperation.formatTabName("BIMRL_RELSPACEBOUNDARY")
                     + " (SPACEELEMENTID, BOUNDARYELEMENTID, BOUNDARYELEMENTTYPE, BOUNDARYTYPE, INTERNALOREXTERNAL) values (:1, :2, :3, :4, :5)";
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);

         OracleParameter[] Param = new OracleParameter[5];
         for (int i = 0; i < 5; i++)
         {
            Param[i] = command.Parameters.Add((i + 1).ToString(), OracleDbType.Varchar2);
            Param[i].Direction = ParameterDirection.Input;
         }


         List<string> arrSpaceGuids = new List<string>();
         List<string> arrBoundGuids = new List<string>();
         List<string> arrBoundEleTypes = new List<string>();
         List<string> arrBoundTypes = new List<string>();
         List<string> arrIntOrExt = new List<string>();
#endif
#if POSTGRES
         sqlStmt = "insert into " + DBOperation.formatTabName("BIMRL_RELSPACEBOUNDARY")
                     + " (SPACEELEMENTID, BOUNDARYELEMENTID, BOUNDARYELEMENTTYPE, BOUNDARYTYPE, INTERNALOREXTERNAL) values (@sid, @bid, @betyp, @btyp, @intext)";
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);

         command.Parameters.Add("@sid", NpgsqlDbType.Varchar);
         command.Parameters.Add("@bid", NpgsqlDbType.Varchar);
         command.Parameters.Add("@betyp", NpgsqlDbType.Varchar);
         command.Parameters.Add("@btyp", NpgsqlDbType.Varchar);
         command.Parameters.Add("@intext", NpgsqlDbType.Varchar);
         command.Prepare();
#endif
         currStep = sqlStmt;

         IEnumerable<IIfcRelSpaceBoundary> rels = _model.Instances.OfType<IIfcRelSpaceBoundary>();
         foreach (IIfcRelSpaceBoundary spb in rels)
         {
            string spaceGuid;
            if (spb.RelatingSpace is IIfcSpace)
               spaceGuid = (spb.RelatingSpace as IIfcSpace).GlobalId.ToString();
            else
               spaceGuid = (spb.RelatingSpace as IIfcExternalSpatialElement).GlobalId.ToString();

            if ((_refBIMRLCommon.getLineNoFromMapping(spaceGuid) == null) || (spb.RelatedBuildingElement == null))
               continue;   // skip relationship that involves "non" element Guids

            if (spb.RelatedBuildingElement is IIfcVirtualElement)
            {
               // We will ignore the virtual element and instead get the conencted space as the related boundary
               IIfcVirtualElement ve = spb.RelatedBuildingElement as IIfcVirtualElement;
               // get the element this IfcVirtualElement is connected to
               IEnumerable<IIfcRelSpaceBoundary> veSpaces = ve.ProvidesBoundaries;
               foreach (IIfcRelSpaceBoundary veSp in veSpaces)
               {
                  int lineNo;
                  string space2Guid;
                  if (veSp.RelatingSpace is IIfcSpace)
                     space2Guid = (veSp.RelatingSpace as IIfcSpace).GlobalId.ToString();
                  else
                     space2Guid = (veSp.RelatingSpace as IIfcExternalSpatialElement).GlobalId.ToString();

                  string space2Type = veSp.RelatingSpace.GetType().Name.ToUpper();
                  if (spb.RelatingSpace == veSp.RelatingSpace)
                        continue;       // It points aback to itself, skip
                  if (spBIndex.TryGetValue(Tuple.Create(spaceGuid, space2Guid), out lineNo))
                        continue;       // existing pair found, skip this pair

#if ORACLE
                  insSpaceBoundary(ref arrSpaceGuids, spaceGuid, ref arrBoundGuids, space2Guid, ref arrBoundEleTypes, space2Type,
                     ref arrBoundTypes, spb.PhysicalOrVirtualBoundary.ToString(), ref arrIntOrExt, spb.InternalOrExternalBoundary.ToString());
#endif
#if POSTGRES
                  insSpaceBoundary(command, spaceGuid, space2Guid, space2Type, spb.PhysicalOrVirtualBoundary.ToString(), spb.InternalOrExternalBoundary.ToString());
#endif
                  spBIndex.Add(Tuple.Create(spaceGuid, space2Guid), spb.EntityLabel);
               }
            }
            else
            {
               int lineNo;
               string boundGuid = spb.RelatedBuildingElement.GlobalId.ToString();
               string boundType = spb.RelatedBuildingElement.GetType().Name.ToUpper();

               if (spBIndex.TryGetValue(Tuple.Create(spaceGuid, boundGuid), out lineNo))
                  continue;       // existing pair found, skip this pair

#if ORACLE
               insSpaceBoundary(ref arrSpaceGuids, spaceGuid, ref arrBoundGuids, boundGuid, ref arrBoundEleTypes, boundType,
                  ref arrBoundTypes, spb.PhysicalOrVirtualBoundary.ToString(), ref arrIntOrExt, spb.InternalOrExternalBoundary.ToString());
#endif
#if POSTGRES
               insSpaceBoundary(command, spaceGuid, boundGuid, boundType, spb.PhysicalOrVirtualBoundary.ToString(), spb.InternalOrExternalBoundary.ToString());
#endif

               spBIndex.Add(Tuple.Create(spaceGuid, boundGuid), spb.EntityLabel);
            }

#if ORACLE
            if (arrSpaceGuids.Count >= DBOperation.commitInterval)
            {
               Param[0].Size = arrSpaceGuids.Count();
               Param[0].Value = arrSpaceGuids.ToArray();
               Param[1].Size = arrBoundGuids.Count();
               Param[1].Value = arrBoundGuids.ToArray();
               Param[2].Size = arrBoundEleTypes.Count();
               Param[2].Value = arrBoundEleTypes.ToArray();
               Param[3].Size = arrBoundTypes.Count();
               Param[3].Value = arrBoundTypes.ToArray();
               Param[4].Size = arrIntOrExt.Count();
               Param[4].Value = arrIntOrExt.ToArray();
               try
               {
                  command.ArrayBindCount = arrSpaceGuids.Count;    // No of values in the array to be inserted
                  command.ExecuteNonQuery();
                  //Do commit at interval but keep the long transaction (reopen)
                  DBOperation.commitTransaction();
                  arrSpaceGuids.Clear();
                  arrBoundGuids.Clear();
                  arrBoundEleTypes.Clear();
                  arrBoundTypes.Clear();
                  arrIntOrExt.Clear();
               }
               catch (OracleException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushIgnorableError(excStr);
                  // Ignore any error
                  arrSpaceGuids.Clear();
                  arrBoundGuids.Clear();
                  arrBoundEleTypes.Clear();
                  arrBoundTypes.Clear();
                  arrIntOrExt.Clear();
                  continue;
               }
               catch (SystemException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushError(excStr);
                  throw;
               }
            }
#endif
         }

#if ORACLE
         if (arrSpaceGuids.Count > 0)
         {
            Param[0].Size = arrSpaceGuids.Count();
            Param[0].Value = arrSpaceGuids.ToArray();
            Param[1].Size = arrBoundGuids.Count();
            Param[1].Value = arrBoundGuids.ToArray();
            Param[2].Size = arrBoundEleTypes.Count();
            Param[2].Value = arrBoundEleTypes.ToArray();
            Param[3].Size = arrBoundTypes.Count();
            Param[3].Value = arrBoundTypes.ToArray();
            Param[4].Size = arrIntOrExt.Count();
            Param[4].Value = arrIntOrExt.ToArray();

            try
            {
               command.ArrayBindCount = arrSpaceGuids.Count;    // No of values in the array to be inserted
               command.ExecuteNonQuery();
               //Do commit at interval but keep the long transaction (reopen)
               DBOperation.commitTransaction();
            }
            catch (OracleException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushIgnorableError(excStr);
            }
            catch (SystemException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushError(excStr);
               throw;
            }
         }
#endif

         DBOperation.commitTransaction();
         command.Dispose();
      }

#if ORACLE
      private void insSpaceBoundary(ref List<string> arrSpaceGuids, string spaceGuid, ref List<string> arrBoundGuids, string boundGuid, ref List<string> arrBoundEleTypes, string boundElemType,
                                    ref List<string> arrBoundTypes, string boundaryType, ref List<string> arrIntOrExt, string internalOrExternal)
      {
         arrSpaceGuids.Add(spaceGuid);
         arrBoundGuids.Add(boundGuid);
         arrBoundEleTypes.Add(boundElemType);
         arrBoundTypes.Add(boundaryType);
         arrIntOrExt.Add(internalOrExternal);
      }
#endif
#if POSTGRES
      private void insSpaceBoundary(NpgsqlCommand command, string spaceGuid, string boundGuid, string BoundElemType, string boundaryType, string internalOrExternal)
      {
         command.Parameters["@sid"].Value = spaceGuid;
         command.Parameters["@bid"].Value = boundGuid;
         command.Parameters["@betyp"].Value = BoundElemType;
         if (string.IsNullOrEmpty(boundaryType))
            command.Parameters["@btyp"].Value = DBNull.Value;
         else
            command.Parameters["@btyp"].Value = boundaryType;
         if (string.IsNullOrEmpty(internalOrExternal))
            command.Parameters["@intext"].Value = DBNull.Value;
         else
            command.Parameters["@intext"].Value = internalOrExternal;

         try
         {
            DBOperation.CurrTransaction.Save(DBOperation.def_savepoint);
            command.ExecuteNonQuery();
            //Do commit at interval but keep the long transaction (reopen)
            DBOperation.commitTransaction();
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

      private void processRelGroup()
      {
         string sqlStmt;
         string currStep = string.Empty;

         DBOperation.beginTransaction();

         int commandStatus = -1;

#if ORACLE
         sqlStmt = "insert into " + DBOperation.formatTabName("BIMRL_RELGROUP")
                     + " (GROUPELEMENTID, GROUPELEMENTTYPE, MEMBERELEMENTID, MEMBERELEMENTTYPE) values (:1, :2, :3, :4)";
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);

         OracleParameter[] Param = new OracleParameter[4];
         for (int i = 0; i < 4; i++)
         {
            Param[i] = command.Parameters.Add((i + 1).ToString(), OracleDbType.Varchar2);
            Param[i].Direction = ParameterDirection.Input;
         }

         List<string> arrGroupGuids = new List<string>();
         List<string> arrGroupTypes = new List<string>();
         List<string> arrMemberGuids = new List<string>();
         List<string> arrMemberTypes = new List<string>();
#endif
#if POSTGRES
         sqlStmt = "insert into " + DBOperation.formatTabName("BIMRL_RELGROUP")
                     + " (GROUPELEMENTID, GROUPELEMENTTYPE, MEMBERELEMENTID, MEMBERELEMENTTYPE) values (@gid, @gtyp, @mid, @mtyp)";
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);

         command.Parameters.Add("@gid", NpgsqlDbType.Varchar);
         command.Parameters.Add("@gtyp", NpgsqlDbType.Varchar);
         command.Parameters.Add("@mid", NpgsqlDbType.Varchar);
         command.Parameters.Add("@mtyp", NpgsqlDbType.Varchar);
         command.Prepare();
#endif
         currStep = sqlStmt;

         // IEnumerable<IfcRelAssignsToGroup> rels = _model.InstancesLocal.OfType<IfcRelAssignsToGroup>(true).Where(gr => gr.RelatingGroup is IfcSystem || gr.RelatingGroup is IfcZone);
         // Handle other types of Group too
         IEnumerable<IIfcRelAssignsToGroup> rels = _model.Instances.OfType<IIfcRelAssignsToGroup>();
         foreach (IIfcRelAssignsToGroup rGr in rels)
         {
            string grpGuid = rGr.RelatingGroup.GlobalId.ToString();
            if (_refBIMRLCommon.getLineNoFromMapping(grpGuid) == null)
               continue;   // skip relationship if the Group GUID does not exist

            string grType = rGr.RelatingGroup.GetType().Name.ToUpper();

            IEnumerable<IIfcObjectDefinition> members = rGr.RelatedObjects;

#if ORACLE
            foreach (IIfcObjectDefinition oDef in members)
            {
               string memberGuid = oDef.GlobalId.ToString();
               string memberType = oDef.GetType().Name.ToUpper();
               if (_refBIMRLCommon.getLineNoFromMapping(memberGuid) == null)
                  continue;       // Skip if member is not loaded into BIMRL_ELEMENT already

               arrGroupGuids.Add(grpGuid);
               arrGroupTypes.Add(grType);
               arrMemberGuids.Add(memberGuid);
               arrMemberTypes.Add(memberType);
            }

            if (arrGroupGuids.Count >= DBOperation.commitInterval)
            {
               Param[0].Size = arrGroupGuids.Count();
               Param[0].Value = arrGroupGuids.ToArray();
               Param[1].Size = arrGroupTypes.Count();
               Param[1].Value = arrGroupTypes.ToArray();
               Param[2].Size = arrMemberGuids.Count();
               Param[2].Value = arrMemberGuids.ToArray();
               Param[3].Size = arrMemberTypes.Count();
               Param[3].Value = arrMemberTypes.ToArray();
               try
               {
                  command.ArrayBindCount = arrGroupGuids.Count;    // No of values in the array to be inserted
                  commandStatus = command.ExecuteNonQuery();
                  //Do commit at interval but keep the long transaction (reopen)
                  DBOperation.commitTransaction();
                  arrGroupGuids.Clear();
                  arrGroupTypes.Clear();
                  arrMemberGuids.Clear();
                  arrMemberTypes.Clear();
               }
               catch (OracleException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushIgnorableError(excStr);
                  // Ignore any error
                  arrGroupGuids.Clear();
                  arrGroupTypes.Clear();
                  arrMemberGuids.Clear();
                  arrMemberTypes.Clear();
                  continue;
               }
               catch (SystemException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushError(excStr);
                  throw;
               }
            }
#endif
#if POSTGRES
            foreach (IIfcObjectDefinition oDef in members)
            {
               string memberGuid = oDef.GlobalId.ToString();
               string memberType = oDef.GetType().Name.ToUpper();
               if (_refBIMRLCommon.getLineNoFromMapping(memberGuid) == null)
                  continue;       // Skip if member is not loaded into BIMRL_ELEMENT already

               command.Parameters["@gid"].Value = grpGuid;
               command.Parameters["@gtyp"].Value = grType;
               command.Parameters["@mid"].Value = memberGuid;
               command.Parameters["@mtyp"].Value = memberType;

               try
               {
                  DBOperation.CurrTransaction.Save(DBOperation.def_savepoint);
                  commandStatus = command.ExecuteNonQuery();
                  //Do commit at interval but keep the long transaction (reopen)
                  DBOperation.commitTransaction();
               }
               catch (NpgsqlException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushIgnorableError(excStr);
                  // Ignore any error
                  DBOperation.CurrTransaction.Rollback(DBOperation.def_savepoint);
                  continue;
               }
               catch (SystemException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushError(excStr);
                  throw;
               }
            }
#endif
         }

#if ORACLE
         if (arrGroupGuids.Count > 0)
         {
            Param[0].Size = arrGroupGuids.Count();
            Param[0].Value = arrGroupGuids.ToArray();
            Param[1].Size = arrGroupTypes.Count();
            Param[1].Value = arrGroupTypes.ToArray();
            Param[2].Size = arrMemberGuids.Count();
            Param[2].Value = arrMemberGuids.ToArray();
            Param[3].Size = arrMemberTypes.Count();
            Param[3].Value = arrMemberTypes.ToArray();

            try
            {
               command.ArrayBindCount = arrGroupGuids.Count;    // No of values in the array to be inserted
               commandStatus = command.ExecuteNonQuery();
               //Do commit at interval but keep the long transaction (reopen)
               DBOperation.commitTransaction();
            }
            catch (OracleException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushIgnorableError(excStr);
            }
            catch (SystemException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushError(excStr);
               throw;
            }
         }
#endif
         DBOperation.commitTransaction();
         command.Dispose();
      }

      private void processElemDependency()
      {
         List<string> cEleId = new List<string>();
         List<string> cEleTyp = new List<string>();
         List<string> cDepend = new List<string>();
         List<string> cDependTyp = new List<string>();
         List<string> cDepTyp = new List<string>();

         IEnumerable<IIfcRelVoidsElement> relVoids = _model.Instances.OfType<IIfcRelVoidsElement>(true);
         foreach (IIfcRelVoidsElement connPE in relVoids)
         {
            if ((_refBIMRLCommon.getLineNoFromMapping(connPE.RelatingBuildingElement.GlobalId.ToString()) == null)
               || (_refBIMRLCommon.getLineNoFromMapping(connPE.RelatedOpeningElement.GlobalId.ToString()) == null))
               continue;       // skip "non" element guid in the relationship object

            cEleId.Add(connPE.RelatingBuildingElement.GlobalId.ToString());
            cEleTyp.Add(connPE.RelatingBuildingElement.GetType().Name.ToUpper());
            cDepend.Add(connPE.RelatedOpeningElement.GlobalId.ToString());
            cDependTyp.Add(connPE.RelatedOpeningElement.GetType().Name.ToUpper());
            cDepTyp.Add(connPE.GetType().Name.ToUpper());
         }
         InsertDependencyRecords(cEleId, cEleTyp, cDepend, cDependTyp, cDepTyp);
         cEleId.Clear();
         cEleTyp.Clear();
         cDepend.Clear();
         cDependTyp.Clear();
         cDepTyp.Clear();

         IEnumerable<IIfcRelProjectsElement> relProjects = _model.Instances.OfType<IIfcRelProjectsElement>(true);
         foreach (IIfcRelProjectsElement connPE in relProjects)
         {
            if ((_refBIMRLCommon.getLineNoFromMapping(connPE.RelatingElement.GlobalId.ToString()) == null)
               || (_refBIMRLCommon.getLineNoFromMapping(connPE.RelatedFeatureElement.GlobalId.ToString()) == null))
               continue;       // skip "non" element guid in the relationship object

            cEleId.Add(connPE.RelatingElement.GlobalId.ToString());
            cEleTyp.Add(connPE.RelatingElement.GetType().Name.ToUpper());
            cDepend.Add(connPE.RelatedFeatureElement.GlobalId.ToString());
            cDependTyp.Add(connPE.RelatedFeatureElement.GetType().Name.ToUpper());
            cDepTyp.Add(connPE.GetType().Name.ToUpper());
         }
         InsertDependencyRecords(cEleId, cEleTyp, cDepend, cDependTyp, cDepTyp);
         cEleId.Clear();
         cEleTyp.Clear();
         cDepend.Clear();
         cDependTyp.Clear();
         cDepTyp.Clear();

         IEnumerable<IIfcRelFillsElement> relFills = _model.Instances.OfType<IIfcRelFillsElement>(true);
         foreach (IIfcRelFillsElement connPE in relFills)
         {
            if ((_refBIMRLCommon.getLineNoFromMapping(connPE.RelatingOpeningElement.GlobalId.ToString()) == null)
               || (_refBIMRLCommon.getLineNoFromMapping(connPE.RelatedBuildingElement.GlobalId.ToString()) == null))
               continue;       // skip "non" element guid in the relationship object

            cEleId.Add(connPE.RelatingOpeningElement.GlobalId.ToString());
            cEleTyp.Add(connPE.RelatingOpeningElement.GetType().Name.ToUpper());
            cDepend.Add(connPE.RelatedBuildingElement.GlobalId.ToString());
            cDependTyp.Add(connPE.RelatedBuildingElement.GetType().Name.ToUpper());
            cDepTyp.Add(connPE.GetType().Name.ToUpper());
         }
         InsertDependencyRecords(cEleId, cEleTyp, cDepend, cDependTyp, cDepTyp);
         cEleId.Clear();
         cEleTyp.Clear();
         cDepend.Clear();
         cDependTyp.Clear();
         cDepTyp.Clear();

         IEnumerable<IIfcRelNests> relNests = _model.Instances.OfType<IIfcRelNests>(true);
         foreach (IIfcRelNests connPE in relNests)
         {
            string relatingObject = connPE.RelatingObject.GlobalId.ToString();
            if (_refBIMRLCommon.getLineNoFromMapping(relatingObject) == null)
               continue;
            string relatingType = connPE.RelatingObject.GetType().Name.ToUpper();
            string depTyp = connPE.GetType().Name.ToUpper();

            foreach (IIfcObjectDefinition relatedObj in connPE.RelatedObjects)
            {
               string relatedObjGUID = relatedObj.GlobalId.ToString();
               if (_refBIMRLCommon.getLineNoFromMapping(relatedObjGUID) == null)
                  continue;       // skip "non" element guid in the relationship object

               cEleId.Add(relatingObject);
               cEleTyp.Add(relatingType);
               cDepend.Add(relatedObjGUID);
               cDependTyp.Add(relatedObj.GetType().Name.ToUpper());
               cDepTyp.Add(depTyp);
            }
         }
         InsertDependencyRecords(cEleId, cEleTyp, cDepend, cDependTyp, cDepTyp);
         cEleId.Clear();
         cEleTyp.Clear();
         cDepend.Clear();
         cDependTyp.Clear();
         cDepTyp.Clear();
         DBOperation.commitTransaction();
      }

#if ORACLE
      private void InsertDependencyRecords(List<string> cEleId, List<string> cEleTyp, List<string> cDepend, List<string> cDependTyp, List<string> cDepTyp)
      {
         string sqlStmt = "insert into " + DBOperation.formatTabName("BIMRL_ELEMENTDEPENDENCY")
                     + " (ELEMENTID, ELEMENTTYPE, DEPENDENTELEMENTID, DEPENDENTELEMENTTYPE, DEPENDENCYTYPE) "
                     + "VALUES (:1, :2, :3, :4, :5)";
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
         string currStep = sqlStmt;
         int commandStatus = -1;

         DBOperation.beginTransaction();

         OracleParameter[] Param = new OracleParameter[5];
         for (int i = 0; i < 5; i++)
         {
            Param[i] = command.Parameters.Add((i + 1).ToString(), OracleDbType.Varchar2);
            Param[i].Direction = ParameterDirection.Input;
         }

         if (cEleId.Count > 0)
         {
            Param[0].Size = cEleId.Count();
            Param[0].Value = cEleId.ToArray();
            Param[1].Size = cEleTyp.Count();
            Param[1].Value = cEleTyp.ToArray();
            Param[2].Size = cDepend.Count();
            Param[2].Value = cDepend.ToArray();
            Param[3].Size = cDependTyp.Count();
            Param[3].Value = cDependTyp.ToArray();
            Param[4].Size = cDepTyp.Count();
            Param[4].Value = cDepTyp.ToArray();
            try
            {
               command.ArrayBindCount = cDepTyp.Count;    // No of values in the array to be inserted
               commandStatus = command.ExecuteNonQuery();
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

            DBOperation.commitTransaction();
            command.Dispose();
         }
      }
#endif
#if POSTGRES
      private void InsertDependencyRecords(List<string> cEleId, List<string> cEleTyp, List<string> cDepend, List<string> cDependTyp, List<string> cDepTyp)
      {
         string sqlStmt = "insert into " + DBOperation.formatTabName("BIMRL_ELEMENTDEPENDENCY")
                     + " (ELEMENTID, ELEMENTTYPE, DEPENDENTELEMENTID, DEPENDENTELEMENTTYPE, DEPENDENCYTYPE) "
                     + "VALUES (@eid, @etyp, @did, @detyp, @dtyp)";
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
         string currStep = sqlStmt;
         int commandStatus = -1;
         command.Parameters.Add("@eid", NpgsqlDbType.Varchar);
         command.Parameters.Add("@etyp", NpgsqlDbType.Varchar);
         command.Parameters.Add("@did", NpgsqlDbType.Varchar);
         command.Parameters.Add("@detyp", NpgsqlDbType.Varchar);
         command.Parameters.Add("@dtyp", NpgsqlDbType.Varchar);
         command.Prepare();

         DBOperation.beginTransaction();

         for (int i = 0; i < cEleId.Count; ++i)
         {
            command.Parameters["@eid"].Value = cEleId[i];
            command.Parameters["@etyp"].Value = cEleTyp[i];
            command.Parameters["@did"].Value = cDepend[i];
            command.Parameters["@detyp"].Value = cDependTyp[i];
            command.Parameters["@dtyp"].Value = cDepTyp[i];

            try
            {
               DBOperation.CurrTransaction.Save(DBOperation.def_savepoint);
               commandStatus = command.ExecuteNonQuery();
               DBOperation.commitTransaction();
            }
            catch (NpgsqlException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushIgnorableError(excStr);
               // Ignore any error
               DBOperation.CurrTransaction.Rollback(DBOperation.def_savepoint);
            }
            catch (SystemException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushError(excStr);
               throw;
            }
         }

         DBOperation.commitTransaction();
         command.Dispose();
      }
#endif

#if ORACLE
      void insRelConnect(ref List<string> cIngEle, string connectingId, ref List<string> cIngEleTyp, string connectingType, 
                           ref List<string> cIngAttrN, ref List<OracleParameterStatus> cIngAttrNBS, string connectingAttr,
                           ref List<string> cIngAttrV, ref List<OracleParameterStatus> cIngAttrVBS, string connectingAttrVal,
                           ref List<string> cEdEle, string connectedId, ref List<string> cEdEleTyp, string connectedType,
                           ref List<string> cEdAttrN, ref List<OracleParameterStatus> cEdAttrNBS, string connectedAttr,
                           ref List<string> cEdAttrV, ref List<OracleParameterStatus> cEdAttrVBS, string connectedAttrVal,
                           ref List<string> cAttrN, ref List<OracleParameterStatus> cAttrNBS, string connectionAttr,
                           ref List<string> cAttrV, ref List<OracleParameterStatus> cAttrVBS, string connectionAttrVal, 
                           ref List<string> realEl, ref List<OracleParameterStatus> realElBS, string realizingElId,
                           ref List<string> realElTyp, ref List<OracleParameterStatus> realElTBS, string realizingElType, 
                           ref List<string> relTyp, string relationshipType)
      {
         cIngEle.Add(connectingId);
         cIngEleTyp.Add(connectingType);
         cEdEle.Add(connectedId);
         cEdEleTyp.Add(connectedType);

         if (!string.IsNullOrEmpty(connectingAttr))
         {
            cIngAttrN.Add("RELATINGCONNECTIONTYPE");
            cIngAttrNBS.Add(OracleParameterStatus.Success);
         }
         else
         {
            cIngAttrN.Add(string.Empty);
            cIngAttrNBS.Add(OracleParameterStatus.NullInsert);
         }

         if (!string.IsNullOrEmpty(connectingAttrVal))
         {
            cIngAttrV.Add(connectingAttrVal);
            cIngAttrVBS.Add(OracleParameterStatus.Success);
         }
         else
         {
            cIngAttrV.Add(string.Empty);
            cIngAttrVBS.Add(OracleParameterStatus.NullInsert);
         }

         if (!string.IsNullOrEmpty(connectedAttr))
         {
            cEdAttrN.Add(connectedAttr);
            cEdAttrNBS.Add(OracleParameterStatus.Success);
         }
         else
         {
            cEdAttrN.Add(string.Empty);
            cEdAttrNBS.Add(OracleParameterStatus.NullInsert);
         }

         if (!string.IsNullOrEmpty(connectedAttrVal))
         {
            cEdAttrV.Add(connectingAttrVal);
            cEdAttrVBS.Add(OracleParameterStatus.Success);
         }
         else
         {
            cEdAttrV.Add(string.Empty);
            cEdAttrVBS.Add(OracleParameterStatus.NullInsert);
         }

         if (!string.IsNullOrEmpty(connectionAttr))
         {
            cAttrN.Add(connectionAttr);
            cAttrNBS.Add(OracleParameterStatus.Success);
         }
         else
         {
            cAttrN.Add(string.Empty);
            cAttrNBS.Add(OracleParameterStatus.NullInsert);
         }

         if (!string.IsNullOrEmpty(connectionAttrVal))
         {
            cAttrV.Add(connectionAttrVal);
            cAttrVBS.Add(OracleParameterStatus.Success);
         }
         else
         {
            cAttrV.Add(string.Empty);
            cAttrVBS.Add(OracleParameterStatus.NullInsert);
         }

         if (!string.IsNullOrEmpty(realizingElId))
         {
            realEl.Add(realizingElId);
            realElBS.Add(OracleParameterStatus.Success);
         }
         else
         {
            realEl.Add(string.Empty);
            realElBS.Add(OracleParameterStatus.NullInsert);
         }

         if (!string.IsNullOrEmpty(realizingElType))
         {
            realElTyp.Add(realizingElType);
            realElTBS.Add(OracleParameterStatus.Success);
         }
         else
         {
            realElTyp.Add(string.Empty);
            realElTBS.Add(OracleParameterStatus.NullInsert);
         }

         relTyp.Add(relationshipType);
      }
#endif
#if POSTGRES
      void insRelConnect(NpgsqlCommand command, string connectingId, string connectingType, string connectingAttr, string connectingAttrVal,
                           string connectedId, string connectedType, string connectedAttr, string connectedAttrVal,
                           string connectionAttr, string connectionAttrVal, string realizingElId, string realizingElType, string relationshipType)
      {
         command.Parameters["@ieid"].Value = connectingId;
         command.Parameters["@ietyp"].Value = connectingType;

         if (string.IsNullOrEmpty(connectingAttr))
            command.Parameters["@iattrn"].Value = DBNull.Value;
         else
            command.Parameters["@iattrn"].Value = connectingAttr;

         if (string.IsNullOrEmpty(connectingAttrVal))
            command.Parameters["@iattrv"].Value = DBNull.Value;
         else
            command.Parameters["@iattrv"].Value = connectingAttrVal;

         command.Parameters["@deid"].Value = connectedId;
         command.Parameters["@detyp"].Value = connectedType;

         if (string.IsNullOrEmpty(connectedAttr))
            command.Parameters["@dattrn"].Value = DBNull.Value;
         else
            command.Parameters["@dattrn"].Value = connectedAttr;

         if (string.IsNullOrEmpty(connectedAttrVal))
            command.Parameters["@dattrv"].Value =  DBNull.Value;
         else
            command.Parameters["@dattrv"].Value = connectedAttrVal;

         if (string.IsNullOrEmpty(connectionAttr))
            command.Parameters["@cattrn"].Value = DBNull.Value;
         else
            command.Parameters["@cattrn"].Value = connectionAttr;

         if (string.IsNullOrEmpty(connectionAttrVal))
            command.Parameters["@cattrv"].Value = DBNull.Value;
         else
            command.Parameters["@cattrv"].Value = connectionAttrVal;

         if (string.IsNullOrEmpty(realizingElId))
            command.Parameters["@reid"].Value = DBNull.Value;
         else
            command.Parameters["@reid"].Value = realizingElId;

         if (string.IsNullOrEmpty(realizingElType))
            command.Parameters["@retyp"].Value = DBNull.Value;
         else
            command.Parameters["@retyp"].Value = realizingElType;
         command.Parameters["@rtyp"].Value = relationshipType;

         try
         {
            DBOperation.CurrTransaction.Save(DBOperation.def_savepoint);
            command.ExecuteNonQuery();
            DBOperation.CurrTransaction.Release(DBOperation.def_savepoint);
         }
         catch (NpgsqlException e)
         {
            string excStr = "%%Insert Error - " + e.Message + "\n\t" + command.CommandText;
            _refBIMRLCommon.StackPushIgnorableError(excStr);
            // Ignore any error
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

      private void processRelContainedInSpatialStructure()
      {
         string sqlStmt;
         DBOperation.beginTransaction();

         int commandStatus = -1;
         int recCount = 0;

#if ORACLE
         sqlStmt = "update " + DBOperation.formatTabName("BIMRL_ELEMENT") + " set container=:cont where elementid=:eid";
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);

         OracleParameter[] Param = new OracleParameter[2];
         Param[0] = command.Parameters.Add("cont", OracleDbType.Varchar2);
         Param[0].Direction = ParameterDirection.Input;
         Param[1] = command.Parameters.Add("eid", OracleDbType.Varchar2);
         Param[1].Direction = ParameterDirection.Input;

#endif
#if POSTGRES
         sqlStmt = "update " + DBOperation.formatTabName("BIMRL_ELEMENT") + " set container=@cont where elementid=@eid";
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);

         command.Parameters.Add("@cont", NpgsqlDbType.Varchar);
         command.Parameters.Add("@eid", NpgsqlDbType.Varchar);

#endif
         IEnumerable<IIfcRelContainedInSpatialStructure> rels = _model.Instances.OfType<IIfcRelContainedInSpatialStructure>();
         foreach (IIfcRelContainedInSpatialStructure relCont in rels)
         {
            string contGuid = relCont.RelatingStructure.GlobalId.ToString();
            if (_refBIMRLCommon.getLineNoFromMapping(contGuid) == null)
               continue;       // skip "non" element guid in the relationship object

            foreach (IIfcProduct prod in relCont.RelatedElements)
            {
#if ORACLE
               string prodGuid = prod.GlobalId.ToString();
               if (_refBIMRLCommon.getLineNoFromMapping(prodGuid) == null)
                  continue;       // skip "non" element guid in the relationship object

               Param[0].Value = contGuid;
               Param[1].Value = prodGuid;
               recCount++;
               try
               {
                  commandStatus = command.ExecuteNonQuery();
                  if (recCount >= DBOperation.commitInterval)
                  {
                     DBOperation.commitTransaction();
                     recCount = 0;
                  }
               }
               catch (OracleException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + command.CommandText;
                  _refBIMRLCommon.StackPushIgnorableError(excStr);
                  // Ignore any error
                  continue;
               }
               catch (SystemException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + command.CommandText;
                  _refBIMRLCommon.StackPushError(excStr);
                  throw;
               }
#endif
#if POSTGRES
               string prodGuid = prod.GlobalId.ToString();
               if (_refBIMRLCommon.getLineNoFromMapping(prodGuid) == null)
                  continue;       // skip "non" element guid in the relationship object

               command.Parameters["@cont"].Value = contGuid;
               command.Parameters["@eid"].Value = prodGuid;
               recCount++;
               try
               {
                  DBOperation.CurrTransaction.Save(DBOperation.def_savepoint);
                  commandStatus = command.ExecuteNonQuery();
                  DBOperation.CurrTransaction.Release(DBOperation.def_savepoint);
                  if (recCount >= DBOperation.commitInterval)
                  {
                     DBOperation.commitTransaction();
                     recCount = 0;
                  }
               }
               catch (NpgsqlException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + command.CommandText;
                  _refBIMRLCommon.StackPushIgnorableError(excStr);
                  DBOperation.CurrTransaction.Rollback(DBOperation.def_savepoint);
                  continue;
               }
               catch (SystemException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + command.CommandText;
                  _refBIMRLCommon.StackPushError(excStr);
                  throw;
               }
#endif
            }
         }

         DBOperation.commitTransaction();
         command.Dispose();
      }

   }
}
