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
   public class BIMRLMaterial
   {
      private IfcStore _model;
      private BIMRLCommon _refBIMRLCommon;

      public BIMRLMaterial(IfcStore m, BIMRLCommon refBIMRLCommon)
      {
         _model = m;
         _refBIMRLCommon = refBIMRLCommon;
      }

      public void processMaterials()
      {
#if ORACLE
         List<string> insTGuid = new List<string>();
         List<string> insTMatName = new List<string>();
         List<string> insTSetName = new List<string>();
         List<OracleParameterStatus> insTSetNPS = new List<OracleParameterStatus>();
         List<int> insTMatSeq = new List<int>();
         List<OracleParameterStatus> insTMatSPS = new List<OracleParameterStatus>();
         List<double> insTMatThick = new List<double>();
         List<OracleParameterStatus> insTMatTPS = new List<OracleParameterStatus>();
         List<string> insTIsVentilated = new List<string>();
         List<OracleParameterStatus> insTIsVPS = new List<OracleParameterStatus>();
         List<string> insTCategory = new List<string>();
         List<OracleParameterStatus> insTCatPS = new List<OracleParameterStatus>();
         List<string> insTForProfile = new List<string>();
         List<OracleParameterStatus> insTForPPS = new List<OracleParameterStatus>();

         List<string> insGuid = new List<string>();
         List<string> insMatName = new List<string>();
         List<string> insSetName = new List<string>();
         List<OracleParameterStatus> insSetNPS = new List<OracleParameterStatus>();
         List<int> insMatSeq = new List<int>();
         List<OracleParameterStatus> insMatSPS = new List<OracleParameterStatus>();
         List<double> insMatThick = new List<double>();
         List<OracleParameterStatus> insMatTPS = new List<OracleParameterStatus>();
         List<string> insIsVentilated = new List<string>();
         List<OracleParameterStatus> insIsVPS = new List<OracleParameterStatus>();
         List<string> insCategory = new List<string>();
         List<OracleParameterStatus> insCatPS = new List<OracleParameterStatus>();
         List<string> insForProfile = new List<string>();
         List<OracleParameterStatus> insForPPS = new List<OracleParameterStatus>();

         string sqlStmt = "insert into " + DBOperation.formatTabName("BIMRL_TYPEMATERIAL") + " (ElementID, MaterialName, Category, SetName, IsVentilated, forprofile, MaterialSequence, MaterialThickness) "
            + " values (:1, :2, :3, :4, :5, :6, :7, :8)";

         string sqlStmt2 = "insert into " + DBOperation.formatTabName("BIMRL_ELEMENTMATERIAL") + " (ElementID, MaterialName, Category, SetName, IsVentilated, forprofile, MaterialSequence, MaterialThickness) "
                  + " values (:1, :2, :3, :4, :5, :6, :7, :8)";

         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);
         OracleCommand command2 = new OracleCommand(sqlStmt2, DBOperation.DBConn);

         OracleParameter[] Param = new OracleParameter[8];
         for (int i = 0; i < 6; i++)
         {
               Param[i] = command.Parameters.Add(i.ToString(), OracleDbType.Varchar2);
               Param[i].Direction = ParameterDirection.Input;
         }
         Param[6] = command.Parameters.Add("3", OracleDbType.Int16);
         Param[6].Direction = ParameterDirection.Input;
         Param[7] = command.Parameters.Add("4", OracleDbType.Double);
         Param[7].Direction = ParameterDirection.Input;

         OracleParameter[] Param2 = new OracleParameter[8];
         for (int i = 0; i < 6; i++)
         {
               Param2[i] = command2.Parameters.Add(i.ToString(), OracleDbType.Varchar2);
               Param2[i].Direction = ParameterDirection.Input;
         }
         Param2[6] = command2.Parameters.Add("3", OracleDbType.Int16);
         Param2[6].Direction = ParameterDirection.Input;
         Param2[7] = command2.Parameters.Add("4", OracleDbType.Double);
         Param2[7].Direction = ParameterDirection.Input;
#endif
#if POSTGRES
         string sqlStmt = "insert into " + DBOperation.formatTabName("BIMRL_TYPEMATERIAL") + " (ElementID, MaterialName, Category, SetName, IsVentilated, forprofile, MaterialSequence, MaterialThickness) "
            + " values (@0, @1, @2, @3, @4, @5, @6, @7)";

         string sqlStmt2 = "insert into " + DBOperation.formatTabName("BIMRL_ELEMENTMATERIAL") + " (ElementID, MaterialName, Category, SetName, IsVentilated, forprofile, MaterialSequence, MaterialThickness) "
            + " values (@0, @1, @2, @3, @4, @5, @6, @7)";

         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
         NpgsqlCommand command2 = new NpgsqlCommand(sqlStmt2, DBOperation.DBConn);

         command.Prepare();
         command2.Prepare();
#endif
         string currStep = "Processing Materials";

         DBOperation.beginTransaction();

         IEnumerable<IIfcRelAssociatesMaterial> relMaterials = _model.Instances.OfType<IIfcRelAssociatesMaterial>();
         foreach (IIfcRelAssociatesMaterial relMat in relMaterials)
         {
            // reset Relating material data at the start
            List<string> arrMatName = new List<string>();
            List<string> arrCategory = new List<string>();
            List<string> arrSetName = new List<string>();
            List<string> arrIsVentilated = new List<string>();
            List<string> arrForProfile = new List<string>();
            List<int> arrMatSeq = new List<int>();
            List<double> arrMatThick = new List<double>();

            // Handle various IfcMaterialSelect
            if (relMat.RelatingMaterial is IIfcMaterial)
            {
               IIfcMaterial m = relMat.RelatingMaterial as IIfcMaterial;
               arrMatName.Add(m.Name);
               arrSetName.Add(string.Empty);
               arrMatSeq.Add(-1);
               arrMatThick.Add(-1.0);
               arrIsVentilated.Add(string.Empty);
               if (m.Category.HasValue)
                  arrCategory.Add(m.Category);
               else
                  arrCategory.Add(string.Empty);
               arrForProfile.Add(string.Empty);
            }
            else if (relMat.RelatingMaterial is IIfcMaterialConstituent)
            {
               IIfcMaterialConstituent m = relMat.RelatingMaterial as IIfcMaterialConstituent;
               arrMatName.Add(m.Material.Name);
               arrSetName.Add(string.Empty);
               arrMatSeq.Add(-1);
               arrMatThick.Add(-1.0);
               arrIsVentilated.Add(string.Empty);
               if (m.Material.Category.HasValue)
                  arrCategory.Add(m.Material.Category);
               else
                  arrCategory.Add(string.Empty);

               arrForProfile.Add(string.Empty);
            }
            else if (relMat.RelatingMaterial is IIfcMaterialList)
            {
               IIfcMaterialList mList = relMat.RelatingMaterial as IIfcMaterialList;
               foreach (IIfcMaterial m in mList.Materials)
               {
                  arrMatName.Add(m.Name);
                  arrSetName.Add(string.Empty);
                  arrMatSeq.Add(-1);
                  arrMatThick.Add(-1.0);
                  arrIsVentilated.Add(string.Empty);
                  if (m.Category.HasValue)
                     arrCategory.Add(m.Category);
                  else
                     arrCategory.Add(string.Empty);
                  arrForProfile.Add(string.Empty);
               }
            }
            else if (relMat.RelatingMaterial is IIfcMaterialConstituentSet)
            {
               IIfcMaterialConstituentSet mConstSet = relMat.RelatingMaterial as IIfcMaterialConstituentSet;
               foreach (IIfcMaterialConstituent mConst in mConstSet.MaterialConstituents)
               {
                  arrMatName.Add(mConst.Material.Name);
                  arrSetName.Add(mConstSet.Name);
                  arrMatSeq.Add(-1);
                  arrMatThick.Add(-1.0);
                  arrIsVentilated.Add(string.Empty);
                  arrCategory.Add(mConst.Material.Category);
                  if (mConst.Material.Category.HasValue)
                     arrCategory.Add(mConst.Material.Category);
                  else
                     arrCategory.Add(string.Empty);
                  arrForProfile.Add(string.Empty);
               }
            }
            else if (relMat.RelatingMaterial is IIfcMaterialLayer)
            {
               IIfcMaterialLayer mLayer = relMat.RelatingMaterial as IIfcMaterialLayer;
               if (mLayer.Material != null)
                  arrMatName.Add(mLayer.Material.Name);
               else
                  arrMatName.Add("-");
               arrSetName.Add(string.Empty);
               arrMatSeq.Add(-1);
               arrMatThick.Add((double) mLayer.LayerThickness.Value * _model.ModelFactors.LengthToMetresConversionFactor);
               if (mLayer.IsVentilated != null)
               {
                  arrIsVentilated.Add("TRUE");
               }
               else
               {
                  arrIsVentilated.Add(string.Empty);
               }
               if (mLayer.Category.HasValue)
                  arrCategory.Add(mLayer.Category.Value);
               else
                  arrCategory.Add(string.Empty);
               arrForProfile.Add(string.Empty);
            }
            else if (relMat.RelatingMaterial is IIfcMaterialLayerSet || relMat.RelatingMaterial is IIfcMaterialLayerSetUsage)
            {
               IIfcMaterialLayerSet mLayerSet;
               if (relMat.RelatingMaterial is IIfcMaterialLayerSetUsage)
               {
                  // We do not handle LayerSetDirection, DirectionSense, OffserFromReference as they are mainly important for drawing construction
                  IIfcMaterialLayerSetUsage mLSU = relMat.RelatingMaterial as IIfcMaterialLayerSetUsage;
                  mLayerSet = mLSU.ForLayerSet;
               }
               else
                  mLayerSet = relMat.RelatingMaterial as IIfcMaterialLayerSet;

               Int16 seqNo = 1;
               foreach (IIfcMaterialLayer mLayer in mLayerSet.MaterialLayers)
               {
                  if (mLayerSet.LayerSetName != null)
                  {
                        arrSetName.Add(mLayerSet.LayerSetName);
                  }
                  else
                  {
                        arrSetName.Add(string.Empty);
                  }

                  if (mLayer.Material != null)
                        arrMatName.Add(mLayer.Material.Name);
                  else
                        arrMatName.Add("-");
                  arrMatSeq.Add(seqNo++);
                  arrMatThick.Add((double) mLayer.LayerThickness.Value * _model.ModelFactors.LengthToMetresConversionFactor);
                  if (mLayer.IsVentilated != null)
                  {
                        arrIsVentilated.Add("TRUE");
                  }
                  else
                  {
                        arrIsVentilated.Add(string.Empty);
                  }
                  if (mLayer.Category.HasValue)
                     arrCategory.Add(mLayer.Category.Value);
                  else
                     arrCategory.Add(string.Empty);
                  arrForProfile.Add(string.Empty);
               }
            }
            else if (relMat.RelatingMaterial is IIfcMaterialProfile)
            {
               IIfcMaterialProfile mProfile = relMat.RelatingMaterial as IIfcMaterialProfile;
               if (mProfile.Material != null)
               {
                  if (mProfile.Category.HasValue)
                     arrCategory.Add(mProfile.Category);
                  else
                     arrCategory.Add(string.Empty);
               }
               else
               {
                  arrMatName.Add("-");
                  arrCategory.Add(string.Empty);
               }

               if (mProfile.Profile.ProfileName.HasValue)
                  arrForProfile.Add(mProfile.Profile.ProfileName.ToString());
               else
                  arrForProfile.Add(string.Empty);

               arrSetName.Add(string.Empty);
               arrMatSeq.Add(-1);
               arrMatThick.Add(-1.0);
               arrIsVentilated.Add(string.Empty);
            }
            else if (relMat.RelatingMaterial is IIfcMaterialProfileSet
                     || relMat.RelatingMaterial is IIfcMaterialProfileSetUsage
                     || relMat.RelatingMaterial is IIfcMaterialProfileSetUsageTapering)
            {
               IIfcMaterialProfileSet mProfileSet;
               IIfcMaterialProfileSet mProfileSetEnd = null;
               if (relMat.RelatingMaterial is IIfcMaterialProfileSetUsage)
               {
                  // We do not handle other information, except the material name and the profile name
                  IIfcMaterialProfileSetUsage mPSU = relMat.RelatingMaterial as IIfcMaterialProfileSetUsage;
                  mProfileSet = mPSU.ForProfileSet;

                  if (relMat.RelatingMaterial is IIfcMaterialProfileSetUsageTapering)
                     mProfileSetEnd = (relMat.RelatingMaterial as IIfcMaterialProfileSetUsageTapering).ForProfileEndSet;
               }
               else
                  mProfileSet = relMat.RelatingMaterial as IIfcMaterialProfileSet;

               string material;
               string category;
               string forProfile;
               getMaterialProfileSetString(mProfileSet, out material, out category, out forProfile);
               if (mProfileSetEnd != null)
               {
                  string endMaterial;
                  string endCategory;
                  string endProfile;
                  getMaterialProfileSetString(mProfileSetEnd, out endMaterial, out endCategory, out endProfile);

                  BIMRLCommon.appendToString(endMaterial, " | ", ref material);
                  BIMRLCommon.appendToString(endCategory, " | ", ref category);
                  BIMRLCommon.appendToString(endProfile, " | ", ref forProfile);
               }

               Int16 seqNo = 1;
               foreach (IIfcMaterialProfile mProf in mProfileSet.MaterialProfiles)
               {
                  if (mProfileSet.Name != null)
                  {
                     arrSetName.Add(mProfileSet.Name);
                  }
                  else
                  {
                     arrSetName.Add(string.Empty);
                  }

                  arrMatName.Add(material);
                  arrCategory.Add(category);
                  arrForProfile.Add(forProfile);
                  
                  arrMatSeq.Add(seqNo++);
                  arrMatThick.Add(-1.0);
                  arrIsVentilated.Add(string.Empty);
               }
            }
            else
            {
               // Not supported type
            }

            IEnumerable<IIfcDefinitionSelect> relObjects = relMat.RelatedObjects;
            foreach (IIfcDefinitionSelect relObjSel in relObjects)
            {
               IIfcObjectDefinition relObj = relObjSel as IIfcObjectDefinition;
               if (!(relObj is IIfcProduct) && !(relObj is IIfcTypeProduct))
                     continue;

               string guid = relObj.GlobalId.ToString();

               for (int i = 0; i < arrMatName.Count; i++)
               {
                  if (relObj is IIfcProduct)
                  {
#if ORACLE
                     insertMaterial(ref insGuid, guid, ref insMatName, arrMatName[i], 
                                    ref insCategory, ref insCatPS, arrCategory[i],
                                    ref insSetName, ref insSetNPS, arrSetName[i],
                                    ref insIsVentilated, ref insIsVPS, arrIsVentilated[i],
                                    ref insForProfile, ref insForPPS, arrForProfile[i],
                                    ref insMatSeq, ref insMatSPS, arrMatSeq[i],
                                    ref insMatThick, ref insMatTPS, arrMatThick[i]);
#endif
#if POSTGRES
                     insertMaterial(command, guid, arrMatName[i], arrCategory[i], arrSetName[i], arrIsVentilated[i], arrForProfile[i], arrMatSeq[i], arrMatThick[i]);
#endif
                  }
                  else
                  {
#if ORACLE
                     insertMaterial(ref insTGuid, guid, ref insTMatName, arrMatName[i], 
                                    ref insTCategory, ref insTCatPS, arrCategory[i],
                                    ref insTSetName, ref insTSetNPS, arrSetName[i],
                                    ref insTIsVentilated, ref insTIsVPS, arrIsVentilated[i],
                                    ref insTForProfile, ref insTForPPS, arrForProfile[i],
                                    ref insTMatSeq, ref insTMatSPS, arrMatSeq[i],
                                    ref insTMatThick, ref insTMatTPS, arrMatThick[i]);
#endif
#if POSTGRES
                     insertMaterial(command2, guid, arrMatName[i], arrCategory[i], arrSetName[i], arrIsVentilated[i], arrForProfile[i], arrMatSeq[i], arrMatThick[i]);
#endif
                  }
               }
            }

#if ORACLE
            if ((insGuid.Count + insTGuid.Count) >= DBOperation.commitInterval)
            {
               int commandStatus;
               try
               {
                  if (insTGuid.Count > 0)
                  {
                        currStep = "Processing Type Materials";
                        Param[0].Value = insTGuid.ToArray();
                        Param[1].Value = insTMatName.ToArray();
                        Param[2].Value = insTSetName.ToArray();
                        Param[2].ArrayBindStatus = insTSetNPS.ToArray();
                        Param[3].Value = insTIsVentilated.ToArray();
                        Param[3].ArrayBindStatus = insTIsVPS.ToArray();
                        Param[4].Value = insTMatSeq.ToArray();
                        Param[4].ArrayBindStatus = insTMatSPS.ToArray();
                        Param[5].Value = insTMatThick.ToArray();
                        Param[5].ArrayBindStatus = insTMatTPS.ToArray();
                        for (int i = 0; i < 6; i++)
                           Param[i].Size = insTGuid.Count;
                        command.ArrayBindCount = insTGuid.Count;

                        commandStatus = command.ExecuteNonQuery();
                  }

                  if (insGuid.Count > 0)
                  {
                        currStep = "Processing Element Materials";

                        Param2[0].Value = insGuid.ToArray();
                        Param2[1].Value = insMatName.ToArray();
                        Param2[2].Value = insSetName.ToArray();
                        Param2[2].ArrayBindStatus = insSetNPS.ToArray();
                        Param2[3].Value = insIsVentilated.ToArray();
                        Param2[3].ArrayBindStatus = insIsVPS.ToArray();
                        Param2[4].Value = insMatSeq.ToArray();
                        Param2[4].ArrayBindStatus = insMatSPS.ToArray();
                        Param2[5].Value = insMatThick.ToArray();
                        Param2[5].ArrayBindStatus = insMatTPS.ToArray();
                        for (int i = 0; i < 6; i++)
                           Param2[i].Size = insGuid.Count;
                        command2.ArrayBindCount = insGuid.Count;

                        commandStatus = command2.ExecuteNonQuery();
                  }

                  DBOperation.commitTransaction();
                        
                  insTGuid.Clear();
                  insTMatName.Clear();
                  insTSetName.Clear();
                  insTSetNPS.Clear();
                  insTIsVentilated.Clear();
                  insTIsVPS.Clear();
                  insTMatSeq.Clear();
                  insTMatSPS.Clear();
                  insTMatThick.Clear();
                  insTMatTPS.Clear();

                  insGuid.Clear();
                  insMatName.Clear();
                  insSetName.Clear();
                  insSetNPS.Clear();
                  insIsVentilated.Clear();
                  insIsVPS.Clear();
                  insMatSeq.Clear();
                  insMatSPS.Clear();
                  insMatThick.Clear();
                  insMatTPS.Clear();
               }
               catch (OracleException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
                  _refBIMRLCommon.StackPushIgnorableError(excStr);

                  arrMatName.Clear();
                  arrSetName.Clear();
                  arrIsVentilated.Clear();
                  arrMatSeq.Clear();
                  arrMatThick.Clear();

                  insTGuid.Clear();
                  insTMatName.Clear();
                  insTSetName.Clear();
                  insTSetNPS.Clear();
                  insTIsVentilated.Clear();
                  insTIsVPS.Clear();
                  insTMatSeq.Clear();
                  insTMatSPS.Clear();
                  insTMatThick.Clear();
                  insTMatTPS.Clear();

                  insGuid.Clear();
                  insMatName.Clear();
                  insSetName.Clear();
                  insSetNPS.Clear();
                  insIsVentilated.Clear();
                  insIsVPS.Clear();
                  insMatSeq.Clear();
                  insMatSPS.Clear();
                  insMatThick.Clear();
                  insMatTPS.Clear();

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
         if ((insGuid.Count + insTGuid.Count) > 0)
         {
               int commandStatus;
               try
               {
                  if (insTGuid.Count > 0)
                  {
                     currStep = "Processing Type Materials";
                     Param[0].Value = insTGuid.ToArray();
                     Param[1].Value = insTMatName.ToArray();
                     Param[2].Value = insTSetName.ToArray();
                     Param[2].ArrayBindStatus = insTSetNPS.ToArray();
                     Param[3].Value = insTIsVentilated.ToArray();
                     Param[3].ArrayBindStatus = insTIsVPS.ToArray();
                     Param[4].Value = insTMatSeq.ToArray();
                     Param[4].ArrayBindStatus = insTMatSPS.ToArray();
                     Param[5].Value = insTMatThick.ToArray();
                     Param[5].ArrayBindStatus = insTMatTPS.ToArray();
                     for (int i = 0; i < 6; i++)
                           Param[i].Size = insTGuid.Count;
                     command.ArrayBindCount = insTGuid.Count;

                     commandStatus = command.ExecuteNonQuery();
                  }

                  if (insGuid.Count > 0)
                  {
                     currStep = "Processing Element Materials";

                     Param2[0].Value = insGuid.ToArray();
                     Param2[1].Value = insMatName.ToArray();
                     Param2[2].Value = insSetName.ToArray();
                     Param2[2].ArrayBindStatus = insSetNPS.ToArray();
                     Param2[3].Value = insIsVentilated.ToArray();
                     Param2[3].ArrayBindStatus = insIsVPS.ToArray();
                     Param2[4].Value = insMatSeq.ToArray();
                     Param2[4].ArrayBindStatus = insMatSPS.ToArray();
                     Param2[5].Value = insMatThick.ToArray();
                     Param2[5].ArrayBindStatus = insMatTPS.ToArray();
                     for (int i = 0; i < 6; i++)
                           Param2[i].Size = insGuid.Count;
                     command2.ArrayBindCount = insGuid.Count;

                     commandStatus = command2.ExecuteNonQuery();
                  }

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
         command2.Dispose();
      }

      void getMaterialProfileSetString(IIfcMaterialProfileSet mProfileSet, out string materialName, out string category, out string profileName)
      {
         materialName = string.Empty;
         category = string.Empty;
         profileName = string.Empty;

         foreach (IIfcMaterialProfile mProfile in mProfileSet.MaterialProfiles)
         {
            if (mProfile.Material != null)
            {
               BIMRLCommon.appendToString(mProfile.Material.Name, ", ", ref materialName);
               if (mProfile.Category.HasValue)
                  BIMRLCommon.appendToString(mProfile.Category.Value, ", ", ref category);
               else
                  BIMRLCommon.appendToString("-", ", ", ref category);
            }
            else
            {
               BIMRLCommon.appendToString("-", ", ", ref materialName);
               BIMRLCommon.appendToString("-", ", ", ref category);
            }

            if (mProfile.Profile.ProfileName.HasValue)
               BIMRLCommon.appendToString(mProfile.Profile.ProfileName.Value, ", ", ref profileName);
            else
               BIMRLCommon.appendToString("-", ", ", ref profileName);
         }

         materialName = "(" + materialName + ")";
         category = "(" + category + ")";
         profileName = "(" + profileName + ")";
      }

#if ORACLE
      private void insertMaterial(ref List<string> insGUID, string elementid, ref List<string> insMatName, string materialName, 
                                 ref List<string> insCategory, ref List<OracleParameterStatus> insCatPS, string category,
                                 ref List<string> insSetName, ref List<OracleParameterStatus> insSetNPS, string setName,
                                 ref List<string> insIsVentilated, ref List<OracleParameterStatus> insIsVPS, string isVentilated,
                                 ref List<string> insForProfile, ref List<OracleParameterStatus> insForPPS, string forProfile,
                                 ref List<int> insMatSeq, ref List<OracleParameterStatus> insMatSPS, int materialSequence,
                                 ref List<double> insMatThick, ref List<OracleParameterStatus> insMatTPS, double materialThickness)

      {
         insGUID.Add(elementid);
         insMatName.Add(materialName);

         insCategory.Add(category);
         if (string.IsNullOrEmpty(category))
            insCatPS.Add(OracleParameterStatus.NullInsert);
         else
            insCatPS.Add(OracleParameterStatus.Success);
      
         insSetName.Add(setName);
         if (string.IsNullOrEmpty(setName))
            insSetNPS.Add(OracleParameterStatus.NullInsert);
         else
            insSetNPS.Add(OracleParameterStatus.Success);

         insIsVentilated.Add(isVentilated);
         if (string.IsNullOrEmpty(isVentilated))
            insIsVPS.Add(OracleParameterStatus.NullInsert);
         else
            insIsVPS.Add(OracleParameterStatus.Success);

         insForProfile.Add(forProfile);
         if (string.IsNullOrEmpty(forProfile))
            insForPPS.Add(OracleParameterStatus.NullInsert);
         else
            insForPPS.Add(OracleParameterStatus.Success);
      
         if (materialSequence >= 0)
         {
            insMatSeq.Add(materialSequence);
            insMatSPS.Add(OracleParameterStatus.Success);
         }
         else
         {  insMatSeq.Add(0);
            insMatSPS.Add(OracleParameterStatus.NullInsert);
         }

         if (materialThickness >= 0)
         {
            insMatThick.Add(materialThickness);
            insMatTPS.Add(OracleParameterStatus.Success);
         }
         else
         {  insMatThick.Add(0.0);
            insMatTPS.Add(OracleParameterStatus.NullInsert);
         }
      }
#endif
#if POSTGRES
      private void insertMaterial(NpgsqlCommand command, string elementid, string materialName, string category, string setname, string isVentilated, string forProfile,
         int materialSequence, double materialThickness)
      {
         command.Parameters.Clear();
         command.Parameters.AddWithValue("0", elementid);
         command.Parameters.AddWithValue("1", materialName);
         command.Parameters.AddWithValue("2", category);
         command.Parameters.AddWithValue("3", setname);
         command.Parameters.AddWithValue("4", isVentilated);
         command.Parameters.AddWithValue("5", forProfile);

         if (materialSequence >= 0)
            command.Parameters.AddWithValue("6", materialSequence);
         else
            command.Parameters.AddWithValue("6", DBNull.Value);

         if (materialThickness >= 0)
            command.Parameters.AddWithValue("7", materialThickness);
         else
            command.Parameters.AddWithValue("7", DBNull.Value);

         try
         {
            int commandStatus = command.ExecuteNonQuery();
         }
         catch (NpgsqlException e)
         {
            // Ignore error and continue
            _refBIMRLCommon.StackPushIgnorableError(string.Format("Error inserting (\"{0}\",\"{1}\",\"{2}\",\"{3}\",{4},{5},\"{6}\",\"{7}\"); {8})", elementid, materialName, category,
               setname, materialSequence.ToString(), materialThickness.ToString(), isVentilated, forProfile, e.Message));
         }
      }
#endif

   }
}
