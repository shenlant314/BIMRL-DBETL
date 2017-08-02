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
using Xbim.Common.Exceptions;
using System.Diagnostics;
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
using Xbim.Ifc4.Interfaces;
using Xbim.Ifc;
using Xbim.Common;

namespace BIMRL
{
    public class BIMRLProperties
    {
        BIMRLCommon _refBIMRLCommon;

        public BIMRLProperties(BIMRLCommon refBIMRLCommon)
        {
            _refBIMRLCommon = refBIMRLCommon;
        }

        public void processTypeProperties(IIfcTypeProduct typ)
        {
         IList<IIfcPropertySet> pSets = new List<IIfcPropertySet>();
         IList<IIfcPropertySetDefinition> psetDefs = new List<IIfcPropertySetDefinition>();
         foreach (IIfcPropertySetDefinition pDefSel in typ.HasPropertySets)
         {
            // IFC2x3:
            Xbim.Ifc2x3.Kernel.IfcPropertySetDefinition pDefSel2x3 = pDefSel as Xbim.Ifc2x3.Kernel.IfcPropertySetDefinition;
            if (pDefSel2x3 == null)
            {
               if (pDefSel is IIfcPreDefinedPropertySet || pDefSel is IIfcQuantitySet)
                  psetDefs.Add(pDefSel as IIfcPropertySetDefinition);
               if (pDefSel is IIfcPropertySet)
                  pSets.Add(pDefSel as IIfcPropertySet);
            }
            else
            {
               if (pDefSel2x3 is IIfcPropertySet)
                  pSets.Add(pDefSel2x3 as IIfcPropertySet);
               else if (pDefSel2x3 is IIfcDoorLiningProperties || pDefSel2x3 is IIfcDoorPanelProperties
                         || pDefSel2x3 is IIfcWindowLiningProperties || pDefSel2x3 is IIfcWindowPanelProperties
                         || pDefSel2x3 is IIfcElementQuantity)
                  psetDefs.Add(pDefSel2x3 as IIfcPropertySetDefinition);
            }
         }

         processProperties(typ.GlobalId.ToString(), pSets, "BIMRL_TYPEPROPERTIES");
         if (psetDefs.Count > 0)
            processPropertyDefinitions(typ.GlobalId.ToString(), psetDefs, "BIMRL_TYPEPROPERTIES");
      }

      public void processAllElemProperties(IfcStore model)
      {
         // Now process Property set definitions attachd to the object via IsDefinedByProperties for special properties
         IEnumerable<IIfcRelDefinesByProperties> relDProps = model.Instances.OfType<IIfcRelDefinesByProperties>();

         foreach (IIfcRelDefinesByProperties relDProp in relDProps)
         {
            IList<IIfcPropertySet> pSets = new List<IIfcPropertySet>();
            IList<IIfcPropertySetDefinition> psetDefs = new List<IIfcPropertySetDefinition>();

            IIfcPropertySetDefinitionSelect pDefSel = relDProp.RelatingPropertyDefinition;
            // IFC2x3:
            Xbim.Ifc2x3.Interfaces.IIfcPropertySetDefinition pDefSel2x3 = pDefSel as Xbim.Ifc2x3.Interfaces.IIfcPropertySetDefinition;
            if (pDefSel2x3 == null)
            {
               if (pDefSel is IIfcPreDefinedPropertySet || pDefSel is IIfcQuantitySet)
                  psetDefs.Add(pDefSel as IIfcPropertySetDefinition);
               if (pDefSel is IIfcPropertySet)
                  pSets.Add(pDefSel as IIfcPropertySet);
            }
            else
            {
               if (pDefSel2x3 is IIfcPropertySet)
                  pSets.Add(pDefSel2x3 as IIfcPropertySet);
               else if (pDefSel2x3 is IIfcDoorLiningProperties || pDefSel2x3 is IIfcDoorPanelProperties
                           || pDefSel2x3 is IIfcWindowLiningProperties || pDefSel2x3 is IIfcWindowPanelProperties
                           || pDefSel2x3 is IIfcElementQuantity)
                  psetDefs.Add(pDefSel2x3 as IIfcPropertySetDefinition);
            }
            foreach (IIfcObjectDefinition objDef in relDProp.RelatedObjects)
            {
               // We need to process only properties for the objects (not the types. Types will be handled when processing Types)
               if (!(objDef is IIfcObject))
                  continue;

               processProperties(objDef.GlobalId.ToString(), pSets, "BIMRL_ELEMENTPROPERTIES");

               if (psetDefs.Count > 0)
                  processPropertyDefinitions(objDef.GlobalId.ToString(), psetDefs, "BIMRL_ELEMENTPROPERTIES");
            }
         }
      }

      private void processPropertyDefinitions(string guid, IEnumerable<IIfcPropertySetDefinition> psdefs, string tableName)
      {
         string sqlStmt;

#if ORACLE
         sqlStmt = "Insert into " + DBOperation.formatTabName(tableName) + "(ElementId, PropertyGroupName, PropertyName, PropertyValue, PropertyDataType"
            + ", PropertyUnit) Values (:1, :2, :3, :4, :5, :6)";
         string currStep = sqlStmt;
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);

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
#endif
#if POSTGRES
         sqlStmt = "Insert into " + DBOperation.formatTabName(tableName) + "(ElementId, PropertyGroupName, PropertyName, PropertyValue, PropertyDataType"
            + ", PropertyUnit) Values (@eid, @gname, @pname, @pvalue, @pdtyp, @punit)";
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
         command.Parameters.Add("@eid", NpgsqlDbType.Text);
         command.Parameters.Add("@gname", NpgsqlDbType.Text);
         command.Parameters.Add("@pname", NpgsqlDbType.Text);
         command.Parameters.Add("@pvalue", NpgsqlDbType.Text);
         command.Parameters.Add("@pdtyp", NpgsqlDbType.Text);
         command.Parameters.Add("@punit", NpgsqlDbType.Text);
         command.Prepare();
#endif
         foreach (IIfcPropertySetDefinition p in psdefs)
         {
            if (p is IIfcDoorLiningProperties)
            {
               IIfcDoorLiningProperties dl = p as IIfcDoorLiningProperties;

               if (dl.LiningDepth != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCDOORLININGPROPERTIES", ref arrPropName, "LININGDEPTH",
                     ref arrPropVal, ref arrPropValBS, dl.LiningDepth.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(dl.LiningDepth.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCDOORLININGPROPERTIES", "LININGDEPTH", dl.LiningDepth.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(dl.LiningDepth.GetType()));
#endif
               }
               else if (dl.LiningThickness != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCDOORLININGPROPERTIES", ref arrPropName, "LININGTHICKNESS",
                     ref arrPropVal, ref arrPropValBS, dl.LiningThickness.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(dl.LiningThickness.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCDOORLININGPROPERTIES", "LININGTHICKNESS", dl.LiningThickness.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(dl.LiningThickness.GetType()));
#endif
               }
               else if (dl.ThresholdDepth != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCDOORLININGPROPERTIES", ref arrPropName, "THRESHOLDDEPTH",
                     ref arrPropVal, ref arrPropValBS, dl.ThresholdDepth.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(dl.ThresholdDepth.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCDOORLININGPROPERTIES", "THRESHOLDDEPTH", dl.ThresholdDepth.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(dl.ThresholdDepth.GetType()));
#endif
               }
               else if (dl.ThresholdThickness != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCDOORLININGPROPERTIES", ref arrPropName, "THRESHOLDTHICKNESS",
                     ref arrPropVal, ref arrPropValBS, dl.ThresholdThickness.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(dl.ThresholdThickness.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCDOORLININGPROPERTIES", "THRESHOLDTHICKNESS", dl.ThresholdThickness.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(dl.ThresholdThickness.GetType()));
#endif
               }
               else if (dl.TransomThickness != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCDOORLININGPROPERTIES", ref arrPropName, "TRANSOMTHICKNESS",
                     ref arrPropVal, ref arrPropValBS, dl.TransomThickness.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(dl.TransomThickness.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCDOORLININGPROPERTIES", "TRANSOMTHICKNESS", dl.TransomThickness.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(dl.TransomThickness.GetType()));
#endif
               }
               else if (dl.TransomOffset != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCDOORLININGPROPERTIES", ref arrPropName, "TRANSOMOFFSET",
                     ref arrPropVal, ref arrPropValBS, dl.TransomOffset.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(dl.TransomOffset.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCDOORLININGPROPERTIES", "TRANSOMOFFSET", dl.TransomOffset.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(dl.TransomOffset.GetType()));
#endif
               }
               else if (dl.CasingThickness != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCDOORLININGPROPERTIES", ref arrPropName, "CASINGTHICKNESS",
                     ref arrPropVal, ref arrPropValBS, dl.CasingThickness.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(dl.CasingThickness.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCDOORLININGPROPERTIES", "CASINGTHICKNESS", dl.CasingThickness.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(dl.CasingThickness.GetType()));
#endif
               }
               else if (dl.CasingDepth != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCDOORLININGPROPERTIES", ref arrPropName, "CASINGDEPTH",
                     ref arrPropVal, ref arrPropValBS, dl.CasingDepth.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(dl.CasingDepth.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCDOORLININGPROPERTIES", "CASINGDEPTH", dl.CasingDepth.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(dl.CasingDepth.GetType()));
#endif
               }
               else if (dl.ShapeAspectStyle != null)
               {
                  string shapeAspectStyleStr = getShapeAspectString(dl.ShapeAspectStyle);
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCDOORLININGPROPERTIES", ref arrPropName, "SHAPEASPECTSTYLE",
                     ref arrPropVal, ref arrPropValBS, shapeAspectStyleStr, ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, 
                     string.Empty);
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCDOORLININGPROPERTIES", "SHAPEASPECTSTYLE", shapeAspectStyleStr,
                     "STRING", string.Empty);
#endif
               }
            }
            else if (p is IIfcDoorPanelProperties)
            {
               IIfcDoorPanelProperties dp = p as IIfcDoorPanelProperties;

               if (dp.PanelDepth != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCDOORPANELPROPERTIES", ref arrPropName, "PANELDEPTH",
                     ref arrPropVal, ref arrPropValBS, dp.PanelDepth.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(dp.PanelDepth.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCDOORPANELPROPERTIES", "PANELDEPTH", dp.PanelDepth.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(dp.PanelDepth.GetType()));
#endif
               }
               if (dp.PanelWidth != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCDOORPANELPROPERTIES", ref arrPropName, "PANELWIDTH",
                     ref arrPropVal, ref arrPropValBS, dp.PanelWidth.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(dp.PanelWidth.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCDOORPANELPROPERTIES", "PANELWIDTH", dp.PanelWidth.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(dp.PanelWidth.GetType()));
#endif
               }
               else if (dp.ShapeAspectStyle != null)
               {
                  string shapeAspectStyleStr = getShapeAspectString(dp.ShapeAspectStyle);
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCDOORPANELPROPERTIES", ref arrPropName, "SHAPEASPECTSTYLE",
                     ref arrPropVal, ref arrPropValBS, shapeAspectStyleStr, ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, 
                     string.Empty);
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCDOORPANELPROPERTIES", "SHAPEASPECTSTYLE", shapeAspectStyleStr,
                     "STRING", string.Empty);
#endif
               }

#if ORACLE
               insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCDOORPANELPROPERTIES", ref arrPropName, "PANELOPERATION",
                  ref arrPropVal, ref arrPropValBS, dp.PanelOperation.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, 
                  string.Empty);

               insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCDOORPANELPROPERTIES", ref arrPropName, "PANELPOSITION",
                  ref arrPropVal, ref arrPropValBS, dp.PanelPosition.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, 
                  string.Empty);
#endif
#if POSTGRES
               insertProperty(command, guid, "IFCDOORPANELPROPERTIES", "PANELOPERATION", dp.PanelOperation.ToString(),
                  "STRING", string.Empty);

               insertProperty(command, guid, "IFCDOORPANELPROPERTIES", "PANELPOSITION", dp.PanelPosition.ToString(),
                  "STRING", string.Empty);
#endif
            }
                
            if (p is IIfcWindowLiningProperties)
            {
               IIfcWindowLiningProperties wl = p as IIfcWindowLiningProperties;

               if (wl.LiningDepth != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCWINDOWLININGPROPERTIES", ref arrPropName, "LININGDEPTH",
                     ref arrPropVal, ref arrPropValBS, wl.LiningDepth.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(wl.LiningDepth.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCWINDOWLININGPROPERTIES", "LININGDEPTH", wl.LiningDepth.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(wl.LiningDepth.GetType()));
#endif
               }
               else if (wl.LiningThickness != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCWINDOWLININGPROPERTIES", ref arrPropName, "LININGTHICKNESS",
                     ref arrPropVal, ref arrPropValBS, wl.LiningThickness.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(wl.LiningThickness.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCWINDOWLININGPROPERTIES", "LININGTHICKNESS", wl.LiningThickness.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(wl.LiningThickness.GetType()));
#endif
               }
               else if (wl.TransomThickness != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCWINDOWLININGPROPERTIES", ref arrPropName, "TRANSOMTHICKNESS",
                     ref arrPropVal, ref arrPropValBS, wl.TransomThickness.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(wl.TransomThickness.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCWINDOWLININGPROPERTIES", "TRANSOMTHICKNESS", wl.TransomThickness.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(wl.TransomThickness.GetType()));
#endif
               }
               else if (wl.MullionThickness != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCWINDOWLININGPROPERTIES", ref arrPropName, "MULLIONTHICKNESS",
                     ref arrPropVal, ref arrPropValBS, wl.MullionThickness.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(wl.MullionThickness.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCWINDOWLININGPROPERTIES", "MULLIONTHICKNESS", wl.MullionThickness.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(wl.MullionThickness.GetType()));
#endif
               }
               else if (wl.FirstTransomOffset != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCWINDOWLININGPROPERTIES", ref arrPropName, "FIRSTTRANSOMOFFSET",
                     ref arrPropVal, ref arrPropValBS, wl.FirstTransomOffset.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(wl.FirstTransomOffset.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCWINDOWLININGPROPERTIES", "FIRSTTRANSOMOFFSET", wl.FirstTransomOffset.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(wl.FirstTransomOffset.GetType()));
#endif
               }
               else if (wl.SecondTransomOffset != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCWINDOWLININGPROPERTIES", ref arrPropName, "SECONDTRANSOMOFFSET",
                     ref arrPropVal, ref arrPropValBS, wl.SecondTransomOffset.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(wl.SecondTransomOffset.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCWINDOWLININGPROPERTIES", "SECONDTRANSOMOFFSET", wl.SecondTransomOffset.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(wl.SecondTransomOffset.GetType()));
#endif
               }
               else if (wl.FirstMullionOffset != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCWINDOWLININGPROPERTIES", ref arrPropName, "FIRSTMULLIONOFFSET",
                     ref arrPropVal, ref arrPropValBS, wl.FirstMullionOffset.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(wl.FirstMullionOffset.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCWINDOWLININGPROPERTIES", "FIRSTMULLIONOFFSET", wl.FirstMullionOffset.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(wl.FirstMullionOffset.GetType()));
#endif
               }
               else if (wl.SecondMullionOffset != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCWINDOWLININGPROPERTIES", ref arrPropName, "SECONDMULLIONOFFSET",
                     ref arrPropVal, ref arrPropValBS, wl.SecondMullionOffset.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(wl.SecondMullionOffset.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCWINDOWLININGPROPERTIES", "SECONDMULLIONOFFSET", wl.SecondMullionOffset.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(wl.SecondMullionOffset.GetType()));
#endif
               }
               else if (wl.ShapeAspectStyle != null)
               {
                  string shapeAspectStyleStr = getShapeAspectString(wl.ShapeAspectStyle);
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCWINDOWLININGPROPERTIES", ref arrPropName, "SHAPEASPECTSTYLE",
                     ref arrPropVal, ref arrPropValBS, shapeAspectStyleStr, ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, 
                     string.Empty);
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCWINDOWLININGPROPERTIES", "SHAPEASPECTSTYLE", shapeAspectStyleStr,
                     "STRING", string.Empty);
#endif
               }
            }
            else if (p is IIfcWindowPanelProperties)
            {
               IIfcWindowPanelProperties wp = p as IIfcWindowPanelProperties;

               if (wp.FrameDepth != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCWINDOWPANELPROPERTIES", ref arrPropName, "FRAMEDEPTH",
                     ref arrPropVal, ref arrPropValBS, wp.FrameDepth.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(wp.FrameDepth.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCWINDOWPANELPROPERTIES", "FRAMEDEPTH", wp.FrameDepth.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(wp.FrameDepth.GetType()));
#endif
               }
               if (wp.FrameThickness != null)
               {
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCWINDOWPANELPROPERTIES", ref arrPropName, "FRAMETHICKNESS",
                     ref arrPropVal, ref arrPropValBS, wp.FrameThickness.ToString(), ref arrPDatatyp, "DOUBLE", ref arrPUnit, ref arrPUnitBS, 
                     BIMRLUtils.getDefaultIfcUnitStr(wp.FrameThickness.GetType()));
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCWINDOWPANELPROPERTIES", "FRAMETHICKNESS", wp.FrameThickness.ToString(),
                     "DOUBLE", BIMRLUtils.getDefaultIfcUnitStr(wp.FrameThickness.GetType()));
#endif
               }
               else if (wp.ShapeAspectStyle != null)
               {
                  string shapeAspectStyleStr = getShapeAspectString(wp.ShapeAspectStyle);
#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCWINDOWPANELPROPERTIES", ref arrPropName, "SHAPEASPECTSTYLE",
                     ref arrPropVal, ref arrPropValBS, shapeAspectStyleStr, ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, 
                     string.Empty);
#endif
#if POSTGRES
                  insertProperty(command, guid, "IFCWINDOWPANELPROPERTIES", "SHAPEASPECTSTYLE", shapeAspectStyleStr,
                     "STRING", string.Empty);
#endif
               }
#if ORACLE
               insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCWINDOWPANELPROPERTIES", ref arrPropName, "OPERATIONTYPE",
                  ref arrPropVal, ref arrPropValBS, wp.OperationType.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, 
                  string.Empty);

               insertProperty(ref arrEleGuid, guid, ref arrPGrpName, "IFCWINDOWPANELPROPERTIES", ref arrPropName, "PANELPOSITION",
                  ref arrPropVal, ref arrPropValBS, wp.PanelPosition.ToString(), ref arrPDatatyp, "STRING", ref arrPUnit, ref arrPUnitBS, 
                  string.Empty);
#endif
#if POSTGRES
               insertProperty(command, guid, "IFCWINDOWPANELPROPERTIES", "OPERATIONTYPE", wp.OperationType.ToString(),
                  "STRING", string.Empty);

               insertProperty(command, guid, "IFCWINDOWPANELPROPERTIES", "PANELPOSITION", wp.PanelPosition.ToString(),
                  "STRING", string.Empty);
#endif
            }

            if (p is IIfcElementQuantity)
            {
               // Currently will ONLY support IfcPhysicalSimpleQuantity
               IIfcElementQuantity elq = p as IIfcElementQuantity;
               string pGrpName = "IFCELEMENTQUANTITY"; // Default name
               if (!string.IsNullOrEmpty(elq.Name))
                  pGrpName = elq.Name;

               foreach (IIfcPhysicalQuantity pQ in elq.Quantities)
               {
                  string unitOfMeasure = string.Empty;
                  if (pQ is IIfcPhysicalSimpleQuantity)
                  {
                     string pName;
                     if (!string.IsNullOrEmpty(pQ.Name))
                        pName = pQ.Name;
                     else
                        pName = pQ.GetType().Name.ToUpper();       // Set default to the type if name is not defined

                     string pDataType = pQ.GetType().Name.ToUpper();
                     if (((IIfcPhysicalSimpleQuantity)pQ).Unit != null)
                     {
                        IIfcPhysicalSimpleQuantity pQSimple = pQ as IIfcPhysicalSimpleQuantity;
                        unitOfMeasure = BIMRLUtils.getIfcUnitStr(pQSimple.Unit);
                     }

                     string pValue = string.Empty;
                     if (pQ is IIfcQuantityLength)
                     {
                        IIfcQuantityLength quant = pQ as IIfcQuantityLength;
                        pValue = quant.LengthValue.ToString();
                        if (string.IsNullOrEmpty(unitOfMeasure))
                           unitOfMeasure = BIMRLUtils.getDefaultIfcUnitStr(quant.LengthValue);
                     }
                     else if (pQ is IIfcQuantityArea)
                     {
                        IIfcQuantityArea quant = pQ as IIfcQuantityArea;
                        pValue = quant.AreaValue.ToString();
                        if (string.IsNullOrEmpty(unitOfMeasure))
                           unitOfMeasure = BIMRLUtils.getDefaultIfcUnitStr(quant.AreaValue);
                     }
                     else if (pQ is IIfcQuantityVolume)
                     {
                        IIfcQuantityVolume quant = pQ as IIfcQuantityVolume;
                        pValue = quant.VolumeValue.ToString();
                        if (string.IsNullOrEmpty(unitOfMeasure))
                           unitOfMeasure = BIMRLUtils.getDefaultIfcUnitStr(quant.VolumeValue);
                     }
                     else if (pQ is IIfcQuantityCount)
                     {
                        IIfcQuantityCount quant = pQ as IIfcQuantityCount;
                        pValue = quant.CountValue.ToString();
                        if (string.IsNullOrEmpty(unitOfMeasure))
                           unitOfMeasure = BIMRLUtils.getDefaultIfcUnitStr(quant.CountValue);
                     }
                     else if (pQ is IIfcQuantityWeight)
                     {
                        IIfcQuantityWeight quant = pQ as IIfcQuantityWeight;
                        pValue = quant.WeightValue.ToString();
                        if (string.IsNullOrEmpty(unitOfMeasure))
                           unitOfMeasure = BIMRLUtils.getDefaultIfcUnitStr(quant.WeightValue);
                     }
                     else if (pQ is IIfcQuantityTime)
                     {
                        IIfcQuantityTime quant = pQ as IIfcQuantityTime;
                        pValue = quant.TimeValue.ToString();
                        if (string.IsNullOrEmpty(unitOfMeasure))
                           unitOfMeasure = BIMRLUtils.getDefaultIfcUnitStr(quant.TimeValue);
                     }

#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, pGrpName, ref arrPropName, pName,
                     ref arrPropVal, ref arrPropValBS, pValue, ref arrPDatatyp, pDataType, ref arrPUnit, ref arrPUnitBS, 
                     unitOfMeasure);
#endif
#if POSTGRES
                     insertProperty(command, guid, pGrpName, pName, pValue,
                        pDataType, unitOfMeasure);
#endif
                  }
                  else if (pQ is IIfcPhysicalComplexQuantity)
                  {
                        // Not handled yet
                  }
               }
            }

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
               Param[5].Size = arrPUnitBS.Count;
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
            Param[5].Size = arrPUnitBS.Count;
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
         }
#endif
         DBOperation.commitTransaction();
         command.Dispose();
      }

      /// <summary>
      /// Process all properties
      /// </summary>
      /// <param name="el"></param>
      private void processProperties(string guid, IEnumerable<IIfcPropertySet> elPsets, string tableName)
      {
         string sqlStmt;

#if ORACLE
         sqlStmt = "Insert into " + DBOperation.formatTabName(tableName) + "(ElementId, PropertyGroupName, PropertyName, PropertyValue, PropertyDataType"
            + ", PropertyUnit) Values (:1, :2, :3, :4, :5, :6)";
         OracleCommand command = new OracleCommand(sqlStmt, DBOperation.DBConn);

         string currStep = sqlStmt;

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
#endif
#if POSTGRES
         sqlStmt = "Insert into " + DBOperation.formatTabName(tableName) + "(ElementId, PropertyGroupName, PropertyName, PropertyValue, PropertyDataType"
            + ", PropertyUnit) Values (@eid, @gname, @pname, @pvalue, @pdtyp, @punit)";
         NpgsqlCommand command = new NpgsqlCommand(sqlStmt, DBOperation.DBConn);
         string currStep = sqlStmt;
         command.Parameters.Add("@eid", NpgsqlDbType.Text);
         command.Parameters.Add("@gname", NpgsqlDbType.Text);
         command.Parameters.Add("@pname", NpgsqlDbType.Text);
         command.Parameters.Add("@pvalue", NpgsqlDbType.Text);
         command.Parameters.Add("@pdtyp", NpgsqlDbType.Text);
         command.Parameters.Add("@punit", NpgsqlDbType.Text);
         command.Prepare();

#endif
         // IEnumerable<IfcPropertySet> elPsets = el.PropertySets;
         foreach (IIfcPropertySet pset in elPsets)
         {
            IEnumerable<IIfcProperty> props = pset.HasProperties;
            foreach (IIfcProperty prop in props)
            {
               if (prop is IIfcSimpleProperty)
               {
                  Tuple<string, string, string, string> propVal = processSimpleProperty(prop);
                  if (string.IsNullOrEmpty(propVal.Item1))
                     continue;               // property not supported (only for Reference property)

#if ORACLE
                  insertProperty(ref arrEleGuid, guid, ref arrPGrpName, pset.Name, ref arrPropName, propVal.Item1,
                     ref arrPropVal, ref arrPropValBS, propVal.Item2, ref arrPDatatyp, propVal.Item3, ref arrPUnit, ref arrPUnitBS, 
                     propVal.Item4);
#endif
#if POSTGRES
                  insertProperty(command, guid, pset.Name, propVal.Item1, propVal.Item2,
                     propVal.Item3, propVal.Item4);
#endif
               }
               else if (prop is IIfcComplexProperty)
               {
                  IIfcComplexProperty comP = prop as IIfcComplexProperty;
                  List<Tuple<string, Tuple<string, string, string, string>>> compList = processComplexProp(prop);

                  for (int i = 0; i < compList.Count; i++)
                  {
#if ORACLE
                     insertProperty(ref arrEleGuid, guid, ref arrPGrpName, pset.Name + "." + compList[i].Item1, ref arrPropName, compList[i].Item2.Item1,
                        ref arrPropVal, ref arrPropValBS, compList[i].Item2.Item2, ref arrPDatatyp, compList[i].Item2.Item3, ref arrPUnit, ref arrPUnitBS, 
                        compList[i].Item2.Item4);
#endif
#if POSTGRES
                     insertProperty(command, guid, pset.Name + "." + compList[i].Item1, compList[i].Item2.Item1, compList[i].Item2.Item2,
                        compList[i].Item2.Item3, compList[i].Item2.Item4);
#endif

                  }
               }
               else
               {
                  // Not supported IfcProperty type
               }
            }
         }

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
            }
            catch (SystemException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + currStep;
               _refBIMRLCommon.StackPushError(excStr);
               throw;
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
         }
#endif
         DBOperation.commitTransaction();
         command.Dispose();
      }

      /// <summary>
      /// Processing a simple property. Returning string array containing: [0] property value in string format, [1] property data type, [2] Unit
      /// Property format: all will be in string:
      /// - Property Single value: string, single unit, single datatype
      /// - Property Enumerated value: string, no unit, datatype enumeration
      /// - Property Bounded value: [ <LowerBound>, <UpperBound> ], "-" when null, single unit, single datatype
      /// - Property Table value: (<defining value>, <defined value>); ( , ); ...;, unit (<defining unit>, <defined unit>), "-" when null, similarly for datatype
      /// - Property List value: (list1); (list2); ..., single unit, single datatype
      /// - Property Reference value: not supported!
      /// </summary>
      /// <param name="prop"></param>
      /// <param name="outStr"></param>
      /// <returns></returns>
      private Tuple<string, string, string, string> processSimpleProperty(IIfcProperty prop)
      {
         string propName = string.Empty;
         string propValue = string.Empty;
         string propDataType = string.Empty;
         string propUnit = string.Empty;

         if (prop is IIfcPropertySingleValue)
         {
            IIfcPropertySingleValue psv = prop as IIfcPropertySingleValue;
            propName = psv.Name;
            IIfcValue propNominalValue = psv.NominalValue;
            if (propNominalValue != null)
            {
               object pValue = propNominalValue.Value;
               if (pValue != null)
               {
                  propValue = pValue.ToString();
                  propDataType = pValue.GetType().Name.ToUpper();      // This will give the primitive datatype, e.g. Integer, Double, String
               }
            }

            IIfcUnit propSingleValueUnit = psv.Unit;
            if (propSingleValueUnit != null)
            {
               propUnit = BIMRLUtils.getIfcUnitStr(propSingleValueUnit);
            }
            else
            {
               propUnit = BIMRLUtils.getDefaultIfcUnitStr(propNominalValue);
            }
         }
         else if (prop is IIfcPropertyEnumeratedValue)
         {
            IIfcPropertyEnumeratedValue pev = prop as IIfcPropertyEnumeratedValue;
            propName = pev.Name;
            IItemSet<IIfcValue> propEnumerationValues = pev.EnumerationValues;
            if (propEnumerationValues != null)
            {
               string tmpStr = string.Empty;
               for (int i = 0; i < propEnumerationValues.Count; i++)
               {
                  tmpStr += "(" + propEnumerationValues[i].ToString() + "); ";
               }
               propValue = tmpStr;
               propDataType = propEnumerationValues[0].GetType().Name.ToUpper();
            }
         }
         else if (prop is IIfcPropertyBoundedValue)
         {
            IIfcPropertyBoundedValue pbv = prop as IIfcPropertyBoundedValue;
            propName = pbv.Name;
            IIfcValue propLowerBoundValue = pbv.LowerBoundValue;
            IIfcValue propUpperBoundValue = pbv.UpperBoundValue;
            string lowerB;
            string upperB;
            if (propLowerBoundValue == null)
               lowerB = "-";
            else
               lowerB = propLowerBoundValue.ToString();
            if (propUpperBoundValue == null)
               upperB = "-";
            else
               upperB = propUpperBoundValue.ToString();

            string tmpStr = "[" + lowerB + ", " + upperB + "]";

            if (propLowerBoundValue != null)
               propDataType = propLowerBoundValue.GetType().Name.ToUpper();
            else if (propUpperBoundValue != null)
               propDataType = propUpperBoundValue.GetType().Name.ToUpper();

            // We will always assign the property unit by its explicit unit, or by the IfcProject default unit if not specified
            IIfcUnit propBoundedValueUnit = pbv.Unit;
            if (propBoundedValueUnit != null)
            {
               propUnit = BIMRLUtils.getIfcUnitStr(propBoundedValueUnit);
            }
            else
            {
               propUnit = BIMRLUtils.getDefaultIfcUnitStr(propLowerBoundValue);
            }
         }
         else if (prop is IIfcPropertyTableValue)
         {
            IIfcPropertyTableValue ptv = prop as IIfcPropertyTableValue;
            IItemSet<IIfcValue> propDefiningValues = ptv.DefiningValues;
            IItemSet<IIfcValue> propDefinedValues = ptv.DefinedValues;
            propName = ptv.Name;
            if (propDefiningValues != null)
            {
               string tmpStr = string.Empty;
               for (int i = 0; i < propDefiningValues.Count; i++)
               {
                  if (propDefinedValues != null)
                        tmpStr += "(" + propDefiningValues[i].ToString() + ", " + propDefinedValues[i].ToString() + "); ";
                  else
                        tmpStr += "(" + propDefiningValues[i].ToString() + ", ); ";
               }
               propValue = tmpStr;
               if (propDefinedValues != null)
                  propDataType = "(" + propDefiningValues[0].GetType().Name.ToUpper() + ", " + propDefinedValues[0].GetType().Name.ToUpper() + ")";
               else
                  propDataType = "(" + propDefiningValues[0].GetType().Name.ToUpper() + ", )";
            }
            string definingUnitStr = "-";
            string definedUnitStr = "-";
            IIfcUnit propDefiningUnit = ptv.DefiningUnit;
            IIfcUnit propDefinedUnit = ptv.DefinedUnit;
            if (propDefiningUnit != null)
               definingUnitStr = BIMRLUtils.getIfcUnitStr(propDefiningUnit);
            else
               definingUnitStr = BIMRLUtils.getDefaultIfcUnitStr(propDefiningValues[0]);

            if (propDefinedUnit != null)
               definedUnitStr = BIMRLUtils.getIfcUnitStr(propDefinedUnit);
            else
               if (propDefinedValues!= null)
               definedUnitStr = BIMRLUtils.getDefaultIfcUnitStr(propDefinedValues[0]);

         }
         else if (prop is IIfcPropertyReferenceValue)
         {
            // ReferenceValue is not yet supported!
            IIfcPropertyReferenceValue prv = prop as IIfcPropertyReferenceValue;
            propName = prv.Name;
         }
         else if (prop is IIfcPropertyListValue)
         {
            IIfcPropertyListValue plv = prop as IIfcPropertyListValue;
            propName = plv.Name;
            IItemSet<IIfcValue> propListValues = plv.ListValues;
            if (propListValues != null)
            {
               string tmpStr = string.Empty;
               for (int i = 0; i < propListValues.Count; i++)
               {
                  tmpStr += "(" + propListValues[i].ToString() + "); ";
               }
               propValue = tmpStr;
               propDataType = propListValues[0].GetType().Name.ToUpper();
            }

            IIfcUnit propListValueUnit = plv.Unit;
            if (propListValueUnit != null)
            {
               propUnit = BIMRLUtils.getIfcUnitStr(propListValueUnit);
            }
            else
            {
               propUnit = BIMRLUtils.getDefaultIfcUnitStr(propListValues[0]);
            }
         }
         else
         {
               // prop not supported
         }
         propName = BIMRLUtils.checkSingleQuote(propName);
         if (propValue is string)
            propValue = BIMRLUtils.checkSingleQuote(propValue as string);

         return new Tuple<string, string, string, string>(propName, propValue, propDataType, propUnit);
      }
 
      /// <summary>
      /// Process a complex property. It allows recursive processing of complex property. What it does simply create list of the single properties
      /// The nested nature of the complex property is going to be flattened by moving the complex property as a new Pset level with name appended to the
      ///    parent property. When it is called recursively, all the single properties will be rolled up.
      /// An array of 5 strings will returned for each single property. All of them are put into a List:
      /// [0] - Name of the complex property (nested complex property will have name: <parent name>.<its name>
      /// [1] - Name of the property
      /// Array member [2] to [4] are the same as the simple property, in fact it is returned from it.
      /// [2] - Property value in a string format
      /// [3] - Property data type
      /// [4] - Property unit
      /// </summary>
      /// <param name="prop"></param>
      /// <param name="outStr"></param>
      //private void processComplexProp(IIfcProperty prop, out List<string[]> outStr)
      private List<Tuple<string, Tuple<string, string, string, string>>> processComplexProp(IIfcProperty prop)
      {
         //List<string[]> tmpList = new List<string[]>();
         List<Tuple<string, Tuple<string, string, string, string>>> tmpList = new List<Tuple<string, Tuple<string, string, string, string>>>();

         IIfcComplexProperty cProp = prop as IIfcComplexProperty;
         IEnumerable<IIfcProperty> hasProps = cProp.HasProperties;
         foreach (IIfcProperty hProp in hasProps)
         {
            if (hProp is IIfcSimpleProperty)
            {
               string complexPropName = prop.Name;
               Tuple<string, string, string, string> propVal = processSimpleProperty(hProp);
               if (propVal.Item2 == null)
                  continue;       // not supported (reference property only)

               tmpList.Add(new Tuple<string, Tuple<string, string, string, string>>(complexPropName, propVal));
            }
            else if (hProp is IIfcComplexProperty)
            {
               List<Tuple<string, Tuple<string, string, string, string>>> compPropValList = processComplexProp(hProp);
               if (compPropValList.Count == 0)
                  continue;   // empty list, maybe all of unspported types

               // go through the list now and populate own list
               for (int i = 0; i < compPropValList.Count; i++)
               {
                  tmpList.AddRange(compPropValList);
               }
            }
            else
            {
               // Not supported type
            }
         }

         return tmpList;
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

      /// <summary>
      /// It handles only the name/description part and not the geometry/representation
      /// </summary>
      /// <param name="shapeAspect">the shapeAspect</param>
      /// <returns>return the string containing the name and description</returns>
      string getShapeAspectString(IIfcShapeAspect shapeAspect)
      {
         string shapeAspectStyleStr = string.Empty;
         if (shapeAspect.Name.HasValue)
            BIMRLCommon.appendToString(shapeAspect.Name.Value, "; ", ref shapeAspectStyleStr);
         if (shapeAspect.Description.HasValue)
            BIMRLCommon.appendToString(shapeAspect.Description.Value, "; ", ref shapeAspectStyleStr);
         BIMRLCommon.appendToString(shapeAspect.ProductDefinitional.ToString(), "; ", ref shapeAspectStyleStr);
         return shapeAspectStyleStr;
      }
   }
}

