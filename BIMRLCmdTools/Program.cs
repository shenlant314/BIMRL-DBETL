using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using System.Text.RegularExpressions;
using BIMRL.Common;
//using BIMRL.BIMRLGraph;
using BIMRL;

namespace BIMRLCmdTools
{
   class Program
   {
      static BIMRLCommon BIMRLCommonRef = new BIMRLCommon();
      static void Main(string[] args)
      {
         BIMRLQueryModel _qModel;
         DBOperation.refBIMRLCommon = BIMRLCommonRef;      // important to ensure DBoperation has reference to this object!!
         List<BIMRLModelInfo> modelInfos = new List<BIMRLModelInfo>();
         List<FederatedModelInfo> fedModels = new List<FederatedModelInfo>();
         BIMRLCommonRef.resetAll();

         if (args.Count() > 0)
         {
            // It has connection specification, will try to use it before usign the default
            try
            {
               string[] split = args[0].Split('/', '@');
               if (split.Count() >= 3)
               {
                  string username = split[0];
                  string password = split[1];
                  string connectStr = split[3];
                  DBOperation.ConnectToDB(username, password, connectStr);
               }
            }
            catch (Exception e)
            {
               Console.WriteLine("%Error: Unable to connect using the specified connect string! " + e.Message);
               Console.WriteLine(" -- Trying using the default user instead!");
            }
         }

         try
         {
            // Connect to DB
            DBOperation.ExistingOrDefaultConnection();
         }
         catch (Exception ex)
         {
            Console.Write("\n%Error:\n" + BIMRLCommonRef.ErrorMessages + ": ", ex.Message);
            return;
         }

         _qModel = new BIMRLQueryModel(BIMRLCommonRef);
         fedModels = _qModel.getFederatedModels();
         foreach (FederatedModelInfo model in fedModels)
         {
            Console.WriteLine("--------------------------------------");
            Console.WriteLine("\tModel ID           : " + model.FederatedID.ToString());
            Console.WriteLine("\tModel Name         : " + model.ModelName);
            Console.WriteLine("\tProject Number     : " + model.ProjectNumber);
            Console.WriteLine("\tProject Name       : " + model.ProjectName);
            Console.WriteLine("\tWorld bounding box : " + (string.IsNullOrEmpty(model.WorldBoundingBox)? "" : model.WorldBoundingBox.ToString()));
            Console.WriteLine("\tOctree Max Depth   : " + model.OctreeMaxDepth.ToString());
            Console.WriteLine("\tLast update date   : " + model.LastUpdateDate.ToString());
            Console.WriteLine("\tOwner              : " + model.Owner);
            Console.WriteLine("\tDB connection      : " + model.DBConnection);
            Console.WriteLine("");
         }

         while (true)
         {
            Console.WriteLine(" ------------------------------------");
            Console.WriteLine("\tAvailable operations:");
            Console.WriteLine("\t\t 1) Postprocess model");
            Console.WriteLine("\t\t 2) Generate X3D");
            Console.WriteLine("\t\t 3) Delete model");
            Console.WriteLine("\t\t E) Exit");
            Console.Write("\t*** Select option: ");
            string selectedOption = Console.ReadLine();
            if (string.IsNullOrEmpty(selectedOption))
               continue;

            if (selectedOption[0] == 'E' || selectedOption[0] == 'e')
               break;
            else if (selectedOption[0] == '1')
               Postprocess();
            else if (selectedOption[0] == '2')
               GenerateX3D();
            else if (selectedOption[0] == '3')
               DeleteModel();
            else
               continue;
         }
      }

      static void Postprocess()
      {
         int FedID = -1;
         bool exit = false;
         FederatedModelInfo fedModel = null;
         while (true)
         {
            Console.Write("\n\tSelect Model ID to process [E to exit]: ");
            string selID = Console.ReadLine();
            if (string.IsNullOrEmpty(selID))
               continue;

            if (selID[0] == 'E' || selID[0] == 'e')
            {
               exit = true;
               break;
            }

            if (!int.TryParse(selID, out FedID))
            {
               Console.WriteLine("%Incorrect ID selection. Please specify an exisitng Model ID!");
               continue;
            }
            fedModel = DBOperation.getFederatedModelByID(FedID);
            if (fedModel == null)
            {
               Console.WriteLine("%Incorrect ID selection. Please specify an exisitng Model ID!");
               continue;
            }
            else
               break;
         }
         if (exit)
            return;

         DBOperation.currFedModel = fedModel;
         string sel = "E";
         while (true)
         {
            Console.WriteLine("------------------------------------");
            Console.WriteLine("\tAvailable options for Postprocess:");
            Console.WriteLine("\t\t L) Report default Octree level for this model");
            Console.WriteLine("\t\t R) Run Postprocess");
            Console.WriteLine("\t\t E) Exit");
            Console.Write("\t*** Select option: ");
            sel = Console.ReadLine();
            if (string.IsNullOrEmpty(sel))
               continue;

            if (sel[0] != 'L' && sel[0] != 'l' && sel[0] != 'R' && sel[0] != 'r' && sel[0] != 'E' && sel[0] != 'e')
               continue;

            if (sel[0] == 'E' || sel[0] == 'e')
               return;

            if (sel[0] == 'L' || sel[0] == 'l')
            {
               int level = DBOperation.computeRecomOctreeLevel(FedID);
               if (level > 0)
                  Console.WriteLine("\t\tComputed Octree level for this model: " + level.ToString());
               else
                  Console.WriteLine("\t\tNo information available to computed Octree level for this model");
               continue;
            }

            if (sel[0] == 'R' || sel[0] == 'r')
            {
               bool regenSpatialIndex = true;
               bool regenBoundaryFaces = true;
               bool regenMajorAxes = true;
               bool enhanceSpBound = true;
               double currentTol = MathUtils.tol;

               Console.Write("\t\t\tProcess Spatial Index? [Y] ");
               string spIdxSel = Console.ReadLine();
               if (!string.IsNullOrEmpty(spIdxSel))
               {
                  if (spIdxSel[0] == 'N' || spIdxSel[0] == 'n')
                     regenSpatialIndex = false;
               }

               Console.Write("\t\t\tProcess Boundary Faces? [Y] ");
               string bFace = Console.ReadLine();
               if (!string.IsNullOrEmpty(bFace))
               {
                  if (bFace[0] == 'N' || bFace[0] == 'n')
                     regenBoundaryFaces = false;
               }

               Console.Write("\t\t\tProcess Major-Axes and OBB? [Y] ");
               string obb = Console.ReadLine();
               if (!string.IsNullOrEmpty(obb))
               {
                  if (obb[0] == 'N' || obb[0] == 'n')
                     regenMajorAxes = false;
               }

               Console.Write("\t\t\tProcess Enhancement to Space Boundaries? [Y] ");
               string spBound = Console.ReadLine();
               if (!string.IsNullOrEmpty(spBound))
               {
                  if (spBound[0] == 'N' || spBound[0] == 'n')
                     enhanceSpBound = false;
               }

               Console.Write("\t\t\tModify Tolerance? [" + currentTol.ToString() + "] ");
               string tol = Console.ReadLine();
               if (!string.IsNullOrEmpty(tol))
               {
                  double tolSetting;
                  if (double.TryParse(tol, out tolSetting))
                     MathUtils.tol = tolSetting;
               }

               Console.Write("\t\t\tModify Octree Level? [" + DBOperation.computeRecomOctreeLevel(FedID) + "] ");
               string levelSetting = Console.ReadLine();
               if (!string.IsNullOrEmpty(levelSetting))
               {
                  int level = -1;
                  level = int.Parse(levelSetting);
                  if (level > 0)
                     DBOperation.OctreeSubdivLevel = level;
               }

               Console.Write("\t\t\tWhere condition: [] ");
               string whereCond = Console.ReadLine();

               BIMRLSpatialIndex spIdx = new BIMRLSpatialIndex(BIMRLCommonRef);
               DBOperation.commitInterval = 5000;

               var location = new Uri(Assembly.GetEntryAssembly().GetName().CodeBase);
               string exePath = new FileInfo(location.AbsolutePath).Directory.FullName.Replace("%20", " ");

               try
               {
                  if (FedID >= 0)
                  {
                     bool recreateIndex = false;
                     string updOctreeLevel = "";
                     if (!string.IsNullOrEmpty(whereCond))
                     {
                        // Spatial always needs to be reconstructed even when one object is updated to maintain integrity of non-overlapping octree concept
                        if (regenSpatialIndex)
                        {
                           // We need the existing data to regenerate the dictionary. Truncate operation will be deferred until just before insert into the table
                           // DBOperation.executeSingleStmt("TRUNCATE TABLE BIMRL_SPATIALINDEX_" + FedID.ToString("X4"));
                           // DBOperation.executeSingleStmt("DELETE FROM BIMRL_SPATIALINDEX_" + FedID.ToString("X4") + " WHERE " + whereCond);
                        }
                        if (regenBoundaryFaces)
                           DBOperation.executeSingleStmt("DELETE FROM " + DBOperation.formatTabName("BIMRL_TOPO_FACE", FedID) + " WHERE " + whereCond);
                     }
                     else
                     {
                        if (DBOperation.DBUserID.Equals(fedModel.Owner))
                        {
                           recreateIndex = true;
                           if (regenSpatialIndex)
                           {
                              DBOperation.ExecuteSystemScript(FedID, "BIMRL_Idx_SpatialIndexes_dr.sql");
                              DBOperation.executeSingleStmt("TRUNCATE TABLE " + DBOperation.formatTabName("BIMRL_SPATIALINDEX", FedID));
                           }
                           if (regenBoundaryFaces)
                           {
                              DBOperation.ExecuteSystemScript(FedID, "BIMRL_Idx_TopoFace_dr.sql");
                              DBOperation.executeSingleStmt("TRUNCATE TABLE " + DBOperation.formatTabName("BIMRL_TOPO_FACE", FedID));
                           }
                        }
                        else
                        {
                           if (regenSpatialIndex)
                              DBOperation.executeSingleStmt("DELETE FROM " + DBOperation.formatTabName("BIMRL_SPATIALINDEX", FedID));
                           if (regenBoundaryFaces)
                              DBOperation.executeSingleStmt("DELETE FROM " + DBOperation.formatTabName("BIMRL_TOPO_FACE", FedID));
                        }
                     }

                     // Update Spatial index (including major axes and OBB) and Boundary faces
                     if (regenSpatialIndex && regenBoundaryFaces && regenMajorAxes)
                     {
                        spIdx.createSpatialIndexFromBIMRLElement(FedID, whereCond, true);
                        BIMRLUtils.updateMajorAxesAndOBB(FedID, whereCond);
                        //DBOperation.executeScript(Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_Idx_SpatialIndexes_cr.sql"), FedID);
                        //DBOperation.executeScript(Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_Idx_TopoFace_cr.sql"), FedID);
                        //DBOperation.executeScript(Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_Idx_MajorAxes.sql"), FedID);
                        if (recreateIndex)
                           DBOperation.ExecuteSystemScript(FedID, "BIMRL_Idx_SpatialIndexes_cr.sql", "BIMRL_Idx_TopoFace_cr.sql", "BIMRL_Idx_MajorAxes.sql");
                        updOctreeLevel = "MAXOCTREELEVEL=" + DBOperation.OctreeSubdivLevel.ToString();
                     }
                     else if (regenSpatialIndex && regenBoundaryFaces && !regenMajorAxes)
                     {
                        spIdx.createSpatialIndexFromBIMRLElement(FedID, whereCond, true);
                        //DBOperation.executeScript(Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_Idx_SpatialIndexes_cr.sql"), FedID);
                        //DBOperation.executeScript(Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_Idx_TopoFace_cr.sql"), FedID);
                        if (recreateIndex)
                           DBOperation.ExecuteSystemScript(FedID, "BIMRL_Idx_SpatialIndexes_cr.sql", "BIMRL_Idx_TopoFace_cr.sql");
                        updOctreeLevel = "MAXOCTREELEVEL=" + DBOperation.OctreeSubdivLevel.ToString();
                     }
                     // Update Spatial index (including major axes and OBB) only
                     else if (regenSpatialIndex && !regenBoundaryFaces && regenMajorAxes)
                     {
                        spIdx.createSpatialIndexFromBIMRLElement(FedID, whereCond, false);
                        BIMRLUtils.updateMajorAxesAndOBB(FedID, whereCond);
                        //DBOperation.executeScript(Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_Idx_SpatialIndexes_cr.sql"), FedID);
                        //DBOperation.executeScript(Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_Idx_MajorAxes.sql"), FedID);
                        if (recreateIndex)
                           DBOperation.ExecuteSystemScript(FedID, "BIMRL_Idx_SpatialIndexes_cr.sql", "BIMRL_Idx_MajorAxes.sql");
                        updOctreeLevel = "MAXOCTREELEVEL=" + DBOperation.OctreeSubdivLevel.ToString();
                     }
                     // Update Boundary faces and MajorAxes
                     else if (!regenSpatialIndex && regenBoundaryFaces && regenMajorAxes)
                     {
                        spIdx.createFacesFromBIMRLElement(FedID, whereCond);
                        //DBOperation.executeScript(Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_Idx_TopoFace_cr.sql"), FedID);
                        if (recreateIndex)
                           DBOperation.ExecuteSystemScript(FedID, "BIMRL_Idx_TopoFace_cr.sql");
                        BIMRLUtils.updateMajorAxesAndOBB(FedID, whereCond);
                        //DBOperation.executeScript(Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_Idx_MajorAxes.sql"), FedID);
                        if (recreateIndex)
                           DBOperation.ExecuteSystemScript(FedID, "BIMRL_Idx_MajorAxes.sql");
                     }
                     // Update Spatial Index only
                     else if (regenSpatialIndex && !regenBoundaryFaces && !regenMajorAxes)
                     {
                        spIdx.createSpatialIndexFromBIMRLElement(FedID, whereCond, false);
                        //DBOperation.executeScript(Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_Idx_SpatialIndexes_cr.sql"), FedID);
                        if (recreateIndex)
                           DBOperation.ExecuteSystemScript(FedID, "BIMRL_Idx_SpatialIndexes_cr.sql");
                        updOctreeLevel = "MAXOCTREELEVEL=" + DBOperation.OctreeSubdivLevel.ToString();
                     }
                     // update faces only
                     else if (!regenSpatialIndex && regenBoundaryFaces && !regenMajorAxes)
                     {
                        spIdx.createFacesFromBIMRLElement(FedID, whereCond);
                        //DBOperation.executeScript(Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_Idx_TopoFace_cr.sql"), FedID);
                        if (recreateIndex)
                           DBOperation.ExecuteSystemScript(FedID, "BIMRL_Idx_TopoFace_cr.sql");
                     }
                     // Update only the major axes and OBB only
                     else if (!regenSpatialIndex && !regenBoundaryFaces && regenMajorAxes)
                     {
                        BIMRLUtils.updateMajorAxesAndOBB(FedID, whereCond);
                        //DBOperation.executeScript(Path.Combine(exePath, DBOperation.ScriptPath, "BIMRL_Idx_MajorAxes.sql"), FedID);
                        if (recreateIndex)
                           DBOperation.ExecuteSystemScript(FedID, "BIMRL_Idx_MajorAxes.sql");
                     }
                     else
                     {
                        // Invalid option
                     }

                     if (enhanceSpBound)
                     {
                        if (!string.IsNullOrEmpty(whereCond))
                        {
                           string whereCondD = Regex.Replace(whereCond, "elementid", "spaceelementid", RegexOptions.IgnoreCase);
                           DBOperation.executeSingleStmt("DELETE FROM " + DBOperation.formatTabName("BIMRL_RELSPACEB_DETAIL", FedID) + " WHERE " + whereCondD);
                        }
                        else
                        {
                           if (fedModel.Owner.Equals(DBOperation.DBUserID))
                              DBOperation.executeSingleStmt("TRUNCATE TABLE " + DBOperation.formatTabName("BIMRL_RELSPACEB_DETAIL", FedID));
                           else
                              DBOperation.executeSingleStmt("DELETE FROM " + DBOperation.formatTabName("BIMRL_RELSPACEB_DETAIL", FedID));
                        }

                        DBOperation.commitInterval = 5000;
                        EnhanceBRep eBrep = new EnhanceBRep();
                        eBrep.enhanceSpaceBoundary(whereCond);

                        // We will procees the normal face first and then after that the spacial ones (OBB, PROJOBB)
                        string whereCond2 = whereCond;
                        BIMRLCommon.appendToString(" TYPE NOT IN ('OBB','PROJOBB')", " AND ", ref whereCond2);
                        eBrep.ProcessOrientation(whereCond2);
                        whereCond2 = whereCond;
                        BIMRLCommon.appendToString(" TYPE='OBB'", " AND ", ref whereCond2);
                        eBrep.ProcessOrientation(whereCond2);
                        whereCond2 = whereCond;
                        BIMRLCommon.appendToString(" TYPE='PROJOBB'", " AND ", ref whereCond2);
                        eBrep.ProcessOrientation(whereCond2);
                     }

#if ORACLE
                     string sqlStmt = "UPDATE BIMRL_FEDERATEDMODEL SET LASTUPDATEDATE=sysdate";
#endif
#if POSTGRES
                     string sqlStmt = "UPDATE BIMRL_FEDERATEDMODEL SET LASTUPDATEDATE=now()";
#endif
                     BIMRLCommon.appendToString(updOctreeLevel, ", ", ref sqlStmt);
                     BIMRLCommon.appendToString("WHERE FEDERATEDID=" + FedID, " ", ref sqlStmt);
                     DBOperation.executeSingleStmt(sqlStmt);
                  }
               }
               catch (SystemException excp)
               {
                  string excStr = "%% Error - " + excp.Message + "\n\t";
                  BIMRLCommonRef.StackPushError(excStr);
                  if (BIMRLCommonRef.BIMRLErrorStackCount > 0)
                     Console.WriteLine(BIMRLCommonRef.ErrorMessages);
                  DBOperation.rollbackTransaction();
                  BIMRLCommonRef.ErrorStackClear();
               }
               MathUtils.tol = currentTol;
               DBOperation.commitTransaction();
               if (BIMRLCommonRef.BIMRLErrorStackCount > 0)
                  Console.WriteLine(BIMRLCommonRef.ErrorMessages);
               BIMRLCommonRef.ErrorStackClear();
            }
         }
      }

      static void GenerateX3D()
      {
         int FedID = -1;
         bool exit = false;
         FederatedModelInfo fedModel = null;
         string whereCond = string.Empty;
         string outFileName = "/tmp/bimrlout.x3d";

         while (true)
         {
            Console.Write("\n\tSelect Model ID to process [E to exit]: ");
            string selID = Console.ReadLine();

            if (string.IsNullOrEmpty(selID))
               continue;

            if (selID[0] == 'E' || selID[0] == 'e')
            {
               exit = true;
               break;
            }

            if (!int.TryParse(selID, out FedID))
            {
               Console.WriteLine("%Incorrect ID selection. Please specify an exisitng Model ID!");
               continue;
            }
            fedModel = DBOperation.getFederatedModelByID(FedID);
            if (fedModel == null)
            {
               Console.WriteLine("%Incorrect ID selection. Please specify an exisitng Model ID!");
               continue;
            }
            else
               break;
         }
         if (exit)
            return;

         DBOperation.currFedModel = fedModel;
         bool drawElemGeom = true;
         bool drawUserGeom = false;
         bool drawFacesOnly = false;
         bool drawOctree = false;
         bool drawWorldBB = false;
         string altUserTable = "";

         Console.Write("\t\t\tWhere condition: [] ");
         whereCond = Console.ReadLine();
            
         Console.Write("\t\t\tOutput file name [" + outFileName + "] ");
         string outF = Console.ReadLine();
         if (!string.IsNullOrEmpty(outF))
         {
            outFileName = outF;
         }

         Console.Write("\t\t\tDraw Element geometry? [" + (drawElemGeom? "Y" : "N") + "] ");
         string eGeom = Console.ReadLine();
         if (!string.IsNullOrEmpty(eGeom))
         {
            if (eGeom[0] == 'N' || eGeom[0] == 'n')
               drawElemGeom = false;
            else
               drawElemGeom = true;
         }

         Console.Write("\t\t\tDraw User geometry? [" + (drawUserGeom ? "Y" : "N") + "] ");
         string uGeom = Console.ReadLine();
         if (!string.IsNullOrEmpty(uGeom))
         {
            if (uGeom[0] == 'Y' || eGeom[0] == 'y')
            {
               drawUserGeom = true;

               Console.Write("\t\t\tAlternate User Geometry table: [] ");
               altUserTable = Console.ReadLine();
            }
            else
               drawUserGeom = false;
         }

         Console.Write("\t\t\tDraw Faces only? [" + (drawFacesOnly ? "Y" : "N") + "] ");
         string drFaces = Console.ReadLine();
         if (!string.IsNullOrEmpty(drFaces))
         {
            if (drFaces[0] == 'Y' || drFaces[0] == 'y')
               drawFacesOnly = true;
            else
               drawFacesOnly = false;
         }

         Console.Write("\t\t\tDraw Octree cells? [" + (drawOctree ? "Y" : "N") + "] ");
         string drOctree = Console.ReadLine();
         if (!string.IsNullOrEmpty(drOctree))
         {
            if (drOctree[0] == 'Y' || drOctree[0] == 'y')
               drawOctree = true;
            else
               drawOctree = false;
         }

         Console.Write("\t\t\tDraw World bounding box? [" + (drawWorldBB ? "Y" : "N") + "] ");
         string drBB = Console.ReadLine();
         if (!string.IsNullOrEmpty(drBB))
         {
            if (drBB[0] == 'Y' || drBB[0] == 'y')
               drawWorldBB = true;
            else
               drawWorldBB = false;
         }

         try
         {
            BIMRLExportSDOToX3D x3dExp = new BIMRLExportSDOToX3D(BIMRLCommonRef, outFileName);
            if (!string.IsNullOrEmpty(altUserTable))
               x3dExp.altUserTable = altUserTable;

            x3dExp.exportToX3D(FedID, whereCond, drawElemGeom, drawUserGeom, drawFacesOnly, drawOctree, drawWorldBB);
            x3dExp.endExportToX3D();
         }
         catch (SystemException excp)
         {
            string excStr = "%% Error - " + excp.Message + "\n\t";
            BIMRLCommonRef.StackPushError(excStr);
            if (BIMRLCommonRef.BIMRLErrorStackCount > 0)
               Console.WriteLine(BIMRLCommonRef.ErrorMessages);
            DBOperation.rollbackTransaction();
            BIMRLCommonRef.ErrorStackClear();
         }
         DBOperation.rollbackTransaction();
         if (BIMRLCommonRef.BIMRLErrorStackCount > 0)
            Console.WriteLine(BIMRLCommonRef.ErrorMessages);
         BIMRLCommonRef.ErrorStackClear();
      }

      static void DeleteModel()
      {
         int FedID = -1;
         bool exit = false;
         FederatedModelInfo fedModel = null;

         while (true)
         {
            Console.Write("\n\tSelect Model ID to delete [E to exit]: ");
            string selID = Console.ReadLine();
            if (string.IsNullOrEmpty(selID))
               continue;

            if (selID[0] == 'E' || selID[0] == 'e')
            {
               exit = true;
               break;
            }

            if (!int.TryParse(selID, out FedID))
            {
               Console.WriteLine("%Incorrect ID selection. Please specify an exisitng Model ID!");
               continue;
            }
            fedModel = DBOperation.getFederatedModelByID(FedID);
            if (fedModel == null)
            {
               Console.WriteLine("%Incorrect ID selection. Please specify an exisitng Model ID!");
               continue;
            }
            else
            {
               if (!fedModel.Owner.Equals(DBOperation.DBUserID))
               {
                  Console.WriteLine("%The selected model is owned by a different user!");
                  continue;
               }
               break;
            }
         }
         if (exit)
            return;

         DBOperation.currFedModel = fedModel;
         Console.WriteLine("**** You have selected the following model to be deleted:");
         Console.WriteLine("\t\t\tModel ID: " + fedModel.FederatedID.ToString());
         Console.WriteLine("\t\t\tModel Name: " + fedModel.ModelName);
         Console.WriteLine("\t\t\tLast update date: " + fedModel.LastUpdateDate.ToString());
         BIMRLQueryModel qModel = new BIMRLQueryModel(BIMRLCommonRef);
         IList<BIMRLModelInfo> modelInfo = qModel.getModelInfos(FedID);
         Console.WriteLine("\t\t\tNo. of models inside the federation : " + modelInfo.Count());
         int elemCount = 0;
         foreach (BIMRLModelInfo mInfo in modelInfo)
         {
            elemCount += mInfo.NumberOfElement;
         }
         Console.WriteLine("\t\t\tTotal element count                 : " + elemCount);
         Console.Write("\n**** Confirm to DELETE (model will be permanently deleted from the DB)? [N] ");
         string del = Console.ReadLine();
         if (!string.IsNullOrEmpty(del))
         {
            if (del[0] == 'Y' || del[0] == 'y')
            {
               qModel.deleteModel(FedID);
            }
         }

         return;
      }
   }
}
