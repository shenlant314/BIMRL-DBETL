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
public class BIMRLGeometryPostProcess
   {
      static Vector3D _trueNorth = new Vector3D();
      Polyhedron _geom;
      string _elementid;
      int _currFedID;
      HashSet<int> _mergedFaceList = new HashSet<int>();
      BIMRLCommon _refBIMRLCommon;
#if ORACLE
      public SdoGeometry OBB;
#endif
#if POSTGRES
      public Polyhedron OBB;
#endif
      string _faceCategory;
      Vector3D[] majorAxes;
      int _fIDOffset;

      Dictionary<int, Face3D> facesColl = new Dictionary<int, Face3D>();

      Dictionary<Point3D, HashSet<int>> sortedFVert = new Dictionary<Point3D, HashSet<int>>();

      public BIMRLGeometryPostProcess(string elementid, Polyhedron geometry, BIMRLCommon bimrlCommon, int federatedID, string faceCategory)
      {
         _geom = geometry;
         _elementid = elementid;
         _refBIMRLCommon = bimrlCommon;
         _currFedID = federatedID;
         // For optimization purpose, the offset for face id is fixed to 10000 for OBB and 10100 for PROJOBB. If there is any other category in future, this needs to be updated !!
         if (!string.IsNullOrEmpty(faceCategory))
         {
            _faceCategory = faceCategory;
            if (string.Compare(faceCategory, "OBB") == 0)
               _fIDOffset = 10000;
            else if (string.Compare(faceCategory, "PROJOBB") == 0)
               _fIDOffset = 10100;
            else
               _fIDOffset = 10200;
         }
         else
         {
            _faceCategory = "BODY"; // default value
            _fIDOffset = 0;
         }

         string sqlStmt = null;
         try
         {
            Vector3D nullVector = new Vector3D();
            if (_trueNorth == nullVector)
            {
               sqlStmt = "Select PROPERTYVALUE FROM " + DBOperation.formatTabName("BIMRL_PROPERTIES", _currFedID) + " WHERE PROPERTYGROUPNAME='IFCATTRIBUTES' AND PROPERTYNAME='TRUENORTH'";
#if ORACLE
               OracleCommand cmd = new OracleCommand(sqlStmt, DBOperation.DBConn);
               object ret = cmd.ExecuteScalar();
#endif
#if POSTGRES
               object ret = DBOperation.ExecuteScalarWithTrans2(sqlStmt);
#endif
               if (ret != null)
               {
                  string trueNorthStr = ret as string;
                  string tmpStr = trueNorthStr.Replace('[', ' ');
                  tmpStr = tmpStr.Replace(']', ' ');

                  string[] tokens = tmpStr.Trim().Split(',');
                  if (tokens.Length < 2)
                  {
                     // not a valid value, use default
                     _trueNorth = new Vector3D(0.0, 1.0, 0.0);
                  }
                  else
                  {
                     double x = Convert.ToDouble(tokens[0]);
                     double y = Convert.ToDouble(tokens[1]);
                     double z = 0.0;     // ignore Z for true north
                     //if (tokens.Length >= 3)
                     //    z = Convert.ToDouble(tokens[2]); 
                     _trueNorth = new Vector3D(x, y, z);
                  }
               }
               else
               {
                  _trueNorth = new Vector3D(0.0, 1.0, 0.0);   // if not defined, default is the project North = +Y of the coordinate system
               }
#if ORACLE
               cmd.Dispose();
#endif
            }
         }
#if ORACLE
         catch (OracleException e)
#endif
#if POSTGRES
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Insert Error - " + e.Message + "\n\t" + sqlStmt;
            _refBIMRLCommon.StackPushIgnorableError(excStr);
#if POSTGRES
            DBOperation.CurrTransaction.Rollback(DBOperation.def_savepoint);
#endif
            // Ignore any error
         }
         catch (SystemException e)
         {
            string excStr = "%%Insert Error - " + e.Message + "\n\t" + sqlStmt;
            _refBIMRLCommon.StackPushError(excStr);
            throw;
         }	
      }

      public List<Face3D> MergedFaceList
      {
         get 
         {
            List<Face3D> mergedFaceList = new List<Face3D>();
            foreach (int Idx in _mergedFaceList)
               mergedFaceList.Add(facesColl[Idx]);
            return mergedFaceList;
         }
      }

      public void simplifyAndMergeFaces()
      {
         // Set an arbitrary max faces to avoid complex geometry and only work on relatively simpler geometry such as Spaces, Walls
         //if (_geom.Faces.Count > 1000)
         //    return;
         double origTol = MathUtils.tol;
         int origDPrec = MathUtils._doubleDecimalPrecision;
         int origFPrec = MathUtils._floatDecimalPrecision;
         int lastFaceID = 0;

         // Use better precision for the merging of faces because it deals with smaller numbers generally (e.g. Millimeter default tol we use 0.1mm. For this we will use 0.001)
         MathUtils.tol = origTol/100;
         MathUtils._doubleDecimalPrecision = origDPrec + 2;
         MathUtils._floatDecimalPrecision = origFPrec + 2;

         foreach (Face3D f in _geom.Faces)
         {
            facesColl.Add(lastFaceID, f);         // Keep faces in a dictionary and assigns ID

            foreach (Point3D vert in f.outerAndInnerVertices)
            {
               HashSet<int> facesOfVert;
               if (!sortedFVert.TryGetValue(vert, out facesOfVert))
               {
                  facesOfVert = new HashSet<int>();
                  facesOfVert.Add(lastFaceID);
                  sortedFVert.Add(vert, facesOfVert);
               }
               else
               {
                  // Dict already contains the point, update the HashSet with this new face
                  facesOfVert.Add(lastFaceID);
               }
            }
            lastFaceID++;
         }
         // After the above, we have a sorted polyhedron vertices that contains hashset of faces it belongs to
         // Loop through the dictionary to merge faces that have the same normal (on the same plane)
         foreach (KeyValuePair<Point3D, HashSet<int>> dictItem in sortedFVert)
         {
            IEqualityComparer<Vector3D> normalComparer = new vectorCompare(MathUtils.tol, MathUtils._doubleDecimalPrecision);
            Dictionary<Vector3D, List<int>> faceSortedByNormal = new Dictionary<Vector3D, List<int>>(normalComparer);
            List<int> fIDList;
            List<int> badFIDList = new List<int>();

            foreach (int fID in dictItem.Value)
            {
               Face3D f = facesColl[fID];
               if ((double.IsNaN(f.basePlane.normalVector.X) && double.IsNaN(f.basePlane.normalVector.Y) && double.IsNaN(f.basePlane.normalVector.Z))
                  || (f.basePlane.normalVector.X == 0.0 && f.basePlane.normalVector.Y == 0.0 && f.basePlane.normalVector.Z == 0.0))
               {
                  badFIDList.Add(fID);
                  continue;
               }

               if (!faceSortedByNormal.TryGetValue(f.basePlane.normalVector, out fIDList))
               {
                  fIDList = new List<int>();
                  fIDList.Add(fID);
                  faceSortedByNormal.Add(f.basePlane.normalVector, fIDList);
               }
               else
               {
                  if (!fIDList.Contains(fID))
                  {
                     fIDList.Add(fID);
                  }
               }
            }

            foreach(KeyValuePair<Vector3D, List<int>> fListDict in faceSortedByNormal)
            {
               // Add bad face IDs into each list as candidate that may be needed to complete the merge
               fListDict.Value.AddRange(badFIDList);
               List<int> mergedFaceList = null;
               if (fListDict.Value.Count > 1)
               {
                  tryMergeFaces(fListDict.Value, out mergedFaceList);
                  if (mergedFaceList != null && mergedFaceList.Count > 0)
                  {
                     // insert only new face indexes as the mergedlist from different vertices can be duplicated
                     foreach (int fIdx in mergedFaceList)
                        if (!_mergedFaceList.Contains(fIdx))
                           _mergedFaceList.Add(fIdx);
                  }
               }
               else
                  if (!_mergedFaceList.Contains(fListDict.Value[0]))
                     _mergedFaceList.Add(fListDict.Value[0]);    // No pair face, add it into the mergedList
            }
            badFIDList.Clear();
         }

         MathUtils.tol = origTol;
         MathUtils._doubleDecimalPrecision = origDPrec;
         MathUtils._floatDecimalPrecision = origFPrec;
      }

      bool tryMergeFaces(List<int> inputFaceList, out List<int> outputFaceList)
      {
         outputFaceList = new List<int>();
         Face3D firstF = facesColl[inputFaceList[0]];
         //int prevFirstFIdx = 0;
         HashSet<int> mergedFacesIdxList = new HashSet<int>();
         mergedFacesIdxList.Add(inputFaceList[0]);

         inputFaceList.RemoveAt(0);  // remove the first face from the list
         int currEdgeIdx = 0;
         bool merged = false;

         IEqualityComparer<LineSegment3D> segmentCompare = new SegmentCompare();
         IDictionary<LineSegment3D, Tuple<Face3D, int, int>> faceSegmentDict = new Dictionary<LineSegment3D, Tuple<Face3D, int, int>>(segmentCompare);
         List<int> discardList = new List<int>();
         for (int idxF=0; idxF<inputFaceList.Count; ++ idxF)
         {
            int thisIdx = inputFaceList[idxF];
            Face3D theFace = facesColl[thisIdx];
            if (!addSegmentsToDIct(ref faceSegmentDict, ref theFace, thisIdx))
               discardList.Add(idxF);
            //for (int seg=0; seg<theFace.outerAndInnerBoundaries.Count; ++seg)
            //{
            //   faceSegmentDict.Add(theFace.outerAndInnerBoundaries[seg], new Tuple<Face3D, int, int>(theFace, thisIdx, seg));
            //}
         }
         if (discardList.Count > 0)
         {
            // There are bad faces to discard (most likely duplicate face. Do it here in reversed order to maintain validity of the index
            for (int disc = discardList.Count-1; disc == 0; --disc)
               inputFaceList.RemoveAt(disc);
         }

         while (currEdgeIdx < firstF.outerAndInnerBoundaries.Count && inputFaceList.Count > 0)
         {
            LineSegment3D currEdge = firstF.outerAndInnerBoundaries[currEdgeIdx];
            LineSegment3D reversedEdge = new LineSegment3D(firstF.outerAndInnerBoundaries[currEdgeIdx].endPoint, firstF.outerAndInnerBoundaries[currEdgeIdx].startPoint);

            //while (currFaceIdx < inputFaceList.Count && currEdgeIdx < firstF.outerAndInnerBoundaries.Count)
            //{

            //   int idx = -1;
            //   Face3D currFace = facesColl[inputFaceList[currFaceIdx]];
            //   idx = currFace.outerAndInnerBoundaries.IndexOf(reversedEdge);       // Test reversedEdge first as it is the most likely one in our data
            //   if (idx < 0)
            //   {
            //      idx = currFace.outerAndInnerBoundaries.IndexOf(firstF.outerAndInnerBoundaries[currEdgeIdx]);
            //      if (idx >= 0)
            //      {
            //         // Found match, we need to reversed the order of the data in this face
            //         currFace.Reverse();
            //         idx = currFace.outerAndInnerBoundaries.IndexOf(reversedEdge);
            //      }
            //   }
            //   if (idx < 0)
            //   {
            //      currFaceIdx++;
            //      merged = false;
            //      continue;   // not found
            //   }


            {
               Face3D currFace = null;
               int currFaceIdx = -1;
               int idx = -1;
               Tuple<Face3D, int, int> coEdgeFace = null;
               if (!faceSegmentDict.TryGetValue(reversedEdge, out coEdgeFace))
               {
                  if (!faceSegmentDict.TryGetValue(currEdge, out coEdgeFace))
                  {
                     currEdgeIdx++;
                     merged = false;
                     continue;
                  }

                  currFace = coEdgeFace.Item1;
                  currFaceIdx = coEdgeFace.Item2;

                  // CoEdge is with the same direction, need to reverse the face edges
                  for (int coe = 0; coe < currFace.outerAndInnerBoundaries.Count; ++coe)
                     faceSegmentDict.Remove(currFace.outerAndInnerBoundaries[coe]);
                  currFace.Reverse();
                  //for (int coe = 0; coe < currFace.outerAndInnerBoundaries.Count; ++coe)
                  //   faceSegmentDict.Add(currFace.outerAndInnerBoundaries[coe], new Tuple<Face3D, int, int>(currFace, currFaceIdx, coe));
                  addSegmentsToDIct(ref faceSegmentDict, ref currFace, currEdgeIdx);
                  if (!faceSegmentDict.TryGetValue(reversedEdge, out coEdgeFace))
                     if (!faceSegmentDict.TryGetValue(currEdge, out coEdgeFace))
                     {
                        currEdgeIdx++;
                        merged = false;
                        continue;
                     }
                  idx = coEdgeFace.Item3;
               }
               else
               {
                  currFace = coEdgeFace.Item1;
                  currFaceIdx = coEdgeFace.Item2;
                  idx = coEdgeFace.Item3;
               }

               // Now we need to check other edges of this face whether there is other coincide edge (this is in the case of hole(s))
               List<int> fFaceIdxList = new List<int>(); 
               List<int> currFaceIdxList = new List<int>();
               for (int ci = 0; ci < currFace.outerAndInnerBoundaries.Count; ci++)
               {
                  if (ci == idx)
                     continue;   // skip already known coincide edge
                  int ffIdx = -1;
                  LineSegment3D reL = new LineSegment3D(currFace.outerAndInnerBoundaries[ci].endPoint, currFace.outerAndInnerBoundaries[ci].startPoint);
                  ffIdx = firstF.outerAndInnerBoundaries.IndexOf(reL);
                  if (ffIdx > 0)
                  {
                     fFaceIdxList.Add(ffIdx);        // List of edges to skip when merging
                     currFaceIdxList.Add(ci);        // List of edges to skip when merging
                  }
               }

               // Now we will remove the paired edges and merge the faces
               List<LineSegment3D> newFaceEdges = new List<LineSegment3D>();
               for (int i = 0; i < currEdgeIdx; i++)
               {
                  bool toSkip = false;
                  if (fFaceIdxList.Count > 0)
                     toSkip = fFaceIdxList.Contains(i);
                  if (!toSkip)
                     newFaceEdges.Add(firstF.outerAndInnerBoundaries[i]);     // add the previous edges from the firstF faces first. This will skip the currEdge
               }

               // Add the next-in-sequence edges from the second face
               for (int i = idx + 1; i < currFace.outerAndInnerBoundaries.Count; i++)
               {
                  bool toSkip = false;
                  if (currFaceIdxList.Count > 0)
                     toSkip = currFaceIdxList.Contains(i);
                  if (!toSkip)
                     newFaceEdges.Add(currFace.outerAndInnerBoundaries[i]);
               }
               for (int i = 0; i < idx; i++)
               {
                  bool toSkip = false;
                  if (currFaceIdxList.Count > 0)
                     toSkip = currFaceIdxList.Contains(i);
                  if (!toSkip)
                     newFaceEdges.Add(currFace.outerAndInnerBoundaries[i]);
               }

               for (int i = currEdgeIdx + 1; i < firstF.outerAndInnerBoundaries.Count; i++)
               {
                  bool toSkip = false;
                  if (fFaceIdxList.Count > 0)
                     toSkip = fFaceIdxList.Contains(i);
                  if (!toSkip)
                     newFaceEdges.Add(firstF.outerAndInnerBoundaries[i]);
               }

               // Build a new face
               // Important to note that the list of edges may not be continuous if there is a whole. We need to go through the list here to identify whether there is any such
               //   discontinuity and collect the edges into their respective loops
               List<List<LineSegment3D>> loops = new List<List<LineSegment3D>>();

               List<LineSegment3D> loopEdges = new List<LineSegment3D>();
               loops.Add(loopEdges);
               for (int i = 0; i < newFaceEdges.Count; i++)
               {
                  if (i == 0)
                  {
                     loopEdges.Add(newFaceEdges[i]);
                  }
                  else
                  {
                     if (newFaceEdges[i].startPoint == newFaceEdges[i - 1].endPoint)
                        loopEdges.Add(newFaceEdges[i]);
                     else
                     {
                        // Discontinuity detected
                        loopEdges = new List<LineSegment3D>();   // start new loop
                        loops.Add(loopEdges); 
                        loopEdges.Add(newFaceEdges[i]);
                     }
                  }
               }

               List<List<LineSegment3D>> finalLoops = new List<List<LineSegment3D>>();
               {
                  while (loops.Count > 1)
                  {
                     // There are more than 1 loops, need to consolidate if there are fragments to combine due to their continuity between the fragments
                     int toDelIdx = -1;
                     for (int i = 1; i < loops.Count; i++)
                     {
                        if (loops[0][loops[0].Count - 1].endPoint == loops[i][0].startPoint)
                        {
                           // found continuity, merge the loops
                           List<LineSegment3D> newLoop = new List<LineSegment3D>(loops[0]);
                           newLoop.AddRange(loops[i]);
                           finalLoops.Add(newLoop);
                           toDelIdx = i;
                           break;
                        }
                     }
                     if (toDelIdx > 0)
                     {
                        loops.RemoveAt(toDelIdx);   // !!!! Important to remove the later member first before removing the first one 
                        loops.RemoveAt(0);
                     }
                     else
                     {
                        // No continuity found, copy the first loop to the final loop
                        List<LineSegment3D> newLoop = new List<LineSegment3D>(loops[0]);
                        finalLoops.Add(newLoop);
                        loops.RemoveAt(0);
                     }
                  }
                  if (loops.Count > 0)
                  {
                     // Add remaining list into the final loops
                     finalLoops.AddRange(loops);
                  }
               }

               if (finalLoops.Count > 1)
               {
                  // Find the largest loop and put it in the first position signifying the outer loop and the rest are the inner loops
                  int largestPerimeterIdx = 0;
                  double largestPerimeter = 0.0;
                  for (int i = 0; i < finalLoops.Count; i++)
                  {
                     double loopPerimeter = 0.0;
                     foreach (LineSegment3D line in finalLoops[i])
                        loopPerimeter += line.extent;
                     if (loopPerimeter > largestPerimeter)
                     {
                        largestPerimeter = loopPerimeter;
                        largestPerimeterIdx = i;
                     }
                  }
                  // We need to move the largest loop into the head if it is not
                  if (largestPerimeterIdx > 0)
                  {
                     List<LineSegment3D> largestLoop = new List<LineSegment3D>(finalLoops[largestPerimeterIdx]);
                     finalLoops.RemoveAt(largestPerimeterIdx);
                     finalLoops.Insert(0, largestLoop);
                  }
               }

               // Collect the vertices from the list of Edges into list of list of vertices starting with the outer loop (largest loop) following the finalLoop
               List<List<Point3D>> newFaceVertsLoops = new List<List<Point3D>>();
               foreach (List<LineSegment3D> loop in finalLoops)
               {
                  List<Point3D> newFaceVerts = new List<Point3D>();
                  for (int i = 0; i < loop.Count; i++)
                  {
                     if (i == 0)
                     {
                        newFaceVerts.Add(loop[i].startPoint);
                        newFaceVerts.Add(loop[i].endPoint);
                     }
                     else if (i == loop.Count - 1)   // Last
                     {
                        // Add nothing as the last segment ends at the first vertex
                     }
                     else
                     {
                        newFaceVerts.Add(loop[i].endPoint);
                     }
                  }
                  // close the loop with end point from the starting point (it is important to mark the end of loop and if there is any other vertex follow, they start the inner loop)
                  if (newFaceVerts.Count > 0)
                  {
                     if (newFaceVerts[0] != newFaceVerts[newFaceVerts.Count-1])
                     {
                        // If the end vertex is not the same as the start vertex, add the first vertex to the end vertex
                        newFaceVerts.Add(newFaceVerts[0]);
                     }
                     newFaceVertsLoops.Add(newFaceVerts);
                  }
               }

               // Validate the resulting face, skip if not valid
               if (!Face3D.validateFace(newFaceVertsLoops))
               {
                  inputFaceList.RemoveAt(0);  // remove the first face from the list to advance to the next face
                  currEdgeIdx = 0;
                  merged = false;
                  break;
               }

               // This new merged face will override/replace the original firstF for the next round
               firstF = new Face3D(newFaceVertsLoops);

               // Reset currEdgeIdx since the first face has been replaced
               currEdgeIdx = 0;
               reversedEdge = new LineSegment3D(firstF.outerAndInnerBoundaries[currEdgeIdx].endPoint, firstF.outerAndInnerBoundaries[currEdgeIdx].startPoint);

               //mergedFacesIdxList.Add(inputFaceList[currFaceIdx]);
               //inputFaceList.RemoveAt(currFaceIdx);
               //currFaceIdx = 0;
               mergedFacesIdxList.Add(currFaceIdx);
               inputFaceList.Remove(currFaceIdx);

               // Remove merged face from edge dict
               IList<LineSegment3D> rem = facesColl[currFaceIdx].outerAndInnerBoundaries;
               for (int coe = 0; coe < rem.Count; ++coe)
                  faceSegmentDict.Remove(rem[coe]);

               merged = true;
            }

            if (!merged)
            {
               currEdgeIdx++;
            }
            if (merged || currEdgeIdx == firstF.outerAndInnerBoundaries.Count)
            {
               int lastFaceID = facesColl.Count;      // The new face ID is always at the end of the list

               facesColl.Add(lastFaceID, firstF);
               //prevFirstFIdx = lastFaceID;

               // Add the face into output list when there is no more face to process
               if (inputFaceList.Count == 0)
                  outputFaceList.Add(lastFaceID);

               // Now loop through all the dictionary of the sortedVert and replace all merged face indexes with the new one
               foreach (KeyValuePair<Point3D, HashSet<int>> v in sortedFVert)
               {
                  HashSet<int> fIndexes = v.Value;
                  bool replaced = false;
                  foreach(int Idx in mergedFacesIdxList)
                  {
                     replaced |= fIndexes.Remove(Idx);
                     _mergedFaceList.Remove(Idx);        // Remove the idx face also from _mergeFaceList as some faces might be left unmerged in the previous step(s)
                  }
                  if (replaced)
                     fIndexes.Add(lastFaceID);   // replace the merged face indexes with the new merged face index
               }

               if (inputFaceList.Count > 0)
               {
                  firstF = facesColl[inputFaceList[0]];
                  mergedFacesIdxList.Clear();
                  mergedFacesIdxList.Add(inputFaceList[0]);

                  // Remove segments of this face from the segment dict
                  IList<LineSegment3D> rem = firstF.outerAndInnerBoundaries;
                  for (int coe = 0; coe < rem.Count; ++coe)
                     faceSegmentDict.Remove(rem[coe]);

                  inputFaceList.RemoveAt(0);  // remove the first face from the list
                  currEdgeIdx = 0;
                  merged = false;

                  // If there is still face to process, add the new face at the end of the currently process face list
                  inputFaceList.Add(lastFaceID);
                  Face3D face = facesColl[lastFaceID];
                  addSegmentsToDIct(ref faceSegmentDict, ref face, lastFaceID);
               }
            }
         }

         return merged;
      }

      bool addSegmentsToDIct(ref IDictionary<LineSegment3D, Tuple<Face3D, int, int>> faceSegmentDict, ref Face3D theFace, int faceIdx)
      {
         IList<LineSegment3D> addFace = theFace.outerAndInnerBoundaries;
         // The Dictionary Add might fail because there may be coEdge that is in the same direction. In this case the face needs to be reversed
         try
         {
            for (int coe = 0; coe < addFace.Count; ++coe)
               faceSegmentDict.Add(addFace[coe], new Tuple<Face3D, int, int>(theFace, faceIdx, coe));
            return true;
         }
         catch
         { }

         // Remove segments that are already successfully added for this face before reversing
         for (int coe = 0; coe < addFace.Count; ++coe)
            faceSegmentDict.Remove(addFace[coe]);

         Face3D theFaceRev = new Face3D(theFace.verticesWithHoles);
         theFace.Reverse();
         addFace = theFace.outerAndInnerBoundaries;

         try
         {
            for (int coe = 0; coe < addFace.Count; ++coe)
               faceSegmentDict.Add(addFace[coe], new Tuple<Face3D, int, int>(theFace, faceIdx, coe));
            return true;
         }
         catch
         {
            // Still unable to insert even after it is reversed. SOmething is not right. The face will not be added into the Dict
         }
         return false;
      }

      int findMatechedIndexSegment(Dictionary<LineSegment3D, int> segDict, LineSegment3D inpSeg)
      {
         int idx;
         if (segDict.TryGetValue(inpSeg, out idx))
            return idx;
         else
            return -1;
      }

      //bool tryMergeFacesUsingDict(List<int> inputFaceList, out List<int> outputFaceList)
      //{
      //   outputFaceList = new List<int>();
      //   Face3D firstF = facesColl[inputFaceList[0]];
      //   int prevFirstFIdx = 0;
      //   HashSet<int> mergedFacesIdxList = new HashSet<int>();
      //   mergedFacesIdxList.Add(inputFaceList[0]);

      //   inputFaceList.RemoveAt(0);  // remove the first face from the list
      //   int currEdgeIdx = 0;
      //   bool merged = false;

      //   while (currEdgeIdx < firstF.outerAndInnerBoundaries.Count && inputFaceList.Count > 0)
      //   {
      //      LineSegment3D reversedEdge = new LineSegment3D(firstF.outerAndInnerBoundaries[currEdgeIdx].endPoint, firstF.outerAndInnerBoundaries[currEdgeIdx].startPoint);
      //      int currFaceIdx = 0;
      //      while (currFaceIdx < inputFaceList.Count && currEdgeIdx < firstF.outerAndInnerBoundaries.Count)
      //      {

      //         //int idx = -1;
      //         Face3D currFace = facesColl[inputFaceList[currFaceIdx]];
      //         //idx = currFace.outerAndInnerBoundaries.IndexOf(reversedEdge);       // Test reversedEdge first as it is the most likely one in our data
      //         int idx = findMatechedIndexSegment(currFace.outerAndInnerBoundariesWithDict, reversedEdge);
      //         if (idx < 0)
      //         {
      //            //idx = currFace.outerAndInnerBoundaries.IndexOf(firstF.outerAndInnerBoundaries[currEdgeIdx]);
      //            idx = findMatechedIndexSegment(currFace.outerAndInnerBoundariesWithDict, firstF.outerAndInnerBoundaries[currEdgeIdx]);
      //            if (idx >= 0)
      //            {
      //               // Found match, we need to reversed the order of the data in this face
      //               currFace.Reverse();
      //               //idx = currFace.outerAndInnerBoundaries.IndexOf(reversedEdge);
      //               idx = findMatechedIndexSegment(currFace.outerAndInnerBoundariesWithDict, reversedEdge);
      //            }
      //         }
      //         if (idx < 0)
      //         {
      //            currFaceIdx++;
      //            merged = false;
      //            continue;   // not found
      //         }

      //         // Now we need to check other edges of this face whether there is other coincide edge (this is in the case of hole(s))
      //         List<int> fFaceIdxList = new List<int>();
      //         List<int> currFaceIdxList = new List<int>();
      //         for (int ci = 0; ci < currFace.outerAndInnerBoundaries.Count; ci++)
      //         {
      //            if (ci == idx)
      //               continue;   // skip already known coincide edge
      //            int ffIdx = -1;
      //            LineSegment3D reL = new LineSegment3D(currFace.outerAndInnerBoundaries[ci].endPoint, currFace.outerAndInnerBoundaries[ci].startPoint);
      //            //ffIdx = firstF.outerAndInnerBoundaries.IndexOf(reL);
      //            ffIdx = findMatechedIndexSegment(firstF.outerAndInnerBoundariesWithDict, reL);
      //            if (ffIdx > 0)
      //            {
      //               fFaceIdxList.Add(ffIdx);        // List of edges to skip when merging
      //               currFaceIdxList.Add(ci);        // List of edges to skip when merging
      //            }
      //         }

      //         // Now we will remove the paired edges and merge the faces
      //         List<LineSegment3D> newFaceEdges = new List<LineSegment3D>();
      //         for (int i = 0; i < currEdgeIdx; i++)
      //         {
      //            bool toSkip = false;
      //            if (fFaceIdxList.Count > 0)
      //               toSkip = fFaceIdxList.Contains(i);
      //            if (!toSkip)
      //               newFaceEdges.Add(firstF.outerAndInnerBoundaries[i]);     // add the previous edges from the firstF faces first. This will skip the currEdge
      //         }

      //         // Add the next-in-sequence edges from the second face
      //         for (int i = idx + 1; i < currFace.outerAndInnerBoundaries.Count; i++)
      //         {
      //            bool toSkip = false;
      //            if (currFaceIdxList.Count > 0)
      //               toSkip = currFaceIdxList.Contains(i);
      //            if (!toSkip)
      //               newFaceEdges.Add(currFace.outerAndInnerBoundaries[i]);
      //         }
      //         for (int i = 0; i < idx; i++)
      //         {
      //            bool toSkip = false;
      //            if (currFaceIdxList.Count > 0)
      //               toSkip = currFaceIdxList.Contains(i);
      //            if (!toSkip)
      //               newFaceEdges.Add(currFace.outerAndInnerBoundaries[i]);
      //         }

      //         for (int i = currEdgeIdx + 1; i < firstF.outerAndInnerBoundaries.Count; i++)
      //         {
      //            bool toSkip = false;
      //            if (fFaceIdxList.Count > 0)
      //               toSkip = fFaceIdxList.Contains(i);
      //            if (!toSkip)
      //               newFaceEdges.Add(firstF.outerAndInnerBoundaries[i]);
      //         }

      //         // Build a new face
      //         // Important to note that the list of edges may not be continuous if there is a whole. We need to go through the list here to identify whether there is any such
      //         //   discontinuity and collect the edges into their respective loops
      //         List<List<LineSegment3D>> loops = new List<List<LineSegment3D>>();

      //         List<LineSegment3D> loopEdges = new List<LineSegment3D>();
      //         loops.Add(loopEdges);
      //         for (int i = 0; i < newFaceEdges.Count; i++)
      //         {
      //            if (i == 0)
      //            {
      //               loopEdges.Add(newFaceEdges[i]);
      //            }
      //            else
      //            {
      //               if (newFaceEdges[i].startPoint == newFaceEdges[i - 1].endPoint)
      //                  loopEdges.Add(newFaceEdges[i]);
      //               else
      //               {
      //                  // Discontinuity detected
      //                  loopEdges = new List<LineSegment3D>();   // start new loop
      //                  loops.Add(loopEdges);
      //                  loopEdges.Add(newFaceEdges[i]);
      //               }
      //            }
      //         }

      //         List<List<LineSegment3D>> finalLoops = new List<List<LineSegment3D>>();
      //         {
      //            while (loops.Count > 1)
      //            {
      //               // There are more than 1 loops, need to consolidate if there are fragments to combine due to their continuity between the fragments
      //               int toDelIdx = -1;
      //               for (int i = 1; i < loops.Count; i++)
      //               {
      //                  if (loops[0][loops[0].Count - 1].endPoint == loops[i][0].startPoint)
      //                  {
      //                     // found continuity, merge the loops
      //                     List<LineSegment3D> newLoop = new List<LineSegment3D>(loops[0]);
      //                     newLoop.AddRange(loops[i]);
      //                     finalLoops.Add(newLoop);
      //                     toDelIdx = i;
      //                     break;
      //                  }
      //               }
      //               if (toDelIdx > 0)
      //               {
      //                  loops.RemoveAt(toDelIdx);   // !!!! Important to remove the later member first before removing the first one 
      //                  loops.RemoveAt(0);
      //               }
      //               else
      //               {
      //                  // No continuity found, copy the firs loop to the final loop
      //                  List<LineSegment3D> newLoop = new List<LineSegment3D>(loops[0]);
      //                  finalLoops.Add(newLoop);
      //                  loops.RemoveAt(0);
      //               }
      //            }
      //            if (loops.Count > 0)
      //            {
      //               // Add remaining list into the final loops
      //               finalLoops.AddRange(loops);
      //            }
      //         }

      //         if (finalLoops.Count > 1)
      //         {
      //            // Find the largest loop and put it in the first position signifying the outer loop and the rest are the inner loops
      //            int largestPerimeterIdx = 0;
      //            double largestPerimeter = 0.0;
      //            for (int i = 0; i < finalLoops.Count; i++)
      //            {
      //               double loopPerimeter = 0.0;
      //               foreach (LineSegment3D line in finalLoops[i])
      //                  loopPerimeter += line.extent;
      //               if (loopPerimeter > largestPerimeter)
      //               {
      //                  largestPerimeter = loopPerimeter;
      //                  largestPerimeterIdx = i;
      //               }
      //            }
      //            // We need to move the largest loop into the head if it is not
      //            if (largestPerimeterIdx > 0)
      //            {
      //               List<LineSegment3D> largestLoop = new List<LineSegment3D>(finalLoops[largestPerimeterIdx]);
      //               finalLoops.RemoveAt(largestPerimeterIdx);
      //               finalLoops.Insert(0, largestLoop);
      //            }
      //         }

      //         // Collect the vertices from the list of Edges into list of list of vertices starting with the outer loop (largest loop) following the finalLoop
      //         List<List<Point3D>> newFaceVertsLoops = new List<List<Point3D>>();
      //         foreach (List<LineSegment3D> loop in finalLoops)
      //         {
      //            List<Point3D> newFaceVerts = new List<Point3D>();
      //            for (int i = 0; i < loop.Count; i++)
      //            {
      //               if (i == 0)
      //               {
      //                  newFaceVerts.Add(loop[i].startPoint);
      //                  newFaceVerts.Add(loop[i].endPoint);
      //               }
      //               else if (i == loop.Count - 1)   // Last
      //               {
      //                  // Add nothing as the last segment ends at the first vertex
      //               }
      //               else
      //               {
      //                  newFaceVerts.Add(loop[i].endPoint);
      //               }
      //            }
      //            // close the loop with end point from the starting point (it is important to mark the end of loop and if there is any other vertex follow, they start the inner loop)
      //            if (newFaceVerts.Count > 0)
      //            {
      //               if (newFaceVerts[0] != newFaceVerts[newFaceVerts.Count - 1])
      //               {
      //                  // If the end vertex is not the same as the start vertex, add the first vertex to the end vertex
      //                  newFaceVerts.Add(newFaceVerts[0]);
      //               }
      //               newFaceVertsLoops.Add(newFaceVerts);
      //            }
      //         }

      //         // Validate the resulting face, skip if not valid
      //         if (!Face3D.validateFace(newFaceVertsLoops))
      //         {
      //            inputFaceList.RemoveAt(0);  // remove the first face from the list to advance to the next face
      //            currEdgeIdx = 0;
      //            merged = false;
      //            break;
      //         }

      //         // This new merged face will override/replace the original firstF for the next round
      //         firstF = new Face3D(newFaceVertsLoops);
      //         currEdgeIdx = 0;
      //         reversedEdge = new LineSegment3D(firstF.outerAndInnerBoundaries[currEdgeIdx].endPoint, firstF.outerAndInnerBoundaries[currEdgeIdx].startPoint);

      //         mergedFacesIdxList.Add(inputFaceList[currFaceIdx]);
      //         inputFaceList.RemoveAt(currFaceIdx);
      //         currFaceIdx = 0;
      //         merged = true;
      //      }

      //      if (!merged)
      //      {
      //         currEdgeIdx++;
      //      }
      //      if (merged || currEdgeIdx == firstF.outerAndInnerBoundaries.Count)
      //      {
      //         facesColl.Add(lastFaceID, firstF);
      //         prevFirstFIdx = lastFaceID;
      //         outputFaceList.Add(lastFaceID);

      //         // Now loop through all the dictionary of the sortedVert and replace all merged face indexes with the new one
      //         foreach (KeyValuePair<Point3D, HashSet<int>> v in sortedFVert)
      //         {
      //            HashSet<int> fIndexes = v.Value;
      //            bool replaced = false;
      //            foreach (int Idx in mergedFacesIdxList)
      //            {
      //               replaced |= fIndexes.Remove(Idx);
      //               _mergedFaceList.Remove(Idx);        // Remove the idx face also from _mergeFaceList as some faces might be left unmerged in the previous step(s)
      //                                                   // remove also prev firstF
      //                                                   //fIndexes.Remove(prevFirstFIdx);
      //                                                   //_mergedFaceList.Remove(prevFirstFIdx);
      //                                                   //outputFaceList.Remove(prevFirstFIdx);
      //            }
      //            if (replaced)
      //               fIndexes.Add(lastFaceID);   // replace the merged face indexes with the new merged face index
      //         }

      //         lastFaceID++;
      //         if (inputFaceList.Count > 0)
      //         {
      //            firstF = facesColl[inputFaceList[0]];
      //            mergedFacesIdxList.Clear();
      //            mergedFacesIdxList.Add(inputFaceList[0]);
      //            inputFaceList.RemoveAt(0);  // remove the first face from the list
      //            currEdgeIdx = 0;
      //            merged = false;
      //         }
      //      }
      //   }

      //   return merged;
      //}

      public bool insertIntoDB(bool forUserDict)
      {
#if ORACLE
         List<string> arrElementID = new List<string>();
         List<string> arrFaceID = new List<string>();
         List<string> arrType = new List<string>();
         List<SdoGeometry> arrFaceGeom = new List<SdoGeometry>();
         List<SdoGeometry> arrNormal = new List<SdoGeometry>();
         List<double> arrAngle = new List<double>();
         List<SdoGeometry> arrCentroid = new List<SdoGeometry>();
         List<double> arrArea = new List<double>();

         string sqlStmt;
         if (forUserDict)
               sqlStmt = "INSERT INTO USERGEOM_TOPO_FACE (ELEMENTID, ID, TYPE, POLYGON, NORMAL, ANGLEFROMNORTH, CENTROID, AREA) "
                           + "VALUES (:1, :2, :3, :4, :5, :6, :7, :8)";
         else
               sqlStmt = "INSERT INTO " + DBOperation.formatTabName("BIMRL_TOPO_FACE") + "(ELEMENTID, ID, TYPE, POLYGON, NORMAL, ANGLEFROMNORTH, CENTROID, AREA) "
                           + "VALUES (:1, :2, :3, :4, :5, :6, :7, :8)";
         OracleCommand cmd = new OracleCommand(sqlStmt, DBOperation.DBConn);
         OracleParameter[] Params = new OracleParameter[8];
            
         Params[0] = cmd.Parameters.Add("1", OracleDbType.Varchar2);
         Params[1] = cmd.Parameters.Add("2", OracleDbType.Varchar2);
         Params[2] = cmd.Parameters.Add("3", OracleDbType.Varchar2);
         Params[3] = cmd.Parameters.Add("4", OracleDbType.Object);
         Params[3].UdtTypeName = "MDSYS.SDO_GEOMETRY";
         Params[4] = cmd.Parameters.Add("5", OracleDbType.Object);
         Params[4].UdtTypeName = "MDSYS.SDO_GEOMETRY";
         Params[5] = cmd.Parameters.Add("6", OracleDbType.Double);
         Params[6] = cmd.Parameters.Add("7", OracleDbType.Object);
         Params[6].UdtTypeName = "MDSYS.SDO_GEOMETRY";
         Params[7] = cmd.Parameters.Add("7", OracleDbType.Double);
         for (int i = 0; i < 8; i++)
               Params[i].Direction = ParameterDirection.Input;
#endif
#if POSTGRES
         string sqlStmt;
         if (forUserDict)
            sqlStmt = "INSERT INTO USERGEOM_TOPO_FACE (ELEMENTID, ID, TYPE, POLYGON, NORMAL, ANGLEFROMNORTH, CENTROID, AREA) "
                        + "VALUES (@eid, @id, @ftyp, @polyg, @norm, @angle, @cent, @area)";
         else
            sqlStmt = "INSERT INTO " + DBOperation.formatTabName("BIMRL_TOPO_FACE") + "(ELEMENTID, ID, TYPE, POLYGON, NORMAL, ANGLEFROMNORTH, CENTROID, AREA) "
                        + "VALUES (@eid, @id, @ftyp, @polyg, @norm, @angle, @cent, @area)";
         NpgsqlConnection arbConn = DBOperation.arbitraryConnection();
         NpgsqlTransaction arbTrans = arbConn.BeginTransaction();
         NpgsqlCommand cmd = new NpgsqlCommand(sqlStmt, arbConn);

         // Npgsql has problem with Prepare() for Composite type. It insists that the specific type has to be specified when the composite may mean many different items and types
         // Use AddWithValue instead without the need to specify the type explicitly
         //cmd.Parameters.Add("@eid", NpgsqlDbType.Varchar);
         //cmd.Parameters.Add("@id", NpgsqlDbType.Varchar);
         //cmd.Parameters.Add("@ftyp", NpgsqlDbType.Varchar);
         //cmd.Parameters.Add("@norm", NpgsqlDbType.Composite | NpgsqlDbType.Double);
         //cmd.Parameters.Add("@angle", NpgsqlDbType.Double);
         //cmd.Parameters.Add("@cent", NpgsqlDbType.Composite | NpgsqlDbType.Double);
         //cmd.Parameters.Add("@polyg", NpgsqlDbType.Jsonb);
         //cmd.Prepare();

         int insRec = 0;
#endif
         foreach (int fIdx in _mergedFaceList)
         {
            // For the main table, we want to ensure that the face ID is not-overlapped
            int faceID = fIdx;
            if (!forUserDict)
               faceID = fIdx + _fIDOffset;

            Face3D face = facesColl[fIdx];
            if (!Face3D.validateFace(face.verticesWithHoles) || !Face3D.validateFace(face))
            {
               // In some cases for unknown reason, we may get a face with all vertices aligned in a straight line, resulting in invalid normal vector
               // for this case, we will simply skip the face
               _refBIMRLCommon.StackPushError("%Warning (ElementID: '" + _elementid + "'): Skipping face#  " + fIdx.ToString() + " because it is an invalid face!");
               continue;
            }

#if ORACLE
            SdoGeometry normal = new SdoGeometry();
            normal.Dimensionality = 3;
            normal.LRS = 0;
            normal.GeometryType = (int)SdoGeometryTypes.GTYPE.POINT;
            int gType = normal.PropertiesToGTYPE();

            SdoPoint normalP = new SdoPoint();
            normalP.XD = face.basePlane.normalVector.X;
            normalP.YD = face.basePlane.normalVector.Y;
            normalP.ZD = face.basePlane.normalVector.Z;
            normal.SdoPoint = normalP;

            arrNormal.Add(normal);
            arrElementID.Add(_elementid);

            arrFaceID.Add(faceID.ToString());
            arrType.Add(_faceCategory);

            Vector3D normal2D = new Vector3D(normalP.XD.Value, normalP.YD.Value, 0.0);
            double angleRad = Math.Atan2(normal2D.Y, normal2D.X) - Math.Atan2(_trueNorth.Y, _trueNorth.X);
            arrAngle.Add(angleRad);

            SdoGeometry centroid = new SdoGeometry();
            centroid.Dimensionality = 3;
            centroid.LRS = 0;
            centroid.GeometryType = (int)SdoGeometryTypes.GTYPE.POINT;
            gType = centroid.PropertiesToGTYPE();

            SdoPoint centroidP = new SdoPoint();
            centroidP.XD = face.boundingBox.Center.X;
            centroidP.YD = face.boundingBox.Center.Y;
            centroidP.ZD = face.boundingBox.Center.Z;
            centroid.SdoPoint = centroidP;

            arrCentroid.Add(centroid);
            arrArea.Add(face.Area);

            List<double> arrCoord = new List<double>();
            List<int> elemInfoArr = new List<int>();

            SdoGeometry sdoGeomData = new SdoGeometry();
            sdoGeomData.Dimensionality = 3;
            sdoGeomData.LRS = 0;
            sdoGeomData.GeometryType = (int)SdoGeometryTypes.GTYPE.POLYGON;
            gType = sdoGeomData.PropertiesToGTYPE();

            int noVerts = face.vertices.Count;
            foreach (Point3D v in face.vertices)
            {
               arrCoord.Add(v.X);
               arrCoord.Add(v.Y);
               arrCoord.Add(v.Z);
            }
            if (face.vertices[0] != face.vertices[noVerts - 1])
            {
               // If the end vertex is not the same as the first, add it to close for SDO
               arrCoord.Add(face.vertices[0].X);
               arrCoord.Add(face.vertices[0].Y);
               arrCoord.Add(face.vertices[0].Z);
               noVerts++;
            }
            elemInfoArr.Add(1);  // starting position
            elemInfoArr.Add((int)SdoGeometryTypes.ETYPE_SIMPLE.POLYGON_EXTERIOR);
            elemInfoArr.Add(1);

            // Add holes
            // For every hole, we will also create an independent face for the hole in addition of being a hole to the main face
            List<SdoGeometry> holeFaces = new List<SdoGeometry>();
            List<Point3D> holeCentroids = new List<Point3D>();
            List<double> holeArea = new List<double>();
            if (face.verticesWithHoles.Count > 1)
            {
               for (int i = 1; i < face.verticesWithHoles.Count; i++)
               {
                  SdoGeometry hole = new SdoGeometry();
                  hole.Dimensionality = 3;
                  hole.LRS = 0;
                  hole.GeometryType = (int)SdoGeometryTypes.GTYPE.POLYGON;
                  int holeGType = hole.PropertiesToGTYPE();
                  List<double> arrHoleCoord = new List<double>();
                  List<int> holeInfoArr = new List<int>();
                  holeInfoArr.Add(1);
                  holeInfoArr.Add((int)SdoGeometryTypes.ETYPE_SIMPLE.POLYGON_EXTERIOR);
                  holeInfoArr.Add(1);
                  BoundingBox3D holeBB = new BoundingBox3D(face.verticesWithHoles[i]);
                  holeCentroids.Add(holeBB.Center);

                  elemInfoArr.Add(noVerts * 3 + 1);  // starting position
                  elemInfoArr.Add((int)SdoGeometryTypes.ETYPE_SIMPLE.POLYGON_INTERIOR);
                  elemInfoArr.Add(1);
                  foreach (Point3D v in face.verticesWithHoles[i])
                  {
                     arrCoord.Add(v.X);
                     arrCoord.Add(v.Y);
                     arrCoord.Add(v.Z);

                     arrHoleCoord.Add(v.X);
                     arrHoleCoord.Add(v.Y);
                     arrHoleCoord.Add(v.Z);
                     noVerts++;
                  }
                  if (face.verticesWithHoles[i][0] != face.verticesWithHoles[i][face.verticesWithHoles[i].Count - 1])
                  {
                     // If the end vertex is not the same as the first, add it to close for SDO
                     arrCoord.Add(face.verticesWithHoles[i][0].X);
                     arrCoord.Add(face.verticesWithHoles[i][0].Y);
                     arrCoord.Add(face.verticesWithHoles[i][0].Z);
                     arrHoleCoord.Add(face.verticesWithHoles[i][0].X);
                     arrHoleCoord.Add(face.verticesWithHoles[i][0].Y);
                     arrHoleCoord.Add(face.verticesWithHoles[i][0].Z);
                     noVerts++;
                  }
                  hole.ElemArrayOfInts = holeInfoArr.ToArray();
                  hole.OrdinatesArrayOfDoubles = arrHoleCoord.ToArray();
                  holeFaces.Add(hole);
                  Face3D fHole = new Face3D(face.verticesWithHoles[i]);
                  holeArea.Add(fHole.Area);
               }
            }

            sdoGeomData.ElemArrayOfInts = elemInfoArr.ToArray();
            sdoGeomData.OrdinatesArrayOfDoubles = arrCoord.ToArray();
            arrFaceGeom.Add(sdoGeomData);

            // add entry(ies) for holes
            int noHole = 0;
            foreach(SdoGeometry geom in holeFaces)
            {
               arrElementID.Add(_elementid);
               arrFaceID.Add(fIdx.ToString() + "-" + noHole.ToString());

               arrType.Add("HOLE");    // special category for HOLE
               arrNormal.Add(normal);  // follow the normal of the main face
               arrAngle.Add(angleRad);

               SdoGeometry holeCentroid = new SdoGeometry();
               holeCentroid.Dimensionality = 3;
               holeCentroid.LRS = 0;
               holeCentroid.GeometryType = (int)SdoGeometryTypes.GTYPE.POINT;
               gType = holeCentroid.PropertiesToGTYPE();

               SdoPoint holeCentroidP = new SdoPoint();
               holeCentroidP.XD = holeCentroids[noHole].X;
               holeCentroidP.YD = holeCentroids[noHole].Y;
               holeCentroidP.ZD = holeCentroids[noHole].Z;
               holeCentroid.SdoPoint = holeCentroidP;
               arrCentroid.Add(holeCentroid);
               
               arrFaceGeom.Add(holeFaces[noHole]);
               noHole++;
            }

            if (arrElementID.Count >= DBOperation.commitInterval)
            {
               Params[0].Value = arrElementID.ToArray();
               Params[0].Size = arrElementID.Count;
               Params[1].Value = arrFaceID.ToArray();
               Params[1].Size = arrFaceID.Count;
               Params[2].Value = arrType.ToArray();
               Params[2].Size = arrType.Count;
               Params[3].Value = arrFaceGeom.ToArray();
               Params[3].Size = arrFaceGeom.Count;
               Params[4].Value = arrNormal.ToArray();
               Params[4].Size = arrNormal.Count;
               Params[5].Value = arrAngle.ToArray();
               Params[5].Size = arrAngle.Count;
               Params[6].Value = arrCentroid.ToArray();
               Params[6].Size = arrCentroid.Count;

               try
               {
                  cmd.ArrayBindCount = arrElementID.Count;    // No of values in the array to be inserted
                  int commandStatus = cmd.ExecuteNonQuery();
                  DBOperation.commitTransaction();
                  arrElementID.Clear();
                  arrFaceID.Clear();
                  arrType.Clear();
                  arrFaceGeom.Clear();
                  arrNormal.Clear();
                  arrAngle.Clear();
                  arrCentroid.Clear();
               }
               catch (OracleException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + sqlStmt;
                  _refBIMRLCommon.StackPushError(excStr);
                  arrElementID.Clear();
                  arrFaceID.Clear();
                  arrType.Clear();
                  arrFaceGeom.Clear();
                  arrNormal.Clear();
                  arrAngle.Clear();
                  arrCentroid.Clear();
                  continue;
               }
               catch (SystemException e)
               {
                  string excStr = "%%Insert Error - " + e.Message + "\n\t" + sqlStmt;
                  _refBIMRLCommon.StackPushError(excStr);
                  throw;
               }
            }
#endif
#if POSTGRES
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("@eid", _elementid);
            cmd.Parameters.AddWithValue("@id", faceID.ToString());
            cmd.Parameters.AddWithValue("@ftyp", _faceCategory);

            //Point3D normal = new Point3D(face.basePlane.normalVector.X, face.basePlane.normalVector.Y, face.basePlane.normalVector.Z);
            Point3D normal = face.basePlane.normalVector.ToPoint3D();
            cmd.Parameters.AddWithValue("@norm", normal);
            Vector3D normal2D = new Vector3D(normal.X, normal.Y, 0.0);
            double angleRad = Math.Atan2(normal2D.Y, normal2D.X) - Math.Atan2(_trueNorth.Y, _trueNorth.X);
            cmd.Parameters.AddWithValue("@angle", angleRad);

            cmd.Parameters.AddWithValue("@cent", face.boundingBox.Center);

            string polygonStr = JsonConvert.SerializeObject(face);
            cmd.Parameters.AddWithValue("@polyg", NpgsqlDbType.Jsonb, polygonStr);
            cmd.Parameters.AddWithValue("@area", NpgsqlDbType.Double, face.Area);

            try
            {
               int commandStatus = cmd.ExecuteNonQuery();
               insRec++;

               // Add holes
               // For every hole, we will also create an independent face for the hole in addition of being a hole to the main face
               if (face.verticesWithHoles.Count > 1)
               {
                  List<Face3D> holeFaces = new List<Face3D>();
                  List<Point3D> holeCentroids = new List<Point3D>();
                  for (int i = 1; i < face.verticesWithHoles.Count; i++)
                  {
                     List<Point3D> holeVerts = face.verticesWithHoles[i].ToList();
                     holeVerts.Reverse();          // Reverse it to get the positive normal direction
                     Face3D holeFace = new Face3D(holeVerts);

                     cmd.Parameters.Clear();
                     cmd.Parameters.AddWithValue("@eid", _elementid);
                     cmd.Parameters.AddWithValue("@id", faceID.ToString() + "-" + (i-1).ToString());
                     cmd.Parameters.AddWithValue("@ftyp", "HOLE");
                     normal = holeFace.basePlane.normalVector.ToPoint3D();
                     cmd.Parameters.AddWithValue("@norm", normal);
                     normal2D = new Vector3D(normal.X, normal.Y, 0.0);
                     angleRad = Math.Atan2(normal2D.Y, normal2D.X) - Math.Atan2(_trueNorth.Y, _trueNorth.X);
                     cmd.Parameters.AddWithValue("@angle", angleRad);
                     cmd.Parameters.AddWithValue("@cent", holeFace.boundingBox.Center);

                     polygonStr = JsonConvert.SerializeObject(holeFace);
                     cmd.Parameters.AddWithValue("@polyg", NpgsqlDbType.Jsonb, polygonStr);
                     cmd.Parameters.AddWithValue("@area", NpgsqlDbType.Double, holeFace.Area);
                     commandStatus = cmd.ExecuteNonQuery();
                     insRec++;
                  }
               }

               if (insRec > DBOperation.commitInterval)
               {
                  arbTrans.Commit();
                  arbTrans = arbConn.BeginTransaction();
                  insRec = 0;
               }
            }
            catch (NpgsqlException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + sqlStmt;
               _refBIMRLCommon.StackPushError(excStr);
               continue;
            }
            catch (SystemException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + sqlStmt;
               _refBIMRLCommon.StackPushError(excStr);
               throw;
            }
         }
         if (insRec > 0)
            arbTrans.Commit();
         cmd.Dispose();
         arbConn.Close();
#endif
#if ORACLE
         }
         if (arrElementID.Count > 0)
         {

            Params[0].Value = arrElementID.ToArray();
            Params[0].Size = arrElementID.Count;
            Params[1].Value = arrFaceID.ToArray();
            Params[1].Size = arrFaceID.Count;
            Params[2].Value = arrType.ToArray();
            Params[2].Size = arrType.Count;
            Params[3].Value = arrFaceGeom.ToArray();
            Params[3].Size = arrFaceGeom.Count;
            Params[4].Value = arrNormal.ToArray();
            Params[4].Size = arrNormal.Count;
            Params[5].Value = arrAngle.ToArray();
            Params[5].Size = arrAngle.Count;
            Params[6].Value = arrCentroid.ToArray();
            Params[6].Size = arrCentroid.Count;

            try
            {
               cmd.ArrayBindCount = arrElementID.Count;    // No of values in the array to be inserted
               int commandStatus = cmd.ExecuteNonQuery();
               DBOperation.commitTransaction();
            }
            catch (OracleException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + sqlStmt;
               _refBIMRLCommon.StackPushIgnorableError(excStr);
               // Ignore any error
            }
            catch (SystemException e)
            {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + sqlStmt;
               _refBIMRLCommon.StackPushError(excStr);
               throw;
            }
         }

         DBOperation.commitTransaction();
         cmd.Dispose();
#endif
         return true;
      }

      public void deriveMajorAxes()
      {
         PrincipalComponentAnalysis pca = new PrincipalComponentAnalysis(_geom);
         majorAxes = pca.identifyMajorAxes();
         Point3D centroid = pca.Centroid;
         List<Point3D> OBBVerts = pca.OBBVertices;

#if ORACLE
         // Update BIMRL_ELEMENT table
         string sqlStmt = "UPDATE " + DBOperation.formatTabName("BIMRL_ELEMENT") + " SET BODY_MAJOR_AXIS_CENTROID=:1, BODY_MAJOR_AXIS1=:2, BODY_MAJOR_AXIS2=:3, BODY_MAJOR_AXIS3=:4, OBB=:5 WHERE ELEMENTID='" + _elementid + "'";
         OracleCommand cmd = new OracleCommand(sqlStmt, DBOperation.DBConn);
         OracleParameter[] Params = new OracleParameter[5];

         Params[0] = cmd.Parameters.Add("1", OracleDbType.Object);
         Params[0].UdtTypeName = "MDSYS.SDO_GEOMETRY";
         Params[1] = cmd.Parameters.Add("2", OracleDbType.Object);
         Params[1].UdtTypeName = "MDSYS.SDO_GEOMETRY";
         Params[2] = cmd.Parameters.Add("3", OracleDbType.Object);
         Params[2].UdtTypeName = "MDSYS.SDO_GEOMETRY";
         Params[3] = cmd.Parameters.Add("4", OracleDbType.Object);
         Params[3].UdtTypeName = "MDSYS.SDO_GEOMETRY";
         Params[4] = cmd.Parameters.Add("5", OracleDbType.Object);
         Params[4].UdtTypeName = "MDSYS.SDO_GEOMETRY";
         for (int i = 0; i < 5; i++)
               Params[i].Direction = ParameterDirection.Input;

         SdoGeometry cent = new SdoGeometry();
         cent.Dimensionality = 3;
         cent.LRS = 0;
         cent.GeometryType = (int)SdoGeometryTypes.GTYPE.POINT;
         int gType = cent.PropertiesToGTYPE();
         SdoPoint centP = new SdoPoint();
         centP.XD = centroid.X;
         centP.YD = centroid.Y;
         centP.ZD = centroid.Z;
         cent.SdoPoint = centP;
         Params[0].Value = cent;
         Params[0].Size = 1;

         SdoGeometry ax1 = new SdoGeometry();
         ax1.Dimensionality = 3;
         ax1.LRS = 0;
         ax1.GeometryType = (int)SdoGeometryTypes.GTYPE.POINT;
         gType = ax1.PropertiesToGTYPE();
         SdoPoint ax1P = new SdoPoint();
         ax1P.XD = majorAxes[0].X;
         ax1P.YD = majorAxes[0].Y;
         ax1P.ZD = majorAxes[0].Z;
         ax1.SdoPoint = ax1P;
         Params[1].Value = ax1;
         Params[1].Size = 1;

         SdoGeometry ax2 = new SdoGeometry();
         ax2.Dimensionality = 3;
         ax2.LRS = 0;
         ax2.GeometryType = (int)SdoGeometryTypes.GTYPE.POINT;
         gType = ax2.PropertiesToGTYPE();
         SdoPoint ax2P = new SdoPoint();
         ax2P.XD = majorAxes[1].X;
         ax2P.YD = majorAxes[1].Y;
         ax2P.ZD = majorAxes[1].Z;
         ax2.SdoPoint = ax2P;
         Params[2].Value = ax2;
         Params[2].Size = 1;

         SdoGeometry ax3 = new SdoGeometry();
         ax3.Dimensionality = 3;
         ax3.LRS = 0;
         ax3.GeometryType = (int)SdoGeometryTypes.GTYPE.POINT;
         gType = ax3.PropertiesToGTYPE();
         SdoPoint ax3P = new SdoPoint();
         ax3P.XD = majorAxes[2].X;
         ax3P.YD = majorAxes[2].Y;
         ax3P.ZD = majorAxes[2].Z;
         ax3.SdoPoint = ax3P;
         Params[3].Value = ax3;
         Params[3].Size = 1;

         OBB = createGeomOBB(OBBVerts);
         Params[4].Value = OBB;
         Params[4].Size = 1;
         try
         {
               int commandStatus = cmd.ExecuteNonQuery();
               DBOperation.commitTransaction();
         }
         catch (OracleException e)
#endif
#if POSTGRES
         string sqlStmt = "UPDATE " + DBOperation.formatTabName("BIMRL_ELEMENT") + " SET obb_ecs=@0, OBB=@1 WHERE ELEMENTID=@2";
         IList<object> paramList = new List<object>();
         IList<NpgsqlDbType> typeList = new List<NpgsqlDbType>();

         //CoordSystem ecs = new CoordSystem();
         //ecs.XAxis = new Point3D(majorAxes[0].X, majorAxes[0].Y, majorAxes[0].Z);
         //ecs.YAxis = new Point3D(majorAxes[1].X, majorAxes[1].Y, majorAxes[1].Z);
         //ecs.ZAxis = new Point3D(majorAxes[2].X, majorAxes[2].Y, majorAxes[2].Z);
         //ecs.Origin = centroid;
         Point3D[] ecs = new Point3D[4];
         ecs[0] = new Point3D(majorAxes[0].X, majorAxes[0].Y, majorAxes[0].Z);
         ecs[1] = new Point3D(majorAxes[1].X, majorAxes[1].Y, majorAxes[1].Z);
         ecs[2] = new Point3D(majorAxes[2].X, majorAxes[2].Y, majorAxes[2].Z);
         ecs[3] = centroid;
         paramList.Add(ecs);
         typeList.Add(NpgsqlDbType.Unknown);

         OBB = createGeomOBB(OBBVerts);
         string obb = OBB.ToJsonString();
         paramList.Add(obb);
         typeList.Add(NpgsqlDbType.Jsonb);

         paramList.Add(_elementid);
         typeList.Add(NpgsqlDbType.Varchar);

         try
         {
            DBOperation.ExecuteNonQueryWithTrans2(sqlStmt, paramList, typeList, commit: true);
         }
         catch (NpgsqlException e)
#endif
         {
            string excStr = "%%Insert Error - " + e.Message + "\n\t" + sqlStmt;
               _refBIMRLCommon.StackPushIgnorableError(excStr);
            // Ignore any error
         }
         catch (SystemException e)
         {
               string excStr = "%%Insert Error - " + e.Message + "\n\t" + sqlStmt;
               _refBIMRLCommon.StackPushError(excStr);
               throw;
         }

#if ORACLE
         DBOperation.commitTransaction();
         cmd.Dispose();
#endif
      }

      public void writeToX3D()
      {
         BIMRLCommon BIMRLCommonRef = new BIMRLCommon();
         BIMRLExportSDOToX3D x3dExp = new BIMRLExportSDOToX3D(BIMRLCommonRef, "c:\\temp\\drawFaces.x3d");
         if (MergedFaceList != null)
               x3dExp.exportFacesToX3D(MergedFaceList);
         x3dExp.endExportToX3D();
      }

      /// <summary>
      /// Create SDOGeometry data for Solid that comes from OBB vertices in the following order
      ///               7 --- 6
      ///              /|    /|
      ///             4 --- 5 |
      ///             | 3 --| 2 
      ///             |/    |/
      ///             0 --- 1
      /// </summary>
      /// <param name="OBBVerts">OBB vertices: must be in order</param>
      /// <returns></returns>
#if ORACLE
      SdoGeometry createGeomOBB(List<Point3D> OBBVerts)
      {
         SdoGeometry geom = new SdoGeometry();
         geom.Dimensionality = 3;
         geom.LRS = 0;
         geom.GeometryType = (int)SdoGeometryTypes.GTYPE.SOLID;
         int gType = geom.PropertiesToGTYPE();

         // expand the list into denormalized face coordinates for SDO Geometry
         List<Point3D> expVertList = new List<Point3D>();
         expVertList.Add(OBBVerts[0]);
         expVertList.Add(OBBVerts[3]);
         expVertList.Add(OBBVerts[2]);
         expVertList.Add(OBBVerts[1]);
         expVertList.Add(OBBVerts[0]);

         expVertList.Add(OBBVerts[0]);
         expVertList.Add(OBBVerts[1]);
         expVertList.Add(OBBVerts[5]);
         expVertList.Add(OBBVerts[4]);
         expVertList.Add(OBBVerts[0]);

         expVertList.Add(OBBVerts[1]);
         expVertList.Add(OBBVerts[2]);
         expVertList.Add(OBBVerts[6]);
         expVertList.Add(OBBVerts[5]);
         expVertList.Add(OBBVerts[1]);

         expVertList.Add(OBBVerts[2]);
         expVertList.Add(OBBVerts[3]);
         expVertList.Add(OBBVerts[7]);
         expVertList.Add(OBBVerts[6]);
         expVertList.Add(OBBVerts[2]);

         expVertList.Add(OBBVerts[3]);
         expVertList.Add(OBBVerts[0]);
         expVertList.Add(OBBVerts[4]);
         expVertList.Add(OBBVerts[7]);
         expVertList.Add(OBBVerts[3]);

         expVertList.Add(OBBVerts[4]);
         expVertList.Add(OBBVerts[5]);
         expVertList.Add(OBBVerts[6]);
         expVertList.Add(OBBVerts[7]);
         expVertList.Add(OBBVerts[4]);

         int geomType = (int)SdoGeometryTypes.ETYPE_COMPOUND.SOLID;
         List<int> elemInfoArr = new List<int>() { 1, geomType, 1 };
         elemInfoArr.Add(1);
         elemInfoArr.Add((int)SdoGeometryTypes.ETYPE_COMPOUND.SURFACE_EXTERIOR);
         elemInfoArr.Add(6);     // 6 faces of the bounding box

         List<double> arrCoord = new List<double>();

         for (int i = 0; i < 30; i+=5)
         {
               elemInfoArr.Add(i*3 +1);     // ElemInfoArray counts the entry of double value and not the vertex
               elemInfoArr.Add((int)SdoGeometryTypes.ETYPE_SIMPLE.POLYGON_EXTERIOR);
               elemInfoArr.Add(1);

               for (int j = i; j < i + 5; ++j)
               {
                  arrCoord.Add(expVertList[j].X);
                  arrCoord.Add(expVertList[j].Y);
                  arrCoord.Add(expVertList[j].Z);
               }
         }

         geom.ElemArrayOfInts = elemInfoArr.ToArray();
         geom.OrdinatesArrayOfDoubles = arrCoord.ToArray();
         return geom;
      }
#endif
#if POSTGRES
      Polyhedron createGeomOBB(List<Point3D> OBBVerts)
      {
         List<Face3D> faceList = new List<Face3D>();

         // For OBB, there will be 6 faces that are the boundaries of the OBB. Create the face of each
         List<Point3D> expVertList = new List<Point3D>();
         expVertList.Add(OBBVerts[0]);
         expVertList.Add(OBBVerts[3]);
         expVertList.Add(OBBVerts[2]);
         expVertList.Add(OBBVerts[1]);
         Face3D f = new Face3D(expVertList);
         faceList.Add(f);

         expVertList = new List<Point3D>();
         expVertList.Add(OBBVerts[0]);
         expVertList.Add(OBBVerts[1]);
         expVertList.Add(OBBVerts[5]);
         expVertList.Add(OBBVerts[4]);
         f = new Face3D(expVertList);
         faceList.Add(f);

         expVertList = new List<Point3D>();
         expVertList.Add(OBBVerts[1]);
         expVertList.Add(OBBVerts[2]);
         expVertList.Add(OBBVerts[6]);
         expVertList.Add(OBBVerts[5]);
         f = new Face3D(expVertList);
         faceList.Add(f);

         expVertList = new List<Point3D>();
         expVertList.Add(OBBVerts[2]);
         expVertList.Add(OBBVerts[3]);
         expVertList.Add(OBBVerts[7]);
         expVertList.Add(OBBVerts[6]);
         f = new Face3D(expVertList);
         faceList.Add(f);

         expVertList = new List<Point3D>();
         expVertList.Add(OBBVerts[3]);
         expVertList.Add(OBBVerts[0]);
         expVertList.Add(OBBVerts[4]);
         expVertList.Add(OBBVerts[7]);
         f = new Face3D(expVertList);
         faceList.Add(f);

         expVertList = new List<Point3D>();
         expVertList.Add(OBBVerts[4]);
         expVertList.Add(OBBVerts[5]);
         expVertList.Add(OBBVerts[6]);
         expVertList.Add(OBBVerts[7]);
         f = new Face3D(expVertList);
         faceList.Add(f);

         Polyhedron pH = new Polyhedron(faceList);
         return pH;
      }
#endif

      public void trueOBBFaces()
      {
         if (OBB != null)
         {
            Polyhedron obbGeom;
#if ORACLE
            if (SDOGeomUtils.generate_Polyhedron(OBB, out obbGeom))
#endif
#if POSTGRES
            obbGeom = OBB;
#endif
            {
               BIMRLGeometryPostProcess processFaces = new BIMRLGeometryPostProcess(_elementid, obbGeom, _refBIMRLCommon, _currFedID, "OBB");
               processFaces.simplifyAndMergeFaces();
               processFaces.insertIntoDB(false);
            }
         }
      }

      public void projectedFaces()
      {
         PrincipalComponentAnalysis pca = new PrincipalComponentAnalysis(_geom);
         List<Point3D> transfPoints = pca.projectedPointList();
         BoundingBox3D trOBB = new BoundingBox3D(transfPoints);
         List<Point3D> modOBB = pca.transformBackPointSet(trOBB.BBVertices);
#if ORACLE
         SdoGeometry sdomOBB = createGeomOBB(modOBB);
         Polyhedron modOBBPolyH;
         SDOGeomUtils.generate_Polyhedron(sdomOBB, out modOBBPolyH);
#endif
#if POSTGRES
         Polyhedron modOBBPolyH = createGeomOBB(modOBB);
#endif
         BIMRLGeometryPostProcess processFaces = new BIMRLGeometryPostProcess(_elementid, modOBBPolyH, _refBIMRLCommon, _currFedID, "PROJOBB");
         processFaces.simplifyAndMergeFaces();
         processFaces.insertIntoDB(false);
      }
   }
}
