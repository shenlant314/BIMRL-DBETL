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
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace BIMRL.Common
{
   [Serializable]
   public struct CellData
   {
      public byte nodeType;   // node=0, leaf=1
                              //public Dictionary<string, int> data;
      public SortedSet<int> data;
   }

   /// <summary>
   /// This class is introduced to overcome the limit of a Dictionary size (< 2^32 bytes) by using a List of Dictionaries which will be automatically added as the need arises
   /// </summary>
   public class OctreeDict
   {
      static ConcurrentDictionary<int,ConcurrentDictionary<UInt64, CellData>> OctreeDictList;
      static ConcurrentDictionary<int, uint> dictSizeList;
      // The treshold is set to 50% capacity to allow expansion when more data is added (only the new item may create a new Dictionary entry)
      uint DictSizeThreshold = (uint) Math.Round(Math.Pow(2, 32) * 0.5);
      int ConcurrencyLevel = Environment.ProcessorCount;
      int DefaultInitSize = 100000;

      /// <summary>
      /// Initialize the Dictionary list
      /// </summary>
      public OctreeDict()
      {
         OctreeDictList = new ConcurrentDictionary<int, ConcurrentDictionary<ulong, CellData>>();
         dictSizeList = new ConcurrentDictionary<int, uint>();
         ConcurrentDictionary<UInt64, CellData> dictItem = new ConcurrentDictionary<UInt64, CellData>(ConcurrencyLevel, DefaultInitSize);
         OctreeDictList.TryAdd(0, dictItem);
         dictSizeList.TryAdd(0, 0);
      }

      public OctreeDict(int initDictSize)
      {
         DefaultInitSize = initDictSize;
         OctreeDictList = new ConcurrentDictionary<int, ConcurrentDictionary<ulong, CellData>>();
         dictSizeList = new ConcurrentDictionary<int, uint>();
         ConcurrentDictionary<UInt64, CellData> dictItem = new ConcurrentDictionary<UInt64, CellData>(ConcurrencyLevel, DefaultInitSize);
         OctreeDictList.TryAdd(0, dictItem);
         dictSizeList.TryAdd(0, 0);
      }

      /// <summary>
      /// Add or update an item (CellData) given the cellid into the appropriate Dictionary
      /// </summary>
      /// <param name="cellid">the cellid</param>
      /// <param name="celldata">the celldata</param>
      public void AddOrUpdate(UInt64 cellid, CellData celldata)
      {
         ConcurrentDictionary<UInt64, CellData> curDictToUse = null;
         int dictIdx = 0;
         CellData extCellData = new CellData();
         bool found = false;
         for(int i = 0; i < DictEntryCount; ++i)
         {
            curDictToUse = OctreeDictList[i];
            if (curDictToUse.TryGetValue(cellid, out extCellData))
            {
               found = true;
               break;
            }
            if (dictIdx < DictEntryCount - 1)
               dictIdx++;
         }
         if (found)
         {
            // An existing item with the same key has been found, union the item with the existing item
            extCellData.data.UnionWith(celldata.data);
            dictSizeList[dictIdx] += (uint)celldata.data.Count * sizeof(uint);
         }
         else
         {
            if (dictSizeList[dictIdx] < DictSizeThreshold)
            {
               // Size of the Dict is still within the threshold limit, add the item into the last Dict 
               curDictToUse.TryAdd(cellid, celldata);
               dictSizeList[dictIdx] += 72 + (uint)celldata.data.Count * sizeof(uint);
            }
            else
            {
               // Size is already above the threshold, create a new Dictionary and add it into the List and add the item into the new Dict
               dictIdx++;
               OctreeDictList.TryAdd(dictIdx, new ConcurrentDictionary<UInt64, CellData>(ConcurrencyLevel, DefaultInitSize));
               dictSizeList.TryAdd(dictIdx, 0);
               curDictToUse = OctreeDictList[dictIdx];
               curDictToUse.TryAdd(cellid, celldata);
               dictSizeList[dictIdx] += 72 + (uint)celldata.data.Count * sizeof(int);
            }
         }
      }

      /// <summary>
      /// Replace Value in the Dictionary with a new Value
      /// </summary>
      /// <param name="cellid">the cellid</param>
      /// <param name="celldata">the new Value</param>
      public void Replace(UInt64 cellid, CellData celldata)
      {
         ConcurrentDictionary<UInt64, CellData> curDictToUse = null;
         int dictIdx = 0;
         CellData extCellData = new CellData();
         bool found = false;
         for (int i =0; i<DictEntryCount; ++i)
         {
            curDictToUse = OctreeDictList[i];
            if (curDictToUse.TryGetValue(cellid, out extCellData))
            {
               found = true;
               break;
            }
            dictIdx++;
         }
         if (found)
         {
            dictSizeList[dictIdx] += (uint)(celldata.data.Count - extCellData.data.Count) * sizeof(int);
            curDictToUse[cellid] = celldata;
         }
      }

      /// <summary>
      /// Replace Value in the Dictionary with a new Value when the Dict index is known
      /// </summary>
      /// <param name="cellid">the cellid</param>
      /// <param name="celldata">the new Value</param>
      /// <param name="dictIdx">the Dict index</param>
      public void Replace(UInt64 cellid, CellData celldata, int dictIdx)
      {
         CellData extCellData = new CellData();
         if (OctreeDictList[dictIdx].TryGetValue(cellid, out extCellData))
         {
            OctreeDictList[dictIdx][cellid] = celldata;
         }
      }

      /// <summary>
      /// Try to get value for the CellData in the list of Dictionary entries
      /// </summary>
      /// <param name="cellid">the cellid</param>
      /// <param name="dictIndex">output index of the Dictionary containing the cellid</param>
      /// <param name="cellData">the celldata of the cellid</param>
      /// <returns>return true if found the item</returns>
      public bool TryGetValue(UInt64 cellid, out int dictIndex, out CellData cellData)
      {
         ConcurrentDictionary<UInt64, CellData> curDict = null;
         cellData = new CellData();
         dictIndex = 0;
         bool found = false;
         for(int i=0; i<DictEntryCount; ++i)
         {
            curDict = OctreeDictList[i];
            if (curDict.TryGetValue(cellid, out cellData))
            {
               found = true;
               break;
            }
            dictIndex++;
         }

         return found;
      }

      /// <summary>
      /// Check whether the cellid supplied exists in the list of Dictionary entries
      /// </summary>
      /// <param name="cellid">the cellid</param>
      /// <returns>true if found</returns>
      public bool ContainsKey(UInt64 cellid)
      {
         ConcurrentDictionary<UInt64, CellData> curDict = null;
         bool found = false;
         for (int i = 0; i < DictEntryCount; ++i)
         {
            curDict = OctreeDictList[i];
            if (curDict.ContainsKey(cellid))
            {
               found = true;
               break;
            }
         }

         return found;
      }

      /// <summary>
      /// Reset the class to the initial state condition
      /// </summary>
      public void Reset()
      {
         for (int i = OctreeDictList.Count - 1; i == 0; --i)
            OctreeDictList[i].Clear();
         OctreeDictList.Clear();
         dictSizeList.Clear();

         // Initialize new set again
         OctreeDictList = new ConcurrentDictionary<int, ConcurrentDictionary<ulong, CellData>>();
         dictSizeList = new ConcurrentDictionary<int, uint>();
         ConcurrentDictionary<UInt64, CellData> dictItem = new ConcurrentDictionary<UInt64, CellData>(ConcurrencyLevel, DefaultInitSize);
         OctreeDictList.TryAdd(0, dictItem);
         dictSizeList.TryAdd(0, 0);
      }

      public int DictEntryCount
      {
         get { return OctreeDictList.Count; }
      }

      public ConcurrentDictionary<UInt64, CellData> GetElementAt(int index)
      {
         return OctreeDictList[index];
      }
   }
}
