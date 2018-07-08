﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace BIMRL
{
   class ProcFacesPar
   {
      public BIMRLSpatialIndex spIdx;
      public int FedID;
      public string whereCond;
      static ConcurrentBag<TopoFaceRecCollection> bagOfTopoFaceRec;

      public ProcFacesPar(BIMRLSpatialIndex spIdx, int fedID, string where)
      {
         this.spIdx = spIdx;
         this.FedID = fedID;
         this.whereCond = where;
         bagOfTopoFaceRec = new ConcurrentBag<TopoFaceRecCollection>();
      }
   }
}