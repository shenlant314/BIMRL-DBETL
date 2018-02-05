using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using BIMRL;
using BIMRL.Common;


namespace BIMRL
{
   class TopoFaceState
   {
      public string elemTyp;
      public Polyhedron geom;
      public string elemID;
      public BIMRLCommon refBIMRLCommon;
      public int federatedId;
      public ManualResetEvent manualEvent;
      public Vector3D trueNorth;

      public TopoFaceState(string elementID, BIMRLCommon refBIMRLCommon, Polyhedron polyhedronGeom, string elementType, int fedID, ManualResetEvent manualEvent, Vector3D trueNorth)
      {
         this.elemID = elementID;
         this.refBIMRLCommon = refBIMRLCommon;
         this.geom = polyhedronGeom;
         this.elemTyp = elementType;
         this.federatedId = fedID;
         this.manualEvent = manualEvent;
         this.trueNorth = trueNorth;
      }
   }
}
