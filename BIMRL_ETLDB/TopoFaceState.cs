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

      public TopoFaceState(string elementID, BIMRLCommon inpRefBIMRLCommon, Polyhedron polyhedronGeom, string elementType, int fedID, ManualResetEvent inpManualEvent, Vector3D inpTrueNorth)
      {
         elemID = elementID;
         refBIMRLCommon = inpRefBIMRLCommon;
         geom = polyhedronGeom;
         elemTyp = elementType;
         federatedId = fedID;
         manualEvent = inpManualEvent;
         trueNorth = inpTrueNorth;
      }
   }
}
