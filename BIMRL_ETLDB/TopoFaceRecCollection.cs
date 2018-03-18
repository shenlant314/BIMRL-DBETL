using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BIMRL.Common;

namespace BIMRL
{
   class TopoFaceRecCollection
   {
      public string elementid;
      public string id;
      public string type;
#if POSTGRES
      public string facePolygon;
      public Point3D normal;
      public Point3D centroid;
#endif
#if ORACLE
      public SdoGeometry facePolygon;
      public SdoGeometry normal;
      public SdoGeometry centroid;
#endif
      public double angleFromNorth;
      public string orientation;
      public string attribute;
      public double topOrBottomZ;
      public double area;
   }
}
