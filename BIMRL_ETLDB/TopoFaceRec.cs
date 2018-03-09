using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BIMRL.Common;
#if ORACLE
using NetSdoGeometry;
#endif

namespace BIMRL
{
   class TopoFaceRec
   {
      public string elementID;
      public string faceID;
      public string type;
      public double angle;
      public double area;
#if ORACLE
      public SdoGeometry faceGeom;
      public SdoGeometry normal;
      public SdoGeometry centroid;

      public TopoFaceRec(string elementID, string faceID, string type, SdoGeometry facePolygon, SdoGeometry normal, SdoGeometry centroid, double angle, double area)
      {
         this.elementID = elementID;
         this.faceID = faceID;
         this.type = type;
         this.angle = angle;
         this.area = area;
         this.faceGeom = facePolygon;
         this.centroid = centroid;
      }
#endif
#if POSTGRES
      public string faceGeom;
      public Point3D normal;
      public Point3D centroid;

      public TopoFaceRec(string elementID, string faceID, string type, string facePolygon, Point3D normal, Point3D centroid, double angle, double area)
      {
         this.elementID = elementID;
         this.faceID = faceID;
         this.type = type;
         this.angle = angle;
         this.area = area;
         this.faceGeom = facePolygon;
         this.centroid = centroid;
      }
#endif
   }
}
