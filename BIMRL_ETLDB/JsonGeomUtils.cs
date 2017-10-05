using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using BIMRL.Common;

namespace BIMRL
{
   public class JsonGeomUtils
   {
#if POSTGRES
      /// <summary>
      /// Generates the proper geometry objects from jsonb data from inside Postgres
      /// </summary>
      /// <param name="geomType">the geometry enumeration type as stored in the DB together with the geometry data</param>
      /// <param name="geometryJsonStr">the jsonb string</param>
      /// <returns>the appropriate geometry object</returns>
      public static object generateGeometryFromJson(GeometryTypeEnum geomType, string geometryJsonStr)
      {
         object ret = null;
         dynamic geomDyn = JsonConvert.DeserializeObject(geometryJsonStr);
         switch (geomType)
         {
            case GeometryTypeEnum.geomsolid3d:
            case GeometryTypeEnum.geomtriangulatedfaceset3d:
               ret = Generate_Polyhedron(geomDyn);
               break;
            case GeometryTypeEnum.geomsurface3d:
               ret = Generate_Polyhedron(geomDyn, isSolid:false);
               break;
            case GeometryTypeEnum.geomface3d:
               ret = Generate_Face3D(geomDyn);
               break;

            case GeometryTypeEnum.geomline3d:
               ret = Generate_LineSegment3D(geomDyn);
               break;

            case GeometryTypeEnum.geombox3d:
               ret = Generate_BoundingBox3D(geomDyn);
               break;

            case GeometryTypeEnum.geompoint3d:
               ret = Generate_Point3D(geomDyn);
               break;

            case GeometryTypeEnum.geompoint3dset:
               ret = Generate_Point3DSet(geomDyn);
               break;

            case GeometryTypeEnum.geompolyline3d:
               ret = Generate_Polyline3D(geomDyn);
               break;

            default:
               // Unsupported type
               break;
         }

         return ret;
      }

      /// <summary>
      /// Process and return the list of Polyhedron
      /// </summary>
      /// <param name="geomDyn">the jsonb string containing the list of one or more Polyhedron</param>
      /// <returns>the list of Polyhedron</returns>
      public static List<Polyhedron> Generate_Polyhedron(dynamic geomDyn, bool isSolid=true)
      {
         List<Polyhedron> polyHList = new List<Polyhedron>();

         // for each polyhedron in the list
         foreach (dynamic item in geomDyn)
         {
            Polyhedron polyH = null;
            List<Face3D> faceList = new List<Face3D>();

            // for every face3d of the polyhedron
            foreach (dynamic face in item.Faces)
            {
               Face3D f = Generate_Face3D(face);
               if (f != null)
                  faceList.Add(f);
            }
            if (faceList.Count > 0)
            {
               polyH = new Polyhedron(faceList, isSolid);
               polyHList.Add(polyH);
            }
         }

         if (polyHList == null || polyHList.Count == 0)
            return null;

         return polyHList;
      }

      /// <summary>
      /// Process and return Face3D from jsonb string
      /// </summary>
      /// <param name="face">jsonb string containing Face3D data (also known as Polygon)</param>
      /// <returns>the Face3D object</returns>
      public static Face3D Generate_Face3D(dynamic face)
      {
         Face3D f = null;
         // first get the outerbound
         List<List<Point3D>> vList = new List<List<Point3D>>();

         List<Point3D> outer = new List<Point3D>();
         foreach (dynamic p in face.VerticesWithHoles[0])
         {
            Point3D vert = Generate_Point3D(p);
            outer.Add(vert);
         }
         if (outer.Count > 0)
            vList.Add(outer);

         // collect the innerbound if any
         if (face.VerticesWithHoles.Count > 1)
         {
            List<Point3D> inner = new List<Point3D>();
            for (int i = 1; i < face.VerticesWithHoles.Count; ++i)
            {
               foreach (dynamic p in face.VerticesWithHoles[i])
               {
                  Point3D vert = Generate_Point3D(p);
                  inner.Add(vert);
               }
               if (inner.Count > 0)
                  vList.Add(inner);
            }
         }
         if (vList.Count > 0)
         {
            f = new Face3D(vList);
         }

         return f;
      }

      /// <summary>
      /// Process and return LineSegment3D from jsonb string data
      /// </summary>
      /// <param name="geomDyn">the linesegment data from jsonb</param>
      /// <returns>the LineSegment3D object</returns>
      public static object Generate_LineSegment3D(dynamic geomDyn)
      {
         Point3D stPoint = Generate_Point3D(geomDyn.StartPoint);
         Point3D enPoint = Generate_Point3D(geomDyn.EndPoint);
         LineSegment3D lineS = new LineSegment3D(stPoint, enPoint);

         return lineS;
      }

      /// <summary>
      /// Process and return BoundingBox3D from jsonb data
      /// </summary>
      /// <param name="geomDyn">the boundingbox data from jsonb</param>
      /// <returns>the BoundingBox3D object</returns>
      public static object Generate_BoundingBox3D(dynamic geomDyn)
      {
         Point3D LLB = Generate_Point3D(geomDyn.LLB);
         Point3D URT = Generate_Point3D(geomDyn.URT);
         BoundingBox3D bbox = new BoundingBox3D(LLB, URT);

         return bbox;
      }

      /// <summary>
      /// Process and return Point3D from jsonb data
      /// </summary>
      /// <param name="p">the Point3D data from jsonb</param>
      /// <returns>the Point3D object</returns>
      public static Point3D Generate_Point3D(dynamic p)
      {
         double X = (double)p.X;
         double Y = (double)p.Y;
         double Z = (double)p.Z;
         Point3D point = new Point3D(X, Y, Z);

         return point;
      }

      /// <summary>
      /// Process and return Point3Dset from jsonb data
      /// </summary>
      /// <param name="geomDyn">the Point3Dset data from jsonb</param>
      /// <returns>return the List of Point3D</returns>
      public static List<Point3D> Generate_Point3DSet(dynamic geomDyn)
      {
         List<Point3D> pointset = new List<Point3D>();
         foreach (dynamic item in geomDyn)
         {
            Point3D p = Generate_Point3D(item);
            pointset.Add(p);
         }
         return pointset;
      }

      /// <summary>
      /// Process and return Polyline from the jsonb data
      /// </summary>
      /// <param name="geomDyn">the Polyline data from jsonb</param>
      /// <returns>the List of Point3D vertices of the polyline</returns>
      public static List<Point3D> Generate_Polyline3D(dynamic geomDyn)
      {
         List<Point3D> polyline = new List<Point3D>();
         foreach (dynamic item in geomDyn)
         {
            Point3D p = Generate_Point3D(item);
            polyline.Add(p);
         }
         return polyline;
      }
#endif
   }
}
