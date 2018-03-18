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

namespace BIMRL.Common
{
   public class Cuboid
   {
      private Polyhedron _polyHRep;

      Point3D origin;
      Point3D centroid;

      /// <summary>
      /// make 3D cube
      /// </summary>
      /// <param name="Origin">center</param>
      /// <param name="Width">Representative X-coordinate</param>
      /// <param name="Height">Representative Y-coordinate</param>
      /// <param name="Depth">Representative Z-coordinate</param>
      public Cuboid(Point3D Origin, double xLength, double yLength, double zLength)
      {
         origin = Origin;

         centroid = new Point3D();
         centroid.X = origin.X + 0.5 * xLength;
         centroid.Y = origin.Y + 0.5 * yLength;
         centroid.Z = origin.Z + 0.5 * zLength;

         // Define list of corrdinates for the cuboid vertices to be fed to the polyhedron

         List<double> cuboidVerCoords = new List<double>();
         // Point #1
         Point3D p1 = origin;
         //cuboidVerCoords.Add(origin.X);
         //cuboidVerCoords.Add(origin.Y);
         //cuboidVerCoords.Add(origin.Z);

         // Point #2
         Point3D p2 = new Point3D(origin.X + xLength, origin.Y, origin.Z);
            //cuboidVerCoords.Add(origin.X + xLength);
            //cuboidVerCoords.Add(origin.Y);
            //cuboidVerCoords.Add(origin.Z);

         // Point #3
         Point3D p3 = new Point3D(origin.X, origin.Y + yLength, origin.Z);
         //cuboidVerCoords.Add(origin.X);
         //   cuboidVerCoords.Add(origin.Y + yLength);
         //   cuboidVerCoords.Add(origin.Z);

         // Point #4
         Point3D p4 = new Point3D(origin.X + xLength, origin.Y + yLength, origin.Z);
         //cuboidVerCoords.Add(origin.X + xLength);
         //   cuboidVerCoords.Add(origin.Y + yLength) ;
         //   cuboidVerCoords.Add(origin.Z);

         // Point #5
         Point3D p5 = new Point3D(origin.X, origin.Y, origin.Z + zLength);
         //cuboidVerCoords.Add(origin.X);
         //   cuboidVerCoords.Add(origin.Y);
         //   cuboidVerCoords.Add(origin.Z + zLength);

         // Point #6
         Point3D p6 = new Point3D(origin.X + xLength, origin.Y, origin.Z + zLength);
         //cuboidVerCoords.Add(origin.X + xLength);
         //   cuboidVerCoords.Add(origin.Y);
         //   cuboidVerCoords.Add(origin.Z + zLength);

         // Point #7
         Point3D p7 = new Point3D(origin.X, origin.Y + yLength, origin.Z + zLength);
         //cuboidVerCoords.Add(origin.X);
         //   cuboidVerCoords.Add(origin.Y + yLength);
         //   cuboidVerCoords.Add(origin.Z + zLength);

         // Point #8
         Point3D p8 = new Point3D(origin.X + xLength, origin.Y + yLength, origin.Z + zLength);
         //cuboidVerCoords.Add(origin.X + xLength);
         //   cuboidVerCoords.Add(origin.Y + yLength);
         //   cuboidVerCoords.Add(origin.Z + zLength);

         //// Create list of face index to the list of coordinates representing the cuboid rectangular faces (in four tupple) - there will be 6 faces in total        
         //List<int> idxFaceCoords = new List<int>();

         List<Face3D> faceList = new List<Face3D>();
         //// Face #1 - front face
         faceList.Add(new Face3D(new List<Point3D>() { p1, p2, p6, p5 }, skipCheck: true));
         //idxFaceCoords.Add(0*3);
         //idxFaceCoords.Add(1*3);
         //idxFaceCoords.Add(5*3);
         //idxFaceCoords.Add(4*3);

         //// Face #2 - right face
         faceList.Add(new Face3D(new List<Point3D>() { p2, p4, p8, p6 }, skipCheck: true));
         //idxFaceCoords.Add(1*3);
         //idxFaceCoords.Add(3*3);
         //idxFaceCoords.Add(7*3);
         //idxFaceCoords.Add(5*3);

         //// Face #3 - back face
         faceList.Add(new Face3D(new List<Point3D>() { p4, p3, p7, p8 }, skipCheck: true));
         //idxFaceCoords.Add(3*3);
         //idxFaceCoords.Add(2*3);
         //idxFaceCoords.Add(6*3);
         //idxFaceCoords.Add(7*3);

         //// Face #4 - left face
         faceList.Add(new Face3D(new List<Point3D>() { p3, p1, p5, p7 }, skipCheck: true));
         //idxFaceCoords.Add(2*3);
         //idxFaceCoords.Add(0*3);
         //idxFaceCoords.Add(4*3);
         //idxFaceCoords.Add(6*3);

         //// Face #5 - bottom face
         faceList.Add(new Face3D(new List<Point3D>() { p1, p3, p4, p2 }, skipCheck: true));
         //idxFaceCoords.Add(0*3);
         //idxFaceCoords.Add(2*3);
         //idxFaceCoords.Add(3*3);
         //idxFaceCoords.Add(1*3);

         //// Face #6 - top face
         faceList.Add(new Face3D(new List<Point3D>() { p5, p6, p8, p7 }, skipCheck: true));
         //idxFaceCoords.Add(4*3);
         //idxFaceCoords.Add(5*3);
         //idxFaceCoords.Add(7*3);
         //idxFaceCoords.Add(6*3);

         //_polyHRep = new Polyhedron(PolyhedronFaceTypeEnum.RectangularFaces, true, cuboidVerCoords, idxFaceCoords, null);
         _polyHRep = new Polyhedron(faceList);
      }

      public Point3D Centroid
      {
         get { return centroid; }
      }

      public Polyhedron cuboidPolyhedron
      {
         get { return _polyHRep; }
      }

      public double extent
      {
         get { return _polyHRep.boundingBox.extent; }
      }

      public Face3D TopFace
      {
         get
         {
            List<Point3D> verts = new List<Point3D>();
            Point3D l = _polyHRep.boundingBox.LLB;
            Point3D u = _polyHRep.boundingBox.URT;
            verts.Add(new Point3D(l.X, l.Y, u.Z));
            verts.Add(new Point3D(u.X, l.Y, u.Z));
            verts.Add(new Point3D(u.X, u.Y, u.Z));
            verts.Add(new Point3D(l.X, u.Y, u.Z));
            return new Face3D(verts);
         }
      }

      public Face3D BottomFace
      {
         get
         {
            List<Point3D> verts = new List<Point3D>();
            Point3D l = _polyHRep.boundingBox.LLB;
            Point3D u = _polyHRep.boundingBox.URT;
            verts.Add(new Point3D(l.X, l.Y, l.Z));
            verts.Add(new Point3D(u.X, l.Y, l.Z));
            verts.Add(new Point3D(u.X, u.Y, l.Z));
            verts.Add(new Point3D(l.X, u.Y, l.Z));
            return new Face3D(verts);
         }
      }

      public Face3D FrontFace
      {
         get
         {
            List<Point3D> verts = new List<Point3D>();
            Point3D l = _polyHRep.boundingBox.LLB;
            Point3D u = _polyHRep.boundingBox.URT;
            verts.Add(new Point3D(l.X, l.Y, l.Z));
            verts.Add(new Point3D(u.X, l.Y, l.Z));
            verts.Add(new Point3D(u.X, l.Y, u.Z));
            verts.Add(new Point3D(l.X, l.Y, u.Z));
            return new Face3D(verts);
         }
      }

      public Face3D BackFace
      {
         get
         {
            List<Point3D> verts = new List<Point3D>();
            Point3D l = _polyHRep.boundingBox.LLB;
            Point3D u = _polyHRep.boundingBox.URT;
            verts.Add(new Point3D(l.X, u.Y, l.Z));
            verts.Add(new Point3D(u.X, u.Y, l.Z));
            verts.Add(new Point3D(u.X, u.Y, u.Z));
            verts.Add(new Point3D(l.X, u.Y, u.Z));
            return new Face3D(verts);
         }
      }

      public Face3D RightFace
      {
         get
         {
            List<Point3D> verts = new List<Point3D>();
            Point3D l = _polyHRep.boundingBox.LLB;
            Point3D u = _polyHRep.boundingBox.URT;
            verts.Add(new Point3D(u.X, l.Y, l.Z));
            verts.Add(new Point3D(u.X, u.Y, l.Z));
            verts.Add(new Point3D(u.X, u.Y, u.Z));
            verts.Add(new Point3D(u.X, l.Y, u.Z));
            return new Face3D(verts);
         }
   }

      public Face3D LeftFace
      {
         get
         {
            List<Point3D> verts = new List<Point3D>();
            Point3D l = _polyHRep.boundingBox.LLB;
            Point3D u = _polyHRep.boundingBox.URT;
            verts.Add(new Point3D(l.X, l.Y, l.Z));
            verts.Add(new Point3D(l.X, u.Y, l.Z));
            verts.Add(new Point3D(l.X, u.Y, u.Z));
            verts.Add(new Point3D(l.X, l.Y, u.Z));
            return new Face3D(verts);
         }
      }

      public bool intersectWith (Polyhedron PH2)
      {
         return Polyhedron.intersect(this._polyHRep, PH2);
      }

      public bool intersectWith (Face3D F)
      {
         return Polyhedron.intersect(this._polyHRep, F);
      }

      public bool intersectWith (LineSegment3D LS)
      {
         return Polyhedron.intersect(this._polyHRep, LS);
      }

      public bool isInside (Polyhedron pH)
      {
         //return Polyhedron.inside(this._polyHRep, PH);
         // To check whether a polyhedron is inside a cuboid, we simply need to check the polyhedron's bounding box is inside the cuboid
         return _polyHRep.boundingBox.IsInside(pH);
      }

      public bool isInside(BoundingBox3D bbox)
      {
         return _polyHRep.boundingBox.IsInside(bbox);
      }

      public bool isInside (Face3D face)
      {
         //return Polyhedron.inside(this._polyHRep, face);
         // To check whether a face is inside a cuboid, we simply need to check the face's bounding box is inside the cuboid
         return _polyHRep.boundingBox.IsInside(face);
      }

      public bool isInside (LineSegment3D ls)
      {
         //return Polyhedron.inside(this._polyHRep, ls);
         // To check whether a linesegment is inside a cuboid, we simply need to check the linesegment's bounding box is inside the cuboid
         return _polyHRep.boundingBox.IsInside(ls);
      }

      public bool isInside (Point3D point)
      {
         //return Polyhedron.inside(this._polyHRep, point);
         // To check whether a linesegment is inside a cuboid, we simply need to check the linesegment's bounding box is inside the cuboid
         return _polyHRep.boundingBox.IsInside(point);
      }

      public override string ToString()
      {
         return _polyHRep.ToString();
      }
   }
}
