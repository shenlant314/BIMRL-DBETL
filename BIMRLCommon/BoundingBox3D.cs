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
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace BIMRL.Common
{
   public class BoundingBox3D
   {
      Point3D _LLB = new Point3D();
      Point3D _URT = new Point3D();

      /// <summary>
      /// Determine the encosing bounding box (axis-aligned) of a set of vertices in 3D
      /// </summary>
      /// <param name="vertices"></param>
      public BoundingBox3D(List<Point3D> vertices)
      {
         _LLB.X = _LLB.Y = _LLB.Z = Double.MaxValue;
         _URT.X = _URT.Y = _URT.Z = Double.MinValue;

         for (int i=0; i<vertices.Count; i++)
         {
               if (vertices[i].X < _LLB.X) _LLB.X = vertices[i].X;
               if (vertices[i].X > _URT.X) _URT.X = vertices[i].X;
               if (vertices[i].Y < _LLB.Y) _LLB.Y = vertices[i].Y;
               if (vertices[i].Y > _URT.Y) _URT.Y = vertices[i].Y;
               if (vertices[i].Z < _LLB.Z) _LLB.Z = vertices[i].Z;
               if (vertices[i].Z > _URT.Z) _URT.Z = vertices[i].Z;
         }
      }

      public BoundingBox3D(Point3D LLB, Point3D URT)
      {
         _LLB = LLB;
         _URT = URT;
      }

      public BoundingBox3D()
      {
         // Initialize zero BB
         _LLB.X = 0.0;
         _LLB.Y = 0.0;
         _LLB.Z = 0.0;
         _URT.X = 0.0;
         _URT.Y = 0.0;
         _URT.Z = 0.0;
      }

      /// <summary>
      /// This is the Lower Left at the Bottom end
      /// </summary>
      public Point3D LLB
      {
         get { return _LLB; }
      }

      /// <summary>
      /// This is the Upper Right at the Top end
      /// </summary>
      public Point3D URT
      {
         get { return _URT; }
      }

      [JsonIgnore]
      public double extent
      {
         get {return Point3D.distance(LLB, URT);} 
      }

      [JsonIgnore]
      public double XLength
      {
         get { return Math.Abs(URT.X - LLB.X); }
      }

      [JsonIgnore]
      public double YLength
      {
         get { return Math.Abs(URT.Y - LLB.Y); }
      }

      [JsonIgnore]
      public double ZLength
      {
         get { return Math.Abs(URT.Z - LLB.Z); }
      }

      [JsonIgnore]
      public Point3D Center
      {
         get
         {
               return (new Point3D(LLB.X + XLength / 2, LLB.Y + YLength / 2, LLB.Z + ZLength / 2));
         }
      }

      [JsonIgnore]
      public List<Point3D> BBVertices
      {
         get 
         {
               List<Point3D> bbverts = new List<Point3D>();
               bbverts.Add(new Point3D(LLB.X, LLB.Y, LLB.Z));
               bbverts.Add(new Point3D(URT.X, LLB.Y, LLB.Z));
               bbverts.Add(new Point3D(URT.X, URT.Y, LLB.Z));
               bbverts.Add(new Point3D(LLB.X, URT.Y, LLB.Z));
               bbverts.Add(new Point3D(LLB.X, LLB.Y, URT.Z));
               bbverts.Add(new Point3D(URT.X, LLB.Y, URT.Z));
               bbverts.Add(new Point3D(URT.X, URT.Y, URT.Z));
               bbverts.Add(new Point3D(LLB.X, URT.Y, URT.Z));
               return bbverts;
         }
      }

      public bool IsInside (Polyhedron polyH)
      {
         if (polyH.boundingBox.LLB >= this.LLB && polyH.boundingBox.URT <= this.URT)
            return true;
         else
            return false;
      }

      public bool IsInside(Face3D face)
      {
         if (face.boundingBox.LLB >= this.LLB && face.boundingBox.URT <= this.URT)
            return true;
         else
            return false;
      }

      public bool IsInside(LineSegment3D ls)
      {
         if (ls.startPoint >= this.LLB && ls.startPoint <= this.URT && ls.endPoint >= this.LLB && ls.endPoint <= this.URT)
            return true;
         else
            return false;
      }

      public bool IsInside(BoundingBox3D bbox)
      {
         if (bbox.LLB >= this.LLB && bbox.URT <= this.URT)
            return true;
         else
            return false;
      }

      public bool IsInside(Point3D point)
      {
         if (point >= this.LLB && point <= this.URT)
            return true;
         else
            return false;
      }

      public override string ToString()
      {
         string pr = "LLB" + this.LLB.ToString() + "\nURT" + this.URT.ToString();
         return pr;
      }
   }
}
