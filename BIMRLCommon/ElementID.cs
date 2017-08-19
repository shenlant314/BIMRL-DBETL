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
using System.Collections;
using System.Linq;
using System.Text;

namespace BIMRL.Common
{
    // This class is to convert IFC elementid from its 22 character into 128 bit number and back
    public class ElementID
    {
        private static char[] base64 = { '0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
                                   'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','_','$' };
        string _EIDstr;
        Tuple<UInt64, UInt64> _EIDNo;
        private static Hashtable base64Decoding = new Hashtable();
        static UInt64 lowerMask = 0xF;
        static UInt64 upperMask = 0x30;
        static UInt64 firstCharMask = 0x3;
        static UInt64 sixBitMask = 0x3F;

        public ElementID(string elementidStr)
        {
            // If hashtable is empty, insert the reverse map of the character encoding for fast decoding mechanism. It is static so it will be done once
            if (base64Decoding.Count <= 0)
            {
                for (ushort i = 0; i < 64; ++i)
                    base64Decoding.Add(base64[i], i);
            }
            _EIDstr = elementidStr;

            //decode
            UInt64 lowerPart = 0;
            UInt64 upperPart = 0;
            char[] indivChar = elementidStr.ToCharArray();

            // the first character only occupy 2 bit
            UInt64 tmp = (UInt64)((ushort)base64Decoding[indivChar[0]]);
            upperPart = (tmp & firstCharMask) << 62;

            for (int i = 1; i < 11; ++i)
            {
                tmp = (UInt64)((ushort)base64Decoding[indivChar[i]]);
                upperPart = upperPart | (tmp & sixBitMask) << (62 - i * 6);
            }
            // the 11th cannot fit into the remaining
            UInt64 eleventh = (UInt64)(ushort)base64Decoding[indivChar[11]] & sixBitMask;
            // 4 bits will go to the lowerPart and 2 bits to the upperPart
            upperPart = upperPart | (eleventh & upperMask) >> 4;

            lowerPart = (eleventh & lowerMask) << 60;
            for (int i = 12; i < 22; ++i )
            {
                tmp = (UInt64)((ushort)base64Decoding[indivChar[i]]);
                lowerPart = lowerPart | (tmp & sixBitMask) << (54 - (i - 12) * 6);
            }

            _EIDNo = new Tuple<ulong, ulong>(upperPart, lowerPart);
        }

        public ElementID(Tuple<UInt64,UInt64> elemidNo)
        {
            _EIDNo = elemidNo;
            UInt64 upperPart = elemidNo.Item1;
            UInt64 lowerPart = elemidNo.Item2;

            //encode
            char[] tmpChar = new char[22];

            // Work on the higher part of the elementid
            // the first 2 bit goes to the first char:
            tmpChar[0] = (char)(base64[ (upperPart >> 62) & firstCharMask ]);

            UInt64 tmp = 0;
            for (int i = 1; i < 11; i++)
            {
                tmp = 0;
                tmp = upperPart >> (62 - i * 6);
                tmpChar[i] = (char)base64[tmp & sixBitMask];
            }
            // the remaining 2 bits go to the first 2 bits in the lowerPart
            tmp = (upperPart & firstCharMask) << 4;
            UInt64 tmp2 = (lowerPart >> 60) & lowerMask;
            tmpChar[11] = (char)base64[tmp | tmp2];

            for (int i = 12; i < 22; i++)
            {
                tmp = 0;
                tmp = lowerPart >> (54 - (i-12) * 6);
                tmpChar[i] = (char)base64[tmp & sixBitMask];
            }

            _EIDstr = new string(tmpChar);
        }

      public static string GetElementIDstrFromKey(Tuple<Guid,int> elemidKey, bool userGeom=false)
      {
         UInt64 upperPart = 0;
         UInt64 lowerPart = 0;

         if (elemidKey.Item2 == 0)
         {
            // This is a traditional IFC encoded elementid in 22 Char (128 bits), ignore the Item2
            Byte[] guid = elemidKey.Item1.ToByteArray();
            Byte[] guidUpper = guid.Take(8).ToArray();
            upperPart = BitConverter.ToUInt64(guidUpper, 0);
            Byte[] guidLower = guid.Skip(8).Take(8).ToArray();
            lowerPart = BitConverter.ToUInt64(guidLower, 0);
            ElementID eID = new ElementID(new Tuple<UInt64, UInt64>(upperPart, lowerPart));

            if (userGeom)
            {
               // For UserGeom, the ID i s generated from string of a simple number padded left with '0'. Now we need to remove them
               int end0Pos = 0;
               for (int i = 0; i < eID.ElementIDString.Length; ++i)
               {
                  if (eID.ElementIDString[i] != '0')
                  {
                     end0Pos = i;
                     break;
                  }
               }
               return eID.ElementIDString.Remove(0, end0Pos);
            }
            else
               return eID.ElementIDString;
         }
         else
         {
            // This is Revit style element guid
            string eidStr = elemidKey.Item1.ToString() + "-" + elemidKey.Item2.ToString("x8");
            return eidStr;
         }
      }

        public string ElementIDString
        {
            get { return _EIDstr; }
        }

        public Tuple<UInt64,UInt64> ElementIDNo
        {
            get { return _EIDNo; }
        }

        public override string ToString()
        {
            return _EIDstr;
        }

      public Guid ElementIDGuid
      {
         get
         {
            IEnumerable<byte> combined = BitConverter.GetBytes(_EIDNo.Item1).Concat(BitConverter.GetBytes(_EIDNo.Item2));
            Guid guid = new Guid(combined.ToArray());
            return guid;
         }
      }
    }
}
