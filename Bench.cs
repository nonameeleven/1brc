using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using BenchmarkDotNet.Attributes;

namespace _1RBC;

public class Bench
{
    private const uint cityMax = 32768;

    public static uint MurmurHash3(byte[] data, uint seed = 0)
    {
        const uint c1 = 0xcc9e2d51;
        const uint c2 = 0x1b873593;
        const int r1 = 15;
        const int r2 = 13;
        const uint m = 5;
        const uint n = 0xe6546b64;

        uint hash = seed;
        int len = data.Length;
        int index = 0;

        // Process the input in 4-byte chunks
        while (index + 4 <= len)
        {
            uint k = BitConverter.ToUInt32(data, index);
            k *= c1;
            k = RotateLeft(k, r1);
            k *= c2;

            hash ^= k;
            hash = RotateLeft(hash, r2) * m + n;

            index += 4;
        }

        // Handle any remaining bytes
        if (index < len)
        {
            uint k1 = 0;
            for (int i = index; i < len; i++)
            {
                k1 ^= (uint)data[i] << ((i - index) * 8);
            }
            k1 *= c1;
            k1 = RotateLeft(k1, r1);
            k1 *= c2;
            hash ^= k1;
        }

        // Final mixing of bits
        hash ^= (uint)len;
        hash = MixFinal(hash);

        return hash;
    }

    public static unsafe uint RetardHash(byte[] arr)
    {
        const uint c1 = 0xcc9e2d51;
        const uint c2 = 0x1b873593;
        const uint c3 = c1 ^ c2;

        uint hash = 0;

        fixed (byte* dataP = arr)
        {
            uint* data = (uint*)dataP;

            uint i1 = (data[0] + data[1] + data[2] + data[3])  * c1;
            uint i2 = (data[4] + data[5] + data[6] + data[7]) * c2;
            uint i3 = (data[8] + data[9] + data[10] + data[9]) * c1;
            uint i4 = (data[12] + data[13] + data[14] + data[15]) * c2;
            uint i5 = (data[16] + data[17] + data[18] + data[19]) * c1;
            uint i6 = (data[20] + data[21] + data[22] + data[23]) * c2;
            uint i7 = (data[24] + data[25] + data[26] + data[27]) * c1;
            uint i8 = (data[28] + data[29] + data[30] + data[31]) * c2;

            hash = (i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8);
        }

        return hash % cityMax;
    }
    
    private static uint RotateLeft(uint x, int r)
    {
        return (x << r) | (x >> (32 - r));
    }

    private static uint MixFinal(uint hash)
    {
        hash ^= hash >> 16;
        hash *= 0x85ebca6b;
        hash ^= hash >> 13;
        hash *= 0xc2b2ae35;
        hash ^= hash >> 16;
        return hash;
    }

    static unsafe uint tpop_hash(byte[] arr)
    {
        fixed (byte* b00 = arr)
        {
            ulong* longArr =  (ulong*)b00; 
            uint hash = 0;


            hash = (uint)(2346526199184468592 * arr[0]
                   + 7569439352207963203 * arr[1]
                   + 244175462974450425 * arr[2]
                   + 787662783788549761 * arr[3]
                   + 25408476896404831 * arr[4]
                   + 819628286980801 * arr[5]
                   + 26439622160671 * arr[6]
                   + 7274231217480381548 * arr[7]
                   + 27512614111 * arr[8]
                   + 887503681 * arr[9]
                   + 28629151 * arr[10]
                   + 923521 * arr[11]
                   + 29791 * arr[12]
                   + 961 * arr[13]
                   + 31 * arr[14]
                   + arr[15]);
            
                
                
            /*
            for (var i = 0; i < 128/8; i++)
            {
                hash = 31 * hash + arr[i];
                //hash = (hash * 37) + str[i];
            } */
    
            return hash % cityMax;            
        }
    }
    
    static uint tpop_hash(Span<byte> str)
    {
        uint hash = 0;

        for (var i = 0; i < str.Length; i++)
        {
            hash = 31 * hash + str[i];
            //hash = (hash * 37) + str[i];
        }
    
        return hash % cityMax;
    }
    
    static  unsafe bool SIMDNoFallThrough(byte[] arr1, byte[] arr2)
    {
        if (arr1 == null || arr2 == null)
            return false;

        int arr1length = arr1.Length;

        if (arr1length != arr2.Length)
            return false;

        fixed (byte* b00 = arr1, b01 = arr2)
        {
            byte* b0 = b00, b1 = b01, last0 = b0 + arr1length, last1 = b1 + arr1length, last32 = last0 - 31;

            while (b0 < last32)
            {
                if (Avx2.MoveMask(Avx2.CompareEqual(Avx.LoadVector256(b0), Avx.LoadVector256(b1))) != -1)
                    return false;
                b0 += 32;
                b1 += 32;
            }
            return Avx2.MoveMask(Avx2.CompareEqual(Avx.LoadVector256(last0 - 32), Avx.LoadVector256(last1 - 32))) == -1;
        }
    }
    static unsafe bool SIMDNoFallThrough256Fixed(byte[] arr1, byte[] arr2)
    {
        fixed (byte* b00 = arr1, b01 = arr2)
        {
            byte* b0 = b00, b1 = b01;

            if (Avx2.MoveMask(Avx2.CompareEqual(Avx.LoadVector256(b0), Avx.LoadVector256(b1))) != -1)
                return false;

            if (Avx2.MoveMask(Avx2.CompareEqual(Avx.LoadVector256(b0 + 32), Avx.LoadVector256(b1+ 32))) != -1)
                return false;
            
            if (Avx2.MoveMask(Avx2.CompareEqual(Avx.LoadVector256(b0 + 64), Avx.LoadVector256(b1 + 64))) != -1)
                return false;
            
            return Avx2.MoveMask(Avx2.CompareEqual(Avx.LoadVector256(b0 + 96), Avx.LoadVector256(b1+ 96))) == -1;
        }
    }
    
    static unsafe bool SIMDNoFallThrough512(byte[] arr1, byte[] arr2)
    {
        fixed (byte* b0 = arr1, b1 = arr2)
        {
            int* int0p = (int*)b0;
            int* int1p = (int*)b1;
            
            // Load the first 64 bytes from each array
            Vector512<int> vec1_0 = Avx512F.LoadVector512(int0p);
            Vector512<int> vec2_0 = Avx512F.LoadVector512(int1p);
        
            // Load the second 64 bytes from each array
            Vector512<int> vec1_1 = Avx512F.LoadVector512(int0p + 16);
            Vector512<int> vec2_1 = Avx512F.LoadVector512(int1p + 16);
        
            // Compare the first 64 bytes
            bool equalFirstPart = vec1_0 == vec2_0;
            // Compare the second 64 bytes
            bool equalSecondPart = vec1_1 == vec2_1;

            
            
            // Both parts must be equal
            return equalFirstPart && equalSecondPart;
        }
    }
    
    [Benchmark]
    static public void CopyAndAVXBench(string[] args)
    {
        byte[] nameArr = new byte[42]{ (byte)'n', (byte)'a', (byte)'m', (byte)'e', (byte)'a', (byte)'n', (byte)'d', (byte)' ', (byte)'s', (byte)'t', (byte)'u', (byte)'f', (byte)'f', (byte)';', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f' };
        byte[] cpyNameArr = new byte[128];
        byte[] compArr = new byte[128];

        var st = Stopwatch.StartNew();
        var x = true;

        for ( var i = 0L; i < 100_000_000; i++)
        {
            var k = 0;
            while ((cpyNameArr[k] = nameArr[k]) != (byte)';') k++;

            x &= SIMDNoFallThrough(cpyNameArr, compArr);
        }
        
        
        Console.WriteLine("Elapsed CopyAndAVXBench: " + st.Elapsed);
        Console.WriteLine("X: " + x);
    }
    
    [Benchmark]
    static public void CopyAndAVXBenchFixed(string[] args)
    {
        byte[] nameArr = new byte[42]{ (byte)'n', (byte)'a', (byte)'m', (byte)'e', (byte)'a', (byte)'n', (byte)'d', (byte)' ', (byte)'s', (byte)'t', (byte)'u', (byte)'f', (byte)'f', (byte)';', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f' };
        byte[] cpyNameArr = new byte[128];
        byte[] compArr = new byte[128];

        var st = Stopwatch.StartNew();
        var x = true;

        for ( var i = 0L; i < 100_000_000; i++)
        {
            //var k = 0;
            //while ((cpyNameArr[k] = nameArr[k]) != (byte)';') k++;

            x &= SIMDNoFallThrough256Fixed(cpyNameArr, compArr);
        }
        
        
        Console.WriteLine("Elapsed CopyAndAVXBenchFixed: " + st.Elapsed);
        Console.WriteLine("X: " + x);
    }
    
    [Benchmark]
    static public void CopyAndAVX512Bench(string[] args)
    {
        byte[] nameArr = new byte[42]{ (byte)'n', (byte)'a', (byte)'m', (byte)'e', (byte)'a', (byte)'n', (byte)'d', (byte)' ', (byte)'s', (byte)'t', (byte)'u', (byte)'f', (byte)'f', (byte)';', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f' };
        byte[] cpyNameArr = new byte[128];
        byte[] compArr = new byte[128];

        var st = Stopwatch.StartNew();
        var x = true;

        for ( var i = 0L; i < 100_000_000; i++)
        {
            //var k = 0;
            //while ((cpyNameArr[k] = nameArr[k++]) != (byte)';');

            x &= SIMDNoFallThrough512(cpyNameArr, compArr);
        }
        
        
        Console.WriteLine("Elapsed CopyAndAVX512Bench: " + st.Elapsed);
        Console.WriteLine("X: " + x);
    }
    
    [Benchmark]
    static public void NOCopyAndByteCompareBench(string[] args)
    {
        byte[] nameArr = new byte[42]{ (byte)'n', (byte)'a', (byte)'m', (byte)'e', (byte)'a', (byte)'n', (byte)'d', (byte)' ', (byte)'s', (byte)'t', (byte)'u', (byte)'f', (byte)'f', (byte)';', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f', (byte)'f' };
        byte[] cpyNameArr = new byte[128];
        byte[] compArr = new byte[128];

        var st = Stopwatch.StartNew();

        for ( var i = 0L; i < 100_000_000; i++)
        {
            var j = 0;
            while ( nameArr[j] != (byte)';') j++;
            
            var k = 0;
            while ((compArr[k] == nameArr[k]) && nameArr[k] != (byte)';') k++;

            nameArr[2] += (byte)(k + j);

            //x &= SIMDNoFallThrough(cpyNameArr, compArr);
        }
        
        
        Console.WriteLine("Elapsed NOCopyAndByteCompareBench: " + st.Elapsed);
    }
    
    [Benchmark]
    static public void hashUnrollBench(string[] args)
    {
        byte[] arr = new byte[128];
        for (var i = 0; i < arr.Length; i++)
        {
            arr[i]= (byte)new Random(1).Next();
        }
     
        var st = Stopwatch.StartNew();
        uint x = 0;
        
        for ( var i = 0L; i < 100_000_000; i++)
        {

            x += tpop_hash(arr);

        }
        
        
        Console.WriteLine("Elapsed hashUnrollBench: " + st.Elapsed);
        Console.WriteLine("x hashUnrollBench: " + x);
    }
    
    [Benchmark]
    static public void hashBench(string[] args)
    {
        byte[] arr = new byte[9];
        for (var i = 0; i < arr.Length; i++)
        {
            arr[i]= (byte)new Random(1).Next();
        }
        var sp = new Span<byte>(arr);
        var st = Stopwatch.StartNew();
        uint x = 0;
        
        for ( var i = 0L; i < 100_000_000; i++)
        {

            x += tpop_hash(sp);

        }
        
        
        Console.WriteLine("Elapsed hashBench: " + st.Elapsed);
        Console.WriteLine("x hashBench: " + x);
    }
    
    [Benchmark]
    static public void hashMumurBench(string[] args)
    {
        byte[] arr = new byte[9];
        for (var i = 0; i < arr.Length; i++)
        {
            arr[i]= (byte)new Random(1).Next();
        }

        var st = Stopwatch.StartNew();
        uint x = 0;
        
        for ( var i = 0L; i < 100_000_000; i++)
        {

            x += MurmurHash3(arr, 0);

        }
        
        
        Console.WriteLine("Elapsed hashMumurBench: " + st.Elapsed);
        Console.WriteLine("x hashMumurBench: " + x);
    }
    
    [Benchmark]
    static public void hashRetardBench(string[] args)
    {
        byte[] arr = new byte[128];
        for (var i = 0; i < arr.Length; i++)
        {
            arr[i]= (byte)new Random(1).Next();
        }

        var st = Stopwatch.StartNew();
        uint x = 0;
        
        for ( var i = 0L; i < 100_000_000; i++)
        {

            x += RetardHash(arr);

        }
        
        
        Console.WriteLine("Elapsed hashRetardBench: " + st.Elapsed);
        Console.WriteLine("x hashRetardBench: " + x);
    }
}