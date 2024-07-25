using System;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading;
using _1RBC;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;

BenchmarkRunner.Run<v1>();
/*
var x = new v1();
var st1 = Stopwatch.StartNew();

x.Run();
x.Run();
x.Run();
x.Run();
Console.WriteLine("Read Took: " + st1.Elapsed);
*/
return 0;


unsafe
{
    const int sliceLen = 262_144;
//const int cityMax = 10_000;
    const int cityMaxP = 10;
    const int cityMax = 1 << cityMaxP;
    const int cityNameMax = 100;
//const int cityNameMax = 64 * 2;


    string measurementsTxt = "measurements.txt";
    var st = Stopwatch.StartNew();


    var fileInfo = new FileInfo(measurementsTxt);
    byte* ptr = null;
    var mmf = MemoryMappedFile.CreateFromFile(measurementsTxt, FileMode.Open);
    var accessor = mmf.CreateViewAccessor();
    accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

    Console.WriteLine("Read Took: " + st.Elapsed);

    var fileLen = fileInfo.Length;
    var CPU_COUNT = Environment.ProcessorCount;
    var threads = new Thread[CPU_COUNT];


    var cities = new Rec[CPU_COUNT][];
    for (var i = 0; i < CPU_COUNT; i++)
    {
        cities[i] = new Rec[cityMax];
    }

    long nextChunkStart = 0;
    for (int i = 0; i < CPU_COUNT - 1; i++)
    {
        var localI = i;
        long baseChunkSize = (fileLen / CPU_COUNT);
        long chunkStart = nextChunkStart;
        long limitedChunkSize = GetChunkLimitedSize(accessor, chunkStart, baseChunkSize);
        var localPtr = ptr;
        nextChunkStart = chunkStart + limitedChunkSize + 1;

        Console.WriteLine("i: " + localI);
        Console.WriteLine("Start: " + chunkStart);
        Console.WriteLine("End: " + (chunkStart + limitedChunkSize));
        Console.WriteLine("Chunk Size: " + limitedChunkSize);

        threads[i] = new Thread(() =>
        {
            Console.WriteLine("i: " + localI);
            Console.WriteLine("Start: " + chunkStart);
            Console.WriteLine("End: " + (chunkStart + limitedChunkSize));
            Console.WriteLine("Chunk Size: " + limitedChunkSize);

            ProcessChunk(localPtr, chunkStart, limitedChunkSize, cities[localI]);
            Console.WriteLine("Finished Took: " + st.Elapsed);
        });
        threads[i].Start();
    }

    threads[^1] = new Thread(() =>
    {
        Console.WriteLine("i: " + 31);
        Console.WriteLine("Start: " + nextChunkStart);
        Console.WriteLine("End: " + (nextChunkStart + (fileLen - nextChunkStart)));
        Console.WriteLine("Chunk Size: " + (fileLen - nextChunkStart));

        ProcessChunk(ptr, nextChunkStart, fileLen - nextChunkStart, cities[^1]);
        Console.WriteLine("Finished Took: " + st.Elapsed);
    });


    threads[^1].Start();
    Console.WriteLine("i: " + 31);
    Console.WriteLine("Start: " + nextChunkStart);
    Console.WriteLine("End: " + (nextChunkStart + (fileLen - nextChunkStart)));
    Console.WriteLine("Chunk Size: " + (fileLen - nextChunkStart));


    Console.WriteLine("File len: " + fileLen);


    foreach (var thread in threads)
    {
        thread.Join();
    }


    var counts = 0;
    for (var j = 0; j < cityMax; j++)
    {
        var exits = false;
        for (var i = 0; i < CPU_COUNT; i++)
        {
            if (cities[i][j].cityName[0] != 0)
            {
                exits = true;
                break;
            }
        }

        if (exits)
            counts++;
    }


    Console.WriteLine("COUNTED: " + counts);
    Console.WriteLine("Took: " + st.Elapsed);

    long GetChunkLimitedSize(MemoryMappedViewAccessor source, long chunkStart, long chunkSize)
    {
        var pad = cityNameMax + 6;
        var buffer = new byte[pad];

        source.ReadArray(chunkStart + chunkSize, buffer, 0, pad);

        var i = 0;
        for (; i < pad; i++)
        {
            if (buffer[i] == '\n')
                break;
        }

        return chunkSize + i;
    }

    const int slicePadding = (cityNameMax + 5) * 2;

    void ProcessChunk(byte* source, long chunkStart, long chunkSize, Rec[] cities)
    {
        source += chunkStart;

        var sourceEnd = source + chunkSize;
        var finishedIndex = 0;

        Span<byte> sliceSpan;
        while (source < sourceEnd - sliceLen)
        {
            sliceSpan = new Span<byte>(source, sliceLen);
            finishedIndex = ProcessSlice(sliceSpan, sliceSpan.Length, cities);
            source += finishedIndex;
        }

        var leftover = (int)(sourceEnd - source);
        Console.WriteLine("left: " + leftover);
        sliceSpan = new Span<byte>(source, leftover);
        ProcessLeftover(sliceSpan, leftover, cities);
    }

    void ProcessLeftover(Span<byte> target, int sliceLen, Rec[] cities)
    {
        var localIndex = 0;
        while (localIndex < sliceLen)
        {
            var nameStart = localIndex;
            while (target[localIndex++] != ';') ;

            var cityHTIndex = Search(cities, cityMax, target, nameStart, localIndex - 1, out var found);
            var rec = cities[cityHTIndex];
            if (!found)
            {
                Insert(cities, cityHTIndex, target, nameStart, localIndex - 1);
            }

            var temp = ParseTemp(target, ref localIndex);

            if (temp < rec.min)
                rec.min = temp;

            if (temp > rec.max)
                rec.max = temp;

            rec.sum += temp;
            rec.count++;
            //Print(nameSlice.ToArray(), temp);
        }
    }

    int ProcessSlice(Span<byte> target, int sliceLen, Rec[] cities)
    {
        var localIndex = 0;
        var rightLimit = sliceLen - slicePadding;
        while (localIndex < rightLimit)
        {
            var nameStart = localIndex;
            while (target[++localIndex] != ';') ;

            var cityHeapIndex = Search(cities, cityMax, target, nameStart, localIndex, out var found);
            var rec = cities[cityHeapIndex];
            if (!found)
            {
                Insert(cities, cityHeapIndex, target, nameStart, localIndex);
            }

            localIndex++;
            var temp = ParseTemp(target, ref localIndex);
            if (temp < rec.min)
                rec.min = temp;

            if (temp > rec.max)
                rec.max = temp;

            rec.sum += temp;
            rec.count++;
            //Print(nameSlice.ToArray(), temp);
        }

        return localIndex;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    int ParseTemp(Span<byte> target, ref int localOffset)
    {
        int m = target[localOffset] == '-' ? -1 : 1;
        localOffset += target[localOffset] == '-' ? 1 : 0;

        int d1;
        int d2;
        int d3;

        if (target[localOffset + 1] == '.')
        {
            d1 = 0;
            d2 = (target[localOffset] - '0') * 10;
            d3 = target[localOffset + 2] - '0';
            localOffset += 4;

            return m * (d2 + d3);
        }
        else
        {
            d1 = (target[localOffset] - '0') * 100;
            d2 = (target[localOffset + 1] - '0') * 10;
            d3 = target[localOffset + 3] - '0';
            localOffset += 5;

            return m * (d1 + d2 + d3);
        }
    }

    int Search(Rec[] haystack, int haystackLen, Span<byte> needle, int needleStart, int needleEnd, out bool found)
    {
        var i = (int)tpop_hash(needle, needleStart, needleEnd);
        found = false;

        do
        {
            var rightMost = needleEnd;
            var j = needleEnd - needleStart;
            while (rightMost > needleStart && haystack[i].cityName[--j] == needle[--rightMost]) ;

            if ((found = (rightMost <= needleStart)) || haystack[i].cityName[0] == 0)
                return i;

            i = (i + 1) % haystackLen;
        } while (true);
    }

    unsafe void Insert(Rec[] haystack, int cityHeapIndex, Span<byte> item, int start, int end)
    {
        for (var j = 0; start < end; j++, start++)
        {
            haystack[cityHeapIndex].cityName[j] = item[start];
        }
    }

    uint tpop_hash(Span<byte> str, int needleI, int needleEnd)
    {
        uint hash = 0;

        for (; needleI < needleEnd; needleI++)
        {
            hash = 31 * hash + str[needleI];
            //hash = (hash * 37) + str[i];
        }

        return hash % cityMax;
    }

    void Print(byte[] cityName, int temp)
    {
        try
        {
            if (cityName.Any(c =>
                    !char.IsAsciiLetter((char)c) && (char)c != '.' && (char)c != '\'' && (char)c != 0 &&
                    (char)c != ' '))
                throw new InvalidDataException("aaa");
        }
        catch
        {
        }


        var aschars = Encoding.UTF8.GetChars(cityName);
        var str = new String(aschars, 0, aschars.Length);

        Console.WriteLine("CITY NAME: " + str + "  " + "CITY TEMP: " + temp);
    }

    void exch(byte[][] a, int i, int j)
    {
        byte[] t = a[i];
        a[i] = a[j];
        a[j] = t;
    }

    void sort(byte[][] a)
    {
        sort_(a, 0, a.Length - 1, 0);
    }

    void sort_(byte[][] a, int lo, int hi, int d)
    {
        if (hi <= lo) return;
        int lt = lo, gt = hi;
        int v = a[lo][d];
        int i = lo + 1;
        while (i <= gt)
        {
            int t = a[i][d];
            if (t < v) exch(a, lt++, i++);
            else if (t > v) exch(a, i, gt--);
            else i++;
        }

        sort_(a, lo, lt - 1, d);
        if (v >= 0) sort_(a, lt, gt, d + 1);
        sort_(a, gt + 1, hi, d);
    }
}

unsafe struct Rec
{
    public fixed byte cityName[100];
    public int min;
    public int max;
    public int sum;
    public int count;
};