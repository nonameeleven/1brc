using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using BenchmarkDotNet.Attributes;

namespace _1RBC;

public unsafe struct CityStat
{
    public CityStat()
    {
        Min = float.MaxValue;
        Max = float.MinValue;
        Sum = 0;
        Count = 0;
    }

    public float Min;
    public float Max;
    public double Sum;
    public long Count;
    public fixed byte Name[100];
};

[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class v1
{
    private string measurementsTxt = "measurements.txt";
    private MemoryMappedFile memoryMappedFile;

    [Benchmark]
    public unsafe void Run()
    {
        var threadCount = Environment.ProcessorCount;
        byte* ptr = null;

        memoryMappedFile = memoryMappedFile ?? MemoryMappedFile.CreateFromFile(measurementsTxt, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);

        var accessor = memoryMappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
        accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

        var chunkLimits = GetThreadChunkLimits(ptr, measurementsTxt, threadCount);
        var threads = new Thread[threadCount];
        var threadStats = new htable[threadCount];


        for (var i = 0; i < threadCount; i++)
        {
            var localIndex = i;
            var chunkStartPtr = (ptr + chunkLimits[localIndex]);
            var len = (int)(chunkLimits[localIndex + 1] - chunkLimits[localIndex]);
            var threadHtable = new htable(10_000);
            threadStats[localIndex] = threadHtable;

            threads[localIndex] = new Thread(() =>
            {
                var offset = 0;
                var chunk = new Span<byte>(chunkStartPtr, len);
                var l = GetLine(chunk, offset);

                do
                {
                    var commaIndex = l.IndexOf((byte)';') + 1;
                    var name = l.Slice(0, commaIndex - 1);
                    var temp = ParseTemp(l.Slice(commaIndex, l.Length - commaIndex));

                    var index = threadHtable.Search(name, out var found);
                    
                    if (!found)
                        threadHtable.Insert(index, name);
                    
                    threadHtable.Update(index, temp);

                    l = GetLine(chunk, offset);
                    offset += l.Length;
                } while (!l.IsEmpty);
            });
            threads[localIndex].Start();
        }

        foreach (var thread in threads)
        {
            thread.Join();
        }

        var aggregatedStats = threadStats.Aggregate(
                new Dictionary<string, CityStat>(),
                (agg1, agg2) =>
                {
                    var merged = agg1.Union((Dictionary<string, CityStat>)agg2)
                        .GroupBy(kv => kv.Key)
                        .ToDictionary(
                            g => g.Key,
                            g => new CityStat
                            {
                                Count = g.Aggregate(0L, (s, v) => s + v.Value.Count),
                                Sum = g.Aggregate(0d, (s, v) => s + v.Value.Sum),
                                Min = g.Aggregate(float.MaxValue, (s, v) => s > v.Value.Min ? v.Value.Min : s),
                                Max = g.Aggregate(float.MinValue, (s, v) => s < v.Value.Max ? v.Value.Max : s)
                            }
                        );

                    return merged;
                });
        
        var result = aggregatedStats.Aggregate(
            new StringBuilder(), 
            (agg, curr) =>
            {
                var mean = Math.Round((decimal)curr.Value.Sum / curr.Value.Count, 1); 
                var local_result = $"{curr.Key}={curr.Value.Min}/{mean}/{curr.Value.Max}, ";
                agg.Append(local_result);

                return agg;
            });

        Console.Write("{");
        Console.Write(result.ToString(0, result.Length-1));
        Console.Write("}");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private float ParseTemp(Span<byte> target)
    {
        var localOffset = 0;
        int m = target[0] == '-' ? -1 : 1;
        localOffset += target[localOffset] == '-' ? 1 : 0;

        int d1;
        int d2;
        int d3;

        if (target[localOffset + 1] == '.')
        {
            d1 = 0;
            d2 = (target[localOffset] - '0') * 10;
            d3 = target[localOffset + 2] - '0';

            var y = ((float)(m * (d2 + d3))) / 10;
            return y;
        }
        else
        {
            d1 = (target[localOffset] - '0') * 100;
            d2 = (target[localOffset + 1] - '0') * 10;
            d3 = target[localOffset + 3] - '0';

            var y = ((float)(m * (d1 + d2 + d3))) / 10;
            return y;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Span<byte> GetLine(Span<byte> src, int offset)
    {
        var min_offset = offset + 5;
        var i = min_offset;
        
        if (i >= src.Length)
            return Span<byte>.Empty;
        
        while (src[i++] != (byte)'\n');

        var result = src.Slice(offset, i - offset);
        return result;
    }

    private unsafe long[] GetThreadChunkLimits(byte* ptr, string filePath, int threadCount)
    {
        var result = new long[threadCount + 1];
        var fileInfo = new FileInfo(filePath);
        var fileLimit = fileInfo.Length - 1;

        for (var i = 1; i < threadCount + 1; i++)
        {
            var j = (long)Math.Truncate(((double)fileLimit / threadCount) * i);

            while (j < fileLimit && ptr[j] != '\n') j++;

            result[i] = j + 1;
        }

        return result;
    }
}

public unsafe class htable
{
    private uint size;
    public CityStat[] table;

    public htable(int size)
    {
        this.size = (uint)next_power_of_two(size);
        this.table = new CityStat[this.size];

        for (int i = 0; i < this.size; i++)
        {
            this.table[i] = new CityStat();
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Search(Span<byte> needle, out bool found)
    {
        var i = tpop_hash(needle);
        found = false;

        do
        {
            var needleStart = 0;
            var j = needle.Length;
            while (j > needleStart && table[i].Name[--j] == needle[j]) ;

            if ((found = (j <= needleStart)) || table[i].Name[0] == 0)
                return i;

            i = (i + 1) % (int)size;
        } while (true);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Insert(int idx, Span<byte> item)
    {
        for (var j = 0; j < item.Length; j++)
        {
            table[idx].Name[j] = item[j];
        }
    }

    public void Update(int index, float temp)
    {
        if (temp < this.table[index].Min)
            this.table[index].Min = temp;

        if (temp > this.table[index].Max)
            this.table[index].Max = temp;

        this.table[index].Sum += temp;
        this.table[index].Count++;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int tpop_hash(Span<byte> str)
    {
        uint hash = 0;

        for (var i = 0; i < str.Length; i++)
        {
            hash = 31 * hash + str[i];
        }

        return (int)(hash % size);
    }

    public static implicit operator Dictionary<string, CityStat>(htable table)
    {
        var result = new Dictionary<string, CityStat>();

        foreach (var cityStat in table.table)
        {
            if (cityStat.Name[0] == 0)
                continue;
            
            var name = ptr_to_string(cityStat.Name, 100);
            result.Add(name, cityStat);
        }

        return result;
    }

    private static string ptr_to_string(byte* s, int len)
    {
        var span = new Span<byte>(s, len);
        var result = Encoding.UTF8.GetString(span);
        return result;
    }

    private static int next_power_of_two(int x)
    {
        if (x <= 0)
            return 1;

        x--;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        return x + 1;
    }
}