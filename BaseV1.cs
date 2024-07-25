using System;
using System.IO;
using System.Net;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace _1RBC;

[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class BaseV1
{
    private string measurementsTxt = "measurements.txt";

    [Benchmark]
    public void Run()
    {
        const int bufferSize = 1024 * 1024;
        var buffer = new char[bufferSize];
        var spanBufffer = buffer.AsSpan(); 
        using var fileStream = new FileStream(measurementsTxt, FileMode.Open, FileAccess.Read);
        using var streamReader = new StreamReader(fileStream, Encoding.UTF8, true, bufferSize);
        
        while (streamReader.Read(spanBufffer) != 0){}
    }
}