// Copyright © 2019-2020 Mavidian Technologies Limited Liability Company. All Rights Reserved.

using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Diagnostics;
using System.IO;

namespace DataConveyer_AggregateTokens
{
   class Program
   {
      // Location of Data Conveyer input:
      private const string InputFolder = @"..\..\..\Data";

      static void Main()
      {
         var asmName = System.Reflection.Assembly.GetExecutingAssembly().GetName();
         var inputLocation = Path.GetFullPath(InputFolder);
         var outputFile = inputLocation + Path.DirectorySeparatorChar + "TokenAggregates.csv";
         Console.WriteLine($"{asmName.Name} v{asmName.Version} started execution on {DateTime.Now:MM-dd-yyyy a\\t hh:mm:ss tt}");
         Console.WriteLine($"DataConveyer library used: {ProductInfo.CurrentInfo.ToString()}");
         Console.WriteLine();
         Console.WriteLine("This application reads all XML files located in and input folder and aggregates tokens they contain.");
         Console.WriteLine();
         Console.WriteLine($"Input location : {inputLocation}");
         Console.WriteLine($"Output file: {outputFile}");
         Console.WriteLine();

         Console.WriteLine("Hit any key to start.");
         Console.ReadKey();

         Console.WriteLine("Processing started...'.");

         var processor = new FileProcessor(inputLocation, outputFile);

         var stopWatch = new Stopwatch();
         stopWatch.Start();
         var result = processor.ProcessFileAsync().Result;
         stopWatch.Stop();

         if (result.CompletionStatus == CompletionStatus.IntakeDepleted)
         {
            Console.WriteLine($"Processing completed in {stopWatch.Elapsed.TotalSeconds.ToString("##0.000")}s; there were {result.RowsRead} tokens identified in {result.ClustersRead - 1} files.");  // -1 for foot cluster
         }
         else Console.WriteLine($"Oops! Processing resulted in unexpected status of " + result.CompletionStatus.ToString());
         Console.WriteLine();

         Console.WriteLine("Hit Enter key to exit.");
         Console.ReadLine();
      }

   }
}
