// Copyright © 2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.

using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Logging;
using Mavidian.DataConveyer.Orchestrators;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DataConveyer_AggregateTokens
{
   /// <summary>
   /// Represents Data Conveyer functionality specific to aggregating values in a series of XML files.
   /// </summary>
   internal class FileProcessor
   {
      private readonly IOrchestrator Orchestrator;

      private readonly string _inLocation;

      internal FileProcessor(string inLocation, string outFile)
      {
         _inLocation = inLocation;

         var config = new OrchestratorConfig()
         //To facilitate troubleshooting logging, data can be sent to a DataConveyer.log file:
         //var config = new OrchestratorConfig(LoggerCreator.CreateLogger(LoggerType.LogFile, "AggregateTokens", LogEntrySeverity.Information))
         {
            GlobalCacheElements = new string[] { "TokenSummary" },  //a single element - a dictionary - Dict<string,Tuple<int,int>>
            InputDataKind = KindOfTextData.XML,
            IntakeReaders = () => Directory.GetFiles(_inLocation, "*.xml").Select(f => File.OpenText(f)), //note that we're neglecting to dispose the stream readers here (not a production code)
            XmlJsonIntakeSettings = "RecordNode|Token,IncludeExplicitText|true",
            ExplicitTypeDefinitions = "__explicitText__|I",  //in our case, explicit text in Token node contains integer value
            ClusterMarker = (rec,pRec,n) => pRec == null ? true : rec.SourceNo != pRec.SourceNo,  // each file (source) constitutes a cluster
            MarkerStartsCluster = true,  //predicate (marker) matches the first record in cluster
            AppendFootCluster = true,  // to contain summarized token data
            AllowOnTheFlyInputFields = true,
            ConcurrencyLevel = 4,
            TransformerType = TransformerType.Universal,
            UniversalTransformer = CumulateTokenData,
            AllowTransformToAlterFields = true,
            OutputDataKind = KindOfTextData.Delimited,
            HeadersInFirstOutputRow = true,
            OutputFileName = outFile
         };

         Orchestrator = OrchestratorCreator.GetEtlOrchestrator(config);
      }

      /// <summary>
      /// Execute Data Conveyer process.
      /// </summary>
      /// <returns>Task containing the process results.</returns>
      internal async Task<ProcessResult> ProcessFileAsync()
      {
         var result = await Orchestrator.ExecuteAsync();
         Orchestrator.Dispose();

         return result;
      }


      /// <summary>
      /// Universal transformer to cumulate token data in global cache and remove cluster from output.
      /// In case of foot cluster, obtain summary data from global cache and prepare output 
      /// </summary>
      /// <param name="cluster"></param>
      /// <returns>Nothing (i.e. clusters are filtered out), except for the foot cluster (which contains summary data).</returns>
      private IEnumerable<ICluster> CumulateTokenData(ICluster cluster)
      {
         //A single element global cache containing a dictionary Dict<string,Tuple<int,int>>, where
         // Key   = color
         // Value = tuple consisting of count and total (i.e. cumulated __explicitText__)
         var gc = cluster.GlobalCache;

         gc.ReplaceValue("TokenSummary", (ConcurrentDictionary<string, (int count, int total)> t) => t ?? new ConcurrentDictionary<string, (int count, int total)>());  //initialize the dictionary during the 1st pass

         var tokenSummary = (ConcurrentDictionary<string, (int count, int total)>)gc["TokenSummary"];

         if (cluster.StartRecNo == Constants.FootClusterRecNo)
         {  //Foot cluster 
            //Note that Data Conveyer guarantees that foot cluster will be processed AFTER all other clusters have been processed.

            //Prepare a single record foot cluster with summary data to output
            foreach (var color in tokenSummary.Keys)
            {
               var footRec = cluster.ObtainEmptyRecord();
               footRec.AddItem("Color", color);
               (int count, int total) = tokenSummary[color];
               footRec.AddItem("Count", count);
               footRec.AddItem("Total", total);
               footRec.AddItem("Average", string.Format("{0:#0.0}", (double)total / count));

               cluster.AddRecord(footRec);
            }

            return Enumerable.Repeat(cluster, 1);
         }

         //Regular cluster - cumulate data in the global cache
         foreach (var rec in cluster.Records)
         {
            var color = (string)rec["color"];
            var value = (int)rec["__explicitText__"];

            tokenSummary.AddOrUpdate(color, (1, value), (c, t) => (t.count + 1, t.total + value));
         }

         return Enumerable.Empty<ICluster>(); //no data from regular cluster is sent to output
      }

   }
}
